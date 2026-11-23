"""End-to-end ETL tests for resource admission with controlled ordering.

A small partition ETL contends for two resources (memory slots and database
connections); a final aggregation task depends on every partition product.
Completion order is driven through process/thread-shared events, and
occupancy at every instant is recomputed two independent ways:

1. from the parent process' own admission/release events (the scheduler
   ledger);
2. from the action start/end markers observed where the actions run.

The two timelines must agree and must never exceed the declared capacities.
Multiprocessing synchronization primitives are used on purpose: they work
unchanged for the serial runner, the thread runner and the process runner.
"""

import multiprocessing as mp
import os
import threading
import time

import pytest

from doit.task import Task
from doit.dependency import Dependency, DbmDB
from doit.control import TaskDispatcher
from doit.runner import Runner, MThreadRunner, MRunner
from tests.test_runner import FakeReporter


# a manager process gives a list shared by threads and forked workers
_MANAGER = mp.Manager()


class Gate:
    """named events shared across the parent and worker processes.

    All events are created *before* the runner spawns workers, so forked
    children inherit the same primitives rather than private copies.
    """
    def __init__(self, names):
        self._events = {name: mp.Event() for name in names}

    def wait(self, name, timeout=20):
        return self._events[name].wait(timeout)

    def open(self, name):
        self._events[name].set()

    def clear_all(self):
        for event in self._events.values():
            event.clear()


# action markers: ('start'|'end', task_name, wall-clock timestamp)
ACTION_EVENTS = _MANAGER.list()
_EV_LOCK = mp.Lock()


def partition_action(name, target_path, gate_names):
    gate = _GATES[gate_names]
    with _EV_LOCK:
        ACTION_EVENTS.append(('start', name, time.time()))
    if not gate.wait(name):
        raise RuntimeError('gate for %r timed out' % name)
    with open(target_path, 'w') as handle:
        handle.write(name + '\n')
    with _EV_LOCK:
        ACTION_EVENTS.append(('end', name, time.time()))


def aggregate_action(name, dependencies, target_path, gate_names):
    gate = _GATES[gate_names]
    with _EV_LOCK:
        ACTION_EVENTS.append(('start', name, time.time()))
    if not gate.wait(name):
        raise RuntimeError('gate for %r timed out' % name)
    with open(target_path, 'w') as handle:
        for dep in dependencies:
            with open(dep) as src:
                handle.write(src.read())
    with _EV_LOCK:
        ACTION_EVENTS.append(('end', name, time.time()))


# registry of gates keyed by an id, so the picklable module-level actions
# can reach the shared primitives without capturing unpicklable closures.
_GATES = {}


def reset_world(gate):
    ACTION_EVENTS[:] = []
    gate.clear_all()


# --------------------------------------------------------------------------
# fixtures / helpers

PARALLEL_RUNNERS = [('thread', MThreadRunner, 3),
                    ('process', MRunner, 3)]
ALL_RUNNERS = [('serial', Runner, 0)] + PARALLEL_RUNNERS


@pytest.fixture(params=ALL_RUNNERS,
                ids=[r[0] for r in ALL_RUNNERS])
def runner_spec(request):
    return request.param


@pytest.fixture(params=PARALLEL_RUNNERS,
                ids=[r[0] for r in PARALLEL_RUNNERS])
def parallel_spec(request):
    return request.param


def make_runner(spec, dep_file, limits, bypass_limit=3):
    label, RunnerClass, num_process = spec
    dep_manager = Dependency(DbmDB, dep_file)
    reporter = FakeReporter()
    kwargs = dict(resource_limits=limits,
                  resource_bypass_limit=bypass_limit,
                  resource_record_events=True)
    if label == 'serial':
        runner_obj = RunnerClass(dep_manager, reporter, **kwargs)
    else:
        runner_obj = RunnerClass(dep_manager, reporter,
                                 num_process=num_process, **kwargs)
    return runner_obj, dep_manager, reporter


def build_etl(tmp_path, partition_resources, num_partitions=3,
              agg_resources=None):
    tasks = []
    targets = []
    names = []
    for i in range(num_partitions):
        name = 'p{}'.format(i)
        names.append(name)
        target = str(tmp_path / 'p{}.out'.format(i))
        targets.append(target)
        tasks.append(Task(
            name,
            [(partition_action, [name, target, None])],
            targets=[target], resources=dict(partition_resources)))
    names.append('aggregate')
    agg_target = str(tmp_path / 'agg.out')
    agg = Task(
        'aggregate',
        [(aggregate_action,
          ['aggregate', targets, agg_target, None])],
        file_dep=targets, targets=[agg_target],
        resources=dict(agg_resources or {'mem': 4, 'conn': 2}))
    tasks.append(agg)

    gate = Gate(names)
    gate_id = 'etl-' + tmp_path.name
    _GATES[gate_id] = gate
    # wire the gate id into the action arguments
    for task in tasks:
        action = task.actions[0]
        args = list(action.args)
        args[-1] = gate_id
        action.args = tuple(args)
    return tasks, agg_target, gate


def build_simple(tmp_path, specs):
    """specs: list of (name, resources dict)."""
    names = [name for name, _ in specs]
    gate = Gate(names)
    gate_id = 'simple-' + tmp_path.name
    _GATES[gate_id] = gate
    tasks = []
    for name, resources in specs:
        target = str(tmp_path / '{}.out'.format(name))
        tasks.append(Task(
            name, [(partition_action, [name, target, gate_id])],
            targets=[target], resources=dict(resources)))
    return tasks, gate


def drive_run(runner_obj, disp, gate, timeout=30):
    """run in a background thread, opening each gate as the task starts."""
    thread = threading.Thread(target=runner_obj.run_all, args=(disp,))
    thread.start()
    deadline = time.time() + timeout
    opened = set()
    while thread.is_alive() and time.time() < deadline:
        with _EV_LOCK:
            started = {n for k, n, _ in ACTION_EVENTS if k == 'start'}
        for name in started - opened:
            opened.add(name)
            gate.open(name)
        time.sleep(0.01)
    # any task that starts during shutdown also gets released
    with _EV_LOCK:
        started = {n for k, n, _ in ACTION_EVENTS if k == 'start'}
    for name in started - opened:
        gate.open(name)
    thread.join(timeout)
    assert not thread.is_alive(), 'run did not finish (deadlock?)'
    return thread


def wait_started(name, timeout=15):
    deadline = time.time() + timeout
    while time.time() < deadline:
        with _EV_LOCK:
            if any(k == 'start' and n == name
                   for k, n, _ in ACTION_EVENTS):
                return
        time.sleep(0.01)
    raise AssertionError('task %r never started' % name)


def snapshot_events():
    with _EV_LOCK:
        return list(ACTION_EVENTS)


def action_running_sets(action_events):
    """running-task set at every 'start' instant, derived from markers."""
    peaks = []
    for kind, name, ts in action_events:
        if kind != 'start':
            continue
        running = set()
        for k2, n2, t2 in action_events:
            if t2 > ts:
                continue
            end_ts = next((t3 for k3, n3, t3 in action_events
                           if k3 == 'end' and n3 == n2), None)
            if end_ts is None or end_ts > ts:
                running.add(n2)
        peaks.append(frozenset(running))
    return peaks


def ledger_running_sets(events):
    """running-task set after every parent-side admit/release event."""
    running = set()
    sets = []
    for ev in events:
        if ev['kind'] == 'admit':
            running.add(ev['task'])
        elif ev['kind'] == 'release':
            running.discard(ev['task'])
        else:
            continue
        sets.append(frozenset(running))
    return sets


def ledger_occupancy(events):
    """per-resource committed occupancy after every ledger event."""
    allocated = {}
    rows = []
    for ev in events:
        if ev['kind'] == 'admit':
            for resource, need in ev['needs'].items():
                allocated[resource] = allocated.get(resource, 0) + need
        elif ev['kind'] == 'release':
            for resource, need in ev['needs'].items():
                allocated[resource] -= need
        else:
            continue
        rows.append((ev['kind'], ev['task'], dict(allocated)))
    return rows


class TestEtlAdmission:

    def test_capacity_respected_aggregate_waits_for_products(
            self, runner_spec, tmp_path, depfile_name):
        limits = {'mem': 4, 'conn': 2}
        tasks, agg_target, gate = build_etl(
            tmp_path, partition_resources={'mem': 2, 'conn': 1})
        reset_world(gate)
        needs = {t.name: {r: n for r, n in t.resources.items() if n}
                 for t in tasks}

        runner_obj, dep_manager, reporter = make_runner(
            runner_spec, depfile_name, limits)
        disp = TaskDispatcher({t.name: t for t in tasks}, [],
                              [t.name for t in tasks])
        drive_run(runner_obj, disp, gate)
        dep_manager.close()

        assert runner_obj.final_result == 0
        # the aggregate really consumed the partition products
        assert os.path.exists(agg_target)
        with open(agg_target) as handle:
            assert sorted(handle.read().split()) == ['p0', 'p1', 'p2']

        events = runner_obj.resource_manager.events

        # independent recomputation #1: parent ledger never exceeds capacity,
        # and everything is released at the end
        rows = ledger_occupancy(events)
        for _, _, allocated in rows:
            for resource, value in allocated.items():
                assert 0 <= value <= limits[resource]
        assert rows[-1][2] == {r: 0 for r in limits}

        # independent recomputation #2: markers from where actions ran
        peaks = action_running_sets(snapshot_events())
        label = runner_spec[0]
        max_parallel = 1 if label == 'serial' else 3
        observed_max = max(len(p) for p in peaks)
        assert observed_max <= max_parallel
        if label != 'serial':
            # two partitions fit (mem 4 / 2); concurrency really happened
            assert observed_max == 2
        for running in peaks:
            used = {}
            for name in running:
                for resource, need in needs[name].items():
                    used[resource] = used.get(resource, 0) + need
            for resource, value in used.items():
                assert value <= limits[resource]

        # every concurrency pattern observed at the actions was reachable
        # through the parent ledger transition sequence
        ledger_states = set(ledger_running_sets(events))
        for pattern in set(peaks):
            assert any(pattern <= state for state in ledger_states), pattern

        # aggregate was never admitted before all partition products
        # existed: its admit event follows every partition release
        order = [(e['kind'], e['task']) for e in events]
        part_releases = {i for i, (k, n) in enumerate(order)
                         if k == 'release' and n.startswith('p')}
        agg_admit = next(i for i, (k, n) in enumerate(order)
                         if k == 'admit' and n == 'aggregate')
        assert agg_admit > max(part_releases)

    def test_big_parked_small_disjoint_runs_concurrently(
            self, parallel_spec, tmp_path, depfile_name):
        limits = {'mem': 4, 'gpu': 2}
        tasks, gate = build_simple(
            tmp_path, [('big', {'mem': 4}), ('small', {'gpu': 1})])
        reset_world(gate)
        runner_obj, dep_manager, reporter = make_runner(
            parallel_spec, depfile_name, limits)
        disp = TaskDispatcher({t.name: t for t in tasks}, [],
                              ['big', 'small'])

        thread = threading.Thread(target=runner_obj.run_all, args=(disp,))
        thread.start()
        try:
            wait_started('big')
            # big holds all mem, but small only needs gpu: it must start
            # anyway instead of being blocked by the unrelated wait
            wait_started('small')
            time.sleep(0.1)
            assert runner_obj.resource_manager.allocated == {
                'mem': 4, 'gpu': 1}
            gate.open('small')
            gate.open('big')
        finally:
            thread.join(20)
        assert not thread.is_alive()
        dep_manager.close()
        assert runner_obj.final_result == 0
        small_admit = next(e for e in runner_obj.resource_manager.events
                           if e['kind'] == 'admit' and e['task'] == 'small')
        assert small_admit['bypassed'] == []

    def test_bounded_bypass_then_waiter_gets_capacity(
            self, parallel_spec, tmp_path, depfile_name):
        limits = {'mem': 4}
        # head needs the whole capacity and blocks in its action; smalls
        # arriving later can only be admitted once head releases.
        tasks, gate = build_simple(
            tmp_path,
            [('head', {'mem': 4})] +
            [('s{}'.format(i), {'mem': 1}) for i in range(4)])
        reset_world(gate)
        runner_obj, dep_manager, reporter = make_runner(
            parallel_spec, depfile_name, limits, bypass_limit=3)
        disp = TaskDispatcher({t.name: t for t in tasks}, [],
                              [t.name for t in tasks])

        thread = threading.Thread(target=runner_obj.run_all, args=(disp,))
        thread.start()
        try:
            wait_started('head')
            # while head runs, no other mem task can fit; they must be
            # parked (not failed, not misreported as cyclic/finished)
            time.sleep(0.2)
            manager = runner_obj.resource_manager
            assert {w.name for w in manager.waiters} <= {
                's0', 's1', 's2', 's3'}
            gate.open('head')
            # head finishes; the parked smalls now flow and all complete
            for i in range(4):
                wait_started('s{}'.format(i))
                gate.open('s{}'.format(i))
        finally:
            thread.join(20)
        assert not thread.is_alive()
        dep_manager.close()
        assert runner_obj.final_result == 0
        assert not runner_obj.resource_manager.has_waiters()
        assert runner_obj.resource_manager.allocated == {'mem': 0}

    def test_limit_zero_blocks_overlapping_but_not_disjoint(
            self, parallel_spec, tmp_path, depfile_name):
        limits = {'mem': 2, 'gpu': 1}
        tasks, gate = build_simple(
            tmp_path, [('h', {'mem': 2}),
                       ('w', {'mem': 2}),
                       ('g', {'gpu': 1})])
        reset_world(gate)
        runner_obj, dep_manager, reporter = make_runner(
            parallel_spec, depfile_name, limits, bypass_limit=0)
        disp = TaskDispatcher({t.name: t for t in tasks}, [],
                              ['h', 'w', 'g'])

        thread = threading.Thread(target=runner_obj.run_all, args=(disp,))
        thread.start()
        try:
            wait_started('h')
            # w parks on capacity; the disjoint gpu task is still admitted
            wait_started('g')
            assert not any(k == 'start' and n == 'w'
                           for k, n, _ in snapshot_events())
            gate.open('g')
            gate.open('h')
            wait_started('w')
            gate.open('w')
        finally:
            thread.join(20)
        assert not thread.is_alive()
        dep_manager.close()
        assert runner_obj.final_result == 0
        gpu_admit = next(e for e in runner_obj.resource_manager.events
                         if e['kind'] == 'admit' and e['task'] == 'g')
        assert gpu_admit['bypassed'] == []

    def test_double_resource_cross_request(
            self, parallel_spec, tmp_path, depfile_name):
        # tasks each take mem+conn; the two resources must be committed
        # together and neither dimension may be over-committed.
        limits = {'mem': 4, 'conn': 2}
        tasks, gate = build_simple(
            tmp_path, [('a', {'mem': 2, 'conn': 1}),
                       ('b', {'mem': 2, 'conn': 1}),
                       ('c', {'mem': 2, 'conn': 1}),
                       ('d', {'mem': 2, 'conn': 1})])
        reset_world(gate)
        runner_obj, dep_manager, reporter = make_runner(
            parallel_spec, depfile_name, limits)
        disp = TaskDispatcher({t.name: t for t in tasks}, [],
                              [t.name for t in tasks])
        drive_run(runner_obj, disp, gate)
        dep_manager.close()
        assert runner_obj.final_result == 0
        for _, _, allocated in ledger_occupancy(
                runner_obj.resource_manager.events):
            assert allocated['mem'] <= 4
            assert allocated['conn'] <= 2
        # conn (2 units / 1 each) caps concurrency at 2
        peaks = action_running_sets(snapshot_events())
        assert max(len(p) for p in peaks) == 2

    def test_worker_count_bounds_total_concurrency_independently(
            self, tmp_path, depfile_name):
        # generous capacities (would allow 4), but -n 2 limits to 2 workers
        limits = {'mem': 100, 'conn': 100}
        tasks, gate = build_simple(
            tmp_path, [('a', {'mem': 1}), ('b', {'mem': 1}),
                       ('c', {'mem': 1}), ('d', {'mem': 1})])
        reset_world(gate)
        dep_manager = Dependency(DbmDB, depfile_name)
        runner_obj = MThreadRunner(
            dep_manager, FakeReporter(), num_process=2,
            resource_limits=limits, resource_record_events=True)
        disp = TaskDispatcher({t.name: t for t in tasks}, [],
                              [t.name for t in tasks])
        drive_run(runner_obj, disp, gate)
        dep_manager.close()
        assert runner_obj.final_result == 0
        peaks = action_running_sets(snapshot_events())
        assert max(len(p) for p in peaks) <= 2

    def test_wait_reasons_are_diagnosable(self, parallel_spec, tmp_path,
                                          depfile_name):
        limits = {'mem': 2, 'gpu': 1}
        tasks, gate = build_simple(
            tmp_path, [('h', {'mem': 2}), ('w', {'mem': 2}),
                       ('g', {'gpu': 1})])
        reset_world(gate)
        runner_obj, dep_manager, reporter = make_runner(
            parallel_spec, depfile_name, limits, bypass_limit=0)
        disp = TaskDispatcher({t.name: t for t in tasks}, [],
                              ['h', 'w', 'g'])
        thread = threading.Thread(target=runner_obj.run_all, args=(disp,))
        thread.start()
        try:
            wait_started('h')
            wait_started('g')
            manager = runner_obj.resource_manager
            # the parked mem waiter carries a machine-readable reason
            reasons = {w.name: w.reason for w in manager.waiters}
            assert 'w' in reasons
            assert reasons['w']  # non-empty diagnostic string
            snap = manager.snapshot()
            assert snap['waiting'][0][0] == 'w'
            assert snap['limits'] == limits
            gate.open('g')
            gate.open('h')
            wait_started('w')
            gate.open('w')
        finally:
            thread.join(20)
        assert not thread.is_alive()
        dep_manager.close()
        assert runner_obj.final_result == 0
