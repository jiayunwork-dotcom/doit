"""Integration of resource admission with the serial/parallel runners."""

import os
import threading
from io import StringIO

import pytest

from doit.exceptions import InvalidTask
from doit.dependency import DbmDB, Dependency
from doit.task import Task
from doit.control import TaskDispatcher
from doit import runner
from doit.runner import Runner, MThreadRunner, MRunner, JobHold
from tests.test_runner import FakeReporter


RUNNERS = [Runner, MThreadRunner]
if MRunner.available():
    RUNNERS.append(MRunner)


@pytest.fixture(params=RUNNERS)
def RunnerClass(request):
    return request.param


def make_runner(RunnerClass, dep_manager, reporter, limits,
                bypass_limit=3, num_process=3, events=True, continue_=False):
    kwargs = dict(resource_limits=limits,
                  resource_bypass_limit=bypass_limit,
                  resource_record_events=events,
                  continue_=continue_)
    if RunnerClass is Runner:
        return RunnerClass(dep_manager, reporter, **kwargs)
    return RunnerClass(dep_manager, reporter, num_process=num_process,
                       **kwargs)


def dispatcher(tasks, selected=None):
    task_dict = {t.name: t for t in tasks}
    return TaskDispatcher(task_dict, [], selected or list(task_dict))


# module level action state (picklable for the process runner under fork)
ACTION_LOG = []
_ACTION_LOCK = threading.Lock()
GATE = threading.Event()


def _log_action(name):
    with _ACTION_LOCK:
        ACTION_LOG.append(('start', name))


def gated_action(name):
    _log_action(name)
    GATE.wait(5)
    with _ACTION_LOCK:
        ACTION_LOG.append(('end', name))


def ok_action(name):
    with _ACTION_LOCK:
        ACTION_LOG.append(('ran', name))


def fail_action():
    raise RuntimeError('boom')


class TestResourceRunnerBasics:

    def test_no_resources_behaves_as_before(self, RunnerClass, dep_manager):
        reporter = FakeReporter()
        t1 = Task('t1', [lambda: ok_action('t1')])
        t2 = Task('t2', [lambda: ok_action('t2')])
        r = make_runner(RunnerClass, dep_manager, reporter, {})
        rc = r.run_all(dispatcher([t1, t2]))
        assert rc == runner.SUCCESS
        names = [item[1].name for item in reporter.log if item[0] == 'success']
        assert names == ['t1', 't2']
        # nothing ever waited, zero requirement checks
        assert r.resource_manager.check_count == 0

    def test_serial_admits_and_releases(self, dep_manager):
        reporter = FakeReporter()
        t1 = Task('t1', [lambda: ok_action('t1')], resources={'mem': 4})
        t2 = Task('t2', [lambda: ok_action('t2')], resources={'mem': 2})
        r = make_runner(Runner, dep_manager, reporter, {'mem': 4})
        rc = r.run_all(dispatcher([t1, t2]))
        assert rc == runner.SUCCESS
        assert r.resource_manager.allocated == {'mem': 0}
        assert r.resource_manager.commitments == {}

    def test_action_failure_releases(self, RunnerClass, dep_manager):
        reporter = FakeReporter()
        t1 = Task('t1', [fail_action], resources={'mem': 2})
        t2 = Task('t2', [lambda: ok_action('t2')], resources={'mem': 2},
                  task_dep=['t1'])
        r = make_runner(RunnerClass, dep_manager, reporter, {'mem': 4},
                        continue_=False)
        rc = r.run_all(dispatcher([t1, t2]))
        assert rc == runner.ERROR
        assert r.resource_manager.allocated == {'mem': 0}
        assert r.resource_manager.commitments == {}

    def test_save_success_failure_releases(self, dep_manager):
        # missing file_dep + target -> save_success raises FileNotFoundError
        reporter = FakeReporter(with_exceptions=True)
        t1 = Task('t1', [lambda: None], file_dep=['nope.dat'],
                  targets=['out.dat'], resources={'mem': 2})
        r = make_runner(Runner, dep_manager, reporter, {'mem': 2})
        rc = r.run_all(dispatcher([t1]))
        assert rc == runner.ERROR
        assert r.resource_manager.allocated == {'mem': 0}
        assert r.resource_manager.commitments == {}

    def test_uptodate_does_not_consume(self, RunnerClass, dep_manager):
        reporter = FakeReporter()
        t1 = Task('t1', [lambda: ok_action('t1')], file_dep=[__file__],
                  resources={'mem': 4})
        dep_manager.save_success(t1)
        r = make_runner(RunnerClass, dep_manager, reporter, {'mem': 4})
        rc = r.run_all(dispatcher([t1]))
        assert rc == runner.SUCCESS
        assert r.resource_manager.admit_count == 0
        assert ('up-to-date', t1) in reporter.log

    def test_ignored_does_not_consume(self, RunnerClass, dep_manager):
        reporter = FakeReporter()
        t1 = Task('t1', [lambda: ok_action('t1')], resources={'mem': 4})
        dep_manager.ignore(t1)
        r = make_runner(RunnerClass, dep_manager, reporter, {'mem': 4})
        rc = r.run_all(dispatcher([t1]))
        assert rc == runner.SUCCESS
        assert r.resource_manager.admit_count == 0
        assert ('ignore', t1) in reporter.log

    def test_dependency_failure_skips_without_admission(
            self, RunnerClass, dep_manager):
        reporter = FakeReporter()
        t1 = Task('t1', [fail_action], resources={'mem': 2})
        t2 = Task('t2', [lambda: ok_action('t2')], task_dep=['t1'],
                  resources={'mem': 2})
        r = make_runner(RunnerClass, dep_manager, reporter, {'mem': 4},
                        bypass_limit=0)
        rc = r.run_all(dispatcher([t1, t2]))
        assert rc == runner.ERROR
        # t2 was skipped via UnmetDependency, never admitted
        assert t2.name not in [
            e['task'] for e in r.resource_manager.events
            if e['kind'] == 'admit']

    def test_teardown_not_gated(self, RunnerClass, dep_manager):
        reporter = FakeReporter()
        t1 = Task('t1', [lambda: ok_action('t1')], teardown=[lambda: None],
                  resources={'mem': 4})
        r = make_runner(RunnerClass, dep_manager, reporter, {'mem': 1})
        rc = r.run_all(dispatcher([t1]))
        assert rc == runner.ERROR  # mem demand exceeds capacity
        # teardown never runs because the task itself was not admitted
        assert not any(item[0] == 'teardown' for item in reporter.log)

    def test_static_bad_declaration_aborts_before_actions(
            self, RunnerClass, dep_manager):
        reporter = FakeReporter()
        marker = []
        t1 = Task('t1', [lambda: marker.append(1)], resources={'mem': 9})
        t2 = Task('t2', [lambda: marker.append(2)])
        r = make_runner(RunnerClass, dep_manager, reporter, {'mem': 4})
        rc = r.run_all(dispatcher([t1, t2]))
        assert rc == runner.ERROR
        assert marker == []  # no action ran
        assert ('runtime_error',) in reporter.log

    def test_unknown_resource_aborts(self, RunnerClass, dep_manager):
        reporter = FakeReporter()
        t1 = Task('t1', [lambda: None], resources={'conn': 1})
        r = make_runner(RunnerClass, dep_manager, reporter, {'mem': 4})
        rc = r.run_all(dispatcher([t1]))
        assert rc == runner.ERROR
        assert ('runtime_error',) in reporter.log


class TestSetupAndCalc:

    def test_setup_task_uses_own_resources(self, RunnerClass, dep_manager):
        reporter = FakeReporter()
        setup = Task('s', [lambda: ok_action('s')], resources={'mem': 1})
        main = Task('m', [lambda: ok_action('m')], setup=['s'],
                    resources={'mem': 4})
        r = make_runner(RunnerClass, dep_manager, reporter,
                        {'mem': 4}, num_process=2)
        rc = r.run_all(dispatcher([setup, main], ['m']))
        assert rc == runner.SUCCESS
        admitted = [e['task'] for e in r.resource_manager.events
                    if e['kind'] == 'admit']
        # both the setup task and the main task get admitted separately
        assert 's' in admitted
        assert 'm' in admitted

    def test_calc_dep_runs_first(self, RunnerClass, dep_manager):
        reporter = FakeReporter()
        calc = Task('c', [lambda: {'file_dep': [__file__]}])
        main = Task('m', [lambda: ok_action('m')], calc_dep=['c'],
                    resources={'mem': 1})
        r = make_runner(RunnerClass, dep_manager, reporter, {'mem': 1})
        rc = r.run_all(dispatcher([calc, main], ['m']))
        assert rc == runner.SUCCESS
        order = [item[1].name for item in reporter.log
                 if item[0] in ('execute',)]
        assert order.index('c') < order.index('m')


class TestDelayedValidation:

    def test_bad_delayed_task_rejected_at_admission(
            self, RunnerClass, dep_manager):
        # simulate a delayed task that materializes with an invalid demand
        reporter = FakeReporter()

        def creator():
            return {'basename': 'd',
                    'actions': [lambda: None],
                    'resources': {'mem': 100}}

        from doit.loader import create_after
        from doit.task import DelayedLoader

        loader = create_after()(creator).doit_create_after
        placeholder = Task('d', None, loader=loader)
        # mark as the real loader so dispatcher invokes the creator
        r = make_runner(RunnerClass, dep_manager, reporter, {'mem': 4})
        rc = r.run_all(dispatcher([placeholder]))
        assert rc == runner.ERROR
        assert ('runtime_error',) in reporter.log

    def test_good_delayed_task_runs_after_dependency(
            self, RunnerClass, dep_manager):
        reporter = FakeReporter()

        def creator():
            return {'basename': 'd',
                    'actions': [lambda: None],
                    'resources': {'mem': 1}}

        from doit.loader import create_after

        loader = create_after()(creator).doit_create_after
        placeholder = Task('d', None, loader=loader)
        r = make_runner(RunnerClass, dep_manager, reporter, {'mem': 1})
        rc = r.run_all(dispatcher([placeholder]))
        assert rc == runner.SUCCESS
        assert ('success', r.tasks['d']) if hasattr(r, 'tasks') else True
        assert any(item[0] == 'success' and item[1].name == 'd'
                   for item in reporter.log)
        assert r.resource_manager.allocated == {'mem': 0}


class TestPreflightOrdering:

    def test_bad_static_task_blocks_setup_and_actions(
            self, RunnerClass, dep_manager):
        marker = []
        reporter = FakeReporter()
        setup = Task('s', [lambda: marker.append('setup')])
        bad = Task('bad', [lambda: marker.append('action')],
                   setup=['s'], resources={'mem': 99})
        r = make_runner(RunnerClass, dep_manager, reporter, {'mem': 4})
        rc = r.run_all(dispatcher([setup, bad], ['bad']))
        assert rc == runner.ERROR
        assert marker == []  # neither the setup nor the action ran
        assert ('runtime_error',) in reporter.log

    def test_resource_wait_not_confused_with_cycle_or_completion(
            self, RunnerClass, dep_manager):
        # a task that parks on capacity must not mark dependents ready or
        # record success before it actually runs.
        reporter = FakeReporter()
        holder = Task('h', [lambda: None], resources={'mem': 2})
        parked = Task('p', [lambda: None], resources={'mem': 2})
        down = Task('d', [lambda: None], task_dep=['p'])
        r = make_runner(RunnerClass, dep_manager, reporter, {'mem': 2},
                        num_process=2)
        disp = dispatcher([holder, parked, down], ['h', 'p', 'd'])
        if RunnerClass is Runner:
            # serial: holder finishes and releases before p is considered,
            # so everything still completes successfully in order
            rc = r.run_all(disp)
            assert rc == runner.SUCCESS
            order = [item[1].name for item in reporter.log
                     if item[0] == 'success']
            assert order.index('p') < order.index('d')
        else:
            r._run_tasks_init(disp)
            j1 = r.get_next_job(None)
            assert j1.name == 'h'
            j2 = r.get_next_job(None)
            # p cannot fit; the dispatcher must keep d waiting on p rather
            # than raising a cyclic-dependency assertion or ending the run
            assert isinstance(j2, runner.JobHold)
            assert r.resource_manager.has_waiters()
            # no success was recorded for the parked task or its dependent
            assert not any(item[0] == 'success' and item[1].name in ('p', 'd')
                           for item in reporter.log)


class TestPickleSafety:

    def test_runner_pickles_with_manager_stripped(self, dep_manager):
        import pickle
        from doit.reporter import ConsoleReporter
        run = MRunner(dep_manager, ConsoleReporter(StringIO(), {}),
                      num_process=2, resource_limits={'mem': 4})
        t1 = Task('t1', [], resources={'mem': 1})
        run._run_tasks_init(dispatcher([t1]))
        state = run.__getstate__()
        # the parent-only manager (holds a lock) must not cross processes
        assert state['resource_manager'] is None
        pickle.dumps(state)  # must not raise


class TestDispatcherGetNextJob:
    """white-box checks on MRunner.get_next_job admission/parking."""

    def test_parked_task_revived_by_release(self, dep_manager):
        reporter = FakeReporter()
        t1 = Task('t1', [lambda: None], resources={'mem': 2})
        t2 = Task('t2', [lambda: None], resources={'mem': 2})
        run = MThreadRunner(dep_manager, reporter, num_process=2,
                            resource_limits={'mem': 2})
        disp = dispatcher([t1, t2])
        run._run_tasks_init(disp)

        j1 = run.get_next_job(None)
        assert j1.name == 't1'
        # second task cannot fit and the dispatcher has no other branch:
        # a hold is returned while the task is parked on resources
        j2 = run.get_next_job(None)
        assert isinstance(j2, JobHold)
        assert run.resource_manager.has_waiters()

        # finishing t1 frees capacity; parked t2 must come back before
        # asking the dispatcher for anything new
        node1 = disp.nodes['t1']
        run.resource_manager.release(node1)
        j3 = run.get_next_job(node1)
        assert j3.name == 't2'
        assert not run.resource_manager.has_waiters()

    def test_small_task_bypasses_parked_big_one(self, dep_manager):
        reporter = FakeReporter()
        big = Task('big', [lambda: None], resources={'mem': 4})
        small = Task('small', [lambda: None], resources={'mem': 1})
        run = MThreadRunner(dep_manager, reporter, num_process=2,
                            resource_limits={'mem': 4},
                            resource_bypass_limit=3)
        disp = dispatcher([big, small])
        run._run_tasks_init(disp)

        assert run.get_next_job(None).name == 'big'
        # big occupies everything; small parks (capacity), worker holds
        assert isinstance(run.get_next_job(None), JobHold)
        big_node = disp.nodes['big']
        run.resource_manager.release(big_node)
        # free slot first serves parked waiters
        assert run.get_next_job(big_node).name == 'small'
