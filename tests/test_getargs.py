"""Tests for incremental ``getargs`` value tracking.

The consumer's up-to-date decision must be based on the exact saved values it
consumes, not on the producer's last action ``result``.  Consumers must also
receive independent copies, so mutating an argument can never alter the
producer's saved value or another consumer's input.
"""

import json
import os
import subprocess
import sys
import textwrap
from io import StringIO

import pytest

from doit.dependency import DbmDB, Dependency, JsonDB, SqliteDB
from doit.getargs import (
    GetargsError, basis, canonical, deliver, freeze, group_member_items,
    GETARGS_KEY,
)
from doit.task import Task
from tests.conftest import CmdFactory
from doit.cmd_run import Run


# ---------------------------------------------------------------------------
# helpers


def write(path, content):
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    with open(path, 'w') as handle:
        handle.write(content)


def read(path):
    with open(path) as handle:
        return handle.read()


@pytest.fixture
def project(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    return tmp_path


def run_tasks(tasks, dep_file, sel=None, num_process=0, par_type='process'):
    output = StringIO()
    cmd = CmdFactory(Run, outstream=output, backend='dbm', dep_file=dep_file,
                     task_list=tasks, sel_tasks=sel)
    rc = cmd._execute(output, verbosity=0, num_process=num_process,
                      par_type=par_type)
    return rc, output.getvalue()


def open_db(dep_file):
    return Dependency(DbmDB, dep_file)


def saved_values(dep_file, name):
    dep = open_db(dep_file)
    try:
        return dep.get_values(name)
    finally:
        dep.close()


def getargs_basis(dep_file, name):
    dep = open_db(dep_file)
    try:
        return dep._get(name, GETARGS_KEY)
    finally:
        dep.close()


def producer_task(state):
    return Task('produce', [(lambda: dict(state),)])


# ---------------------------------------------------------------------------
# canonical comparison semantics


class TestCanonical:

    def test_mapping_order_is_irrelevant(self):
        assert canonical({'a': 1, 'b': 2}) == canonical({'b': 2, 'a': 1})
        assert canonical({'x': {'p': 1, 'q': 2}}) == \
            canonical({'x': {'q': 2, 'p': 1}})

    def test_identity_is_irrelevant(self):
        first = {'a': [1, 2]}
        second = {'a': [1, 2]}
        assert first is not second
        assert canonical(first) == canonical(second)

    def test_list_order_matters(self):
        assert canonical([1, 2]) != canonical([2, 1])

    def test_missing_key_differs_from_none(self):
        assert canonical({'a': 1}) != canonical({'a': 1, 'b': None})

    def test_bool_differs_from_number(self):
        assert canonical(True) != canonical(1)
        assert canonical(False) != canonical(0)
        assert canonical({'v': True}) != canonical({'v': 1})

    def test_none_differs_from_zero(self):
        assert canonical(None) != canonical(0)
        assert canonical(None) != canonical(False)

    def test_finite_float(self):
        assert canonical(1.5) == canonical(1.5)

    @pytest.mark.parametrize('bad', [float('nan'), float('inf'),
                                     float('-inf')])
    def test_non_finite_rejected(self, bad):
        with pytest.raises(GetargsError):
            canonical(bad)
        with pytest.raises(GetargsError):
            canonical({'v': [bad]})

    def test_acyclic_only(self):
        loop = {}
        loop['self'] = loop
        with pytest.raises(GetargsError):
            canonical(loop)

    def test_scalars(self):
        for value in ('text', 3, 2.25, True, False, None):
            assert canonical(value)


# ---------------------------------------------------------------------------
# group membership


class TestGroupMembers:

    def test_external_prerequisite_not_included(self):
        group = Task('g', None, has_subtask=True,
                     task_dep=['g:a', 'prep'])
        prep = Task('prep', [(lambda: None,)])
        a = Task('g:a', [(lambda: None,)], subtask_of='g')
        tasks = {'g': group, 'g:a': a, 'prep': prep}
        assert group_member_items(tasks, 'g') == [('a', 'g:a')]

    def test_sorted_by_short_name(self):
        group = Task('g', None, has_subtask=True)
        tasks = {'g': group}
        for name in ('b', 'a', 'c'):
            tasks['g:%s' % name] = Task('g:%s' % name, None, subtask_of='g')
        assert group_member_items(tasks, 'g') == [
            ('a', 'g:a'), ('b', 'g:b'), ('c', 'g:c')]

    def test_empty_group(self):
        group = Task('g', None, has_subtask=True)
        assert group_member_items({'g': group}, 'g') == []


# ---------------------------------------------------------------------------
# snapshot / delivery isolation


class TestDeliveryIsolation:

    def _dep(self, tmp_path):
        return Dependency(DbmDB, str(tmp_path / 'db'))

    def test_consumers_get_independent_copies(self, tmp_path):
        dep = self._dep(tmp_path)
        producer = Task('produce', [(lambda: None,)])
        producer.values = {'batch': {'parts': [1, 2]}}
        dep.save_success(producer)
        getargs = {'v': ('produce', 'batch')}
        _, frozen = freeze(dep, {'produce': producer}, getargs)
        first = deliver(frozen)['v']
        second = deliver(frozen)['v']
        first['parts'].append(99)
        assert second == {'parts': [1, 2]}
        assert dep.get_values('produce') == {'batch': {'parts': [1, 2]}}
        dep.close()

    def test_stored_value_not_aliased(self, tmp_path):
        dep = self._dep(tmp_path)
        producer = Task('produce', [(lambda: None,)])
        producer.values = {'batch': {'parts': [1, 2]}}
        dep.save_success(producer)
        received = dep.get_values('produce')
        received['batch']['parts'].append(7)
        assert dep.get_values('produce') == {'batch': {'parts': [1, 2]}}
        dep.close()

    def test_basis_independent_of_mutation_after_freeze(self, tmp_path):
        dep = self._dep(tmp_path)
        producer = Task('produce', [(lambda: None,)])
        producer.values = {'batch': {'parts': [1, 2]}}
        dep.save_success(producer)
        getargs = {'v': ('produce', 'batch')}
        first_basis, frozen = freeze(dep, {'produce': producer}, getargs)
        got = deliver(frozen)['v']
        got['parts'].append(99)
        # a new snapshot sees the untouched stored value
        assert basis(dep, {'produce': producer}, getargs) == first_basis
        dep.close()


# ---------------------------------------------------------------------------
# dependency level: save / status decisions


class TestDependencyStatus:

    def _setup(self, tmp_path, values):
        dep = Dependency(DbmDB, str(tmp_path / 'db'))
        producer = Task('produce', [(lambda: None,)])
        producer.values = dict(values)
        dep.save_success(producer)
        return dep, producer

    def _consumer(self, key):
        return Task('consume', [(lambda: None,)], getargs={'v': ('produce', key)})

    def test_key_changed_triggers_run(self, tmp_path):
        dep, producer = self._setup(tmp_path, {'a': 1, 'b': 2})
        consumer = self._consumer('a')
        # save a success basis with the current values
        dep.save_success(consumer, tasks_dict={'produce': producer,
                                               'consume': consumer})
        assert dep.get_status(consumer, {'produce': producer}).status == 'up-to-date'
        producer.values = {'a': 5, 'b': 2}
        dep.save_success(producer)
        assert dep.get_status(consumer, {'produce': producer}).status == 'run'
        dep.close()

    def test_unselected_key_change_skips(self, tmp_path):
        dep, producer = self._setup(tmp_path, {'a': 1, 'b': 2})
        consumer = self._consumer('a')
        dep.save_success(consumer, tasks_dict={'produce': producer})
        producer.values = {'a': 1, 'b': 99}
        dep.save_success(producer)
        assert dep.get_status(consumer, {'produce': producer}).status == 'up-to-date'
        dep.close()

    def test_whole_dict_checks_every_key(self, tmp_path):
        dep, producer = self._setup(tmp_path, {'a': 1, 'b': 2})
        consumer = self._consumer(None)
        dep.save_success(consumer, tasks_dict={'produce': producer})
        assert dep.get_status(consumer, {'produce': producer}).status == 'up-to-date'
        producer.values = {'a': 1, 'b': 99}
        dep.save_success(producer)
        assert dep.get_status(consumer, {'produce': producer}).status == 'run'
        dep.close()

    def test_missing_key_is_an_error(self, tmp_path):
        dep, producer = self._setup(tmp_path, {'a': 1})
        consumer = self._consumer('nope')
        producer.values = {'a': 1}
        # no basis can be derived: the consumer must not be considered
        # up-to-date on a bogus saved record.
        assert dep.get_status(consumer, {'produce': producer}).status == 'run'
        with pytest.raises(GetargsError):
            basis(dep, {'produce': producer}, consumer.getargs)
        dep.close()

    def test_removed_getargs_invalidates_old_basis(self, tmp_path):
        dep, producer = self._setup(tmp_path, {'a': 1})
        consumer = self._consumer('a')
        dep.save_success(consumer, tasks_dict={'produce': producer})
        plain = Task('consume', [(lambda: None,)], uptodate=[True])
        dep.save_success(plain)
        # a consumer without getargs that still carries a basis must run once
        dep._set('consume', GETARGS_KEY, 'stale')
        assert dep.get_status(plain, {}).status == 'run'
        dep.close()


class TestRebinding:

    def _run(self, tmp_path, getargs):
        dep = Dependency(DbmDB, str(tmp_path / 'db'))
        p1 = Task('p1', [(lambda: None,)])
        p1.values = {'x': 1, 'y': 2}
        p2 = Task('p2', [(lambda: None,)])
        p2.values = {'x': 1, 'y': 2}
        dep.save_success(p1)
        dep.save_success(p2)
        consumer = Task('consume', [(lambda: None,)], getargs=getargs)
        dep.save_success(consumer, tasks_dict={'p1': p1, 'p2': p2})
        return dep, {'p1': p1, 'p2': p2}, consumer

    def test_same_binding_up_to_date(self, tmp_path):
        dep, tasks, consumer = self._run(tmp_path, {'v': ('p1', 'x')})
        assert dep.get_status(consumer, tasks).status == 'up-to-date'
        dep.close()

    def test_source_rebind_invalidates(self, tmp_path):
        dep, tasks, consumer = self._run(tmp_path, {'v': ('p1', 'x')})
        # rebind to p2: the declaration change must invalidate
        consumer.getargs = {'v': ('p2', 'x')}
        assert dep.get_status(consumer, tasks).status == 'run'
        dep.close()

    def test_key_rebind_invalidates(self, tmp_path):
        dep, tasks, consumer = self._run(tmp_path, {'v': ('p1', 'x')})
        consumer.getargs = {'v': ('p1', 'y')}
        assert dep.get_status(consumer, tasks).status == 'run'
        dep.close()

    def test_whole_dict_switch_invalidates(self, tmp_path):
        dep, tasks, consumer = self._run(tmp_path, {'v': ('p1', 'x')})
        consumer.getargs = {'v': ('p1', None)}
        assert dep.get_status(consumer, tasks).status == 'run'
        dep.close()


# ---------------------------------------------------------------------------
# end-to-end through the real runner


REPORTING_DODO = '''
import json
import os

STATE = json.load(open('state.json'))

def task_produce():
    def produce():
        return dict(STATE['values'])
    def trailer():
        return STATE.get('trailer', 'same-trailer')
    return {'actions': [(produce,), (trailer,)]}

def task_consume():
    def consume(batch):
        with open('out.txt', 'w') as handle:
            handle.write(json.dumps(batch, sort_keys=True))
    return {'actions': [(consume,)],
            'getargs': {'batch': tuple(STATE['getargs'])},
            'verbosity': 2}

def task_mutate():
    def mutate(batch):
        batch['parts'].append(99)
        with open('out2.txt', 'w') as handle:
            handle.write(json.dumps(batch, sort_keys=True))
    return {'actions': [(mutate,)],
            'getargs': {'batch': tuple(STATE['getargs'])},
            'verbosity': 2}
'''


def load_module(path):
    """Load a dodo-like module by absolute file path (isolated from repo)."""
    import importlib.util
    spec = importlib.util.spec_from_file_location('getargs_test_dodo', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestEndToEndSerial:

    def test_result_unchanged_value_changed(self, project, depfile_name):
        write('state.json', json.dumps(
            {'values': {'batch': 1}, 'getargs': ['produce', 'batch']}))
        write('dodo.py', REPORTING_DODO)
        module = load_module(str(project / 'dodo.py'))
        tasks = load_tasks(module)
        rc, out = run_tasks(tasks, depfile_name, sel=['produce', 'consume'])
        assert rc == 0
        assert read('out.txt') == '1'

        # change the saved batch; the trailer is unchanged
        write('state.json', json.dumps(
            {'values': {'batch': 2}, 'getargs': ['produce', 'batch']}))
        module = load_module(str(project / 'dodo.py'))
        tasks = load_tasks(module)
        rc, out = run_tasks(tasks, depfile_name, sel=['produce', 'consume'])
        assert rc == 0
        assert read('out.txt') == '2'

        # stable: consumer skips, saved value survives reopen
        module = load_module(str(project / 'dodo.py'))
        tasks = load_tasks(module)
        rc, out = run_tasks(tasks, depfile_name, sel=['produce', 'consume'])
        assert '-- consume' in out
        assert saved_values(depfile_name, 'produce') == {'batch': 2}

    def test_multi_consumer_mutation_isolated(self, project, depfile_name):
        write('state.json', json.dumps(
            {'values': {'batch': {'parts': [1, 2]}},
             'getargs': ['produce', 'batch']}))
        write('dodo.py', REPORTING_DODO)
        module = load_module(str(project / 'dodo.py'))
        tasks = load_tasks(module)
        rc, out = run_tasks(
            tasks, depfile_name, sel=['produce', 'mutate', 'consume'])
        assert rc == 0
        assert saved_values(depfile_name, 'produce') == {'batch': {'parts': [1, 2]}}
        assert json.loads(read('out2.txt')) == {'parts': [1, 2, 99]}
        # the other consumer sees the pristine value
        assert json.loads(read('out.txt')) == {'parts': [1, 2]}


class TestEndToEndParallel:

    @pytest.mark.parametrize('par_type,num', [('thread', 2), ('process', 2)])
    def test_concurrent_consumers_get_pristine_copies(
            self, project, depfile_name, par_type, num):
        write('state.json', json.dumps(
            {'values': {'batch': {'parts': [1, 2]}},
             'getargs': ['produce', 'batch']}))
        write('dodo.py', REPORTING_DODO)
        module = load_module(str(project / 'dodo.py'))
        tasks = load_tasks(module)
        rc, out = run_tasks(tasks, depfile_name,
                            sel=['produce', 'mutate', 'consume'],
                            num_process=num, par_type=par_type)
        assert rc == 0, out
        assert saved_values(depfile_name, 'produce') == {'batch': {'parts': [1, 2]}}
        assert json.loads(read('out2.txt')) == {'parts': [1, 2, 99]}
        assert json.loads(read('out.txt')) == {'parts': [1, 2]}


def load_tasks(module):
    from doit.loader import generate_tasks
    names = [n for n in dir(module) if n.startswith('task_')]
    tasks = []
    for name in names:
        tasks.extend(generate_tasks(name[5:], getattr(module, name)()))
    return tasks


# ---------------------------------------------------------------------------
# real CLI, fresh python process


CLI_DODO = '''
import json
import os

STATE = json.load(open('state.json'))

def task_produce():
    def produce():
        return dict(STATE['values'])
    def trailer():
        return 'same-trailer'
    return {'actions': [(produce,), (trailer,)]}

def task_consume():
    def consume(batch):
        with open('out.txt', 'w') as handle:
            handle.write(json.dumps(batch, sort_keys=True))
    return {'actions': [(consume,)],
            'getargs': {'batch': tuple(STATE['getargs'])},
            'verbosity': 2}
'''


def invoke_cli(cwd, *args):
    env = dict(os.environ)
    env['PYTHONPATH'] = os.pathsep.join(
        [os.path.dirname(os.path.dirname(os.path.abspath(__file__)))]
        + [p for p in [env.get('PYTHONPATH')] if p])
    return subprocess.run(
        [sys.executable, '-m', 'doit'] + list(args),
        cwd=str(cwd), env=env, capture_output=True, text=True)


class TestRealCli:

    def test_fresh_process_skips_stable_and_invalidates_changed(
            self, project):
        write('state.json', json.dumps(
            {'values': {'batch': 1}, 'getargs': ['produce', 'batch']}))
        write('dodo.py', CLI_DODO)

        first = invoke_cli(project, 'run', '-v', '1')
        assert first.returncode == 0, first.stderr
        assert read('out.txt') == '1'

        # nothing changed: reopen in a brand new interpreter, skip
        second = invoke_cli(project, 'run', '-v', '1')
        assert second.returncode == 0, second.stderr
        assert '-- consume' in second.stdout

        # saved value changed, result unchanged: must recompute
        write('state.json', json.dumps(
            {'values': {'batch': 2}, 'getargs': ['produce', 'batch']}))
        third = invoke_cli(project, 'run', '-v', '1')
        assert third.returncode == 0, third.stderr
        assert '.  consume' in third.stdout
        assert read('out.txt') == '2'

    def test_upgrade_from_missing_basis(self, project):
        write('state.json', json.dumps(
            {'values': {'batch': 1}, 'getargs': ['produce', 'batch']}))
        write('dodo.py', CLI_DODO)
        assert invoke_cli(project, 'run', '-v', '1').returncode == 0

        # simulate an old DB: no getargs basis recorded
        dep = open_db(str(project / '.doit.db'))
        dep._set('consume', GETARGS_KEY, None)
        dep.close()

        upgraded = invoke_cli(project, 'run', '-v', '1')
        assert upgraded.returncode == 0, upgraded.stderr
        assert '.  consume' in upgraded.stdout

        stable = invoke_cli(project, 'run', '-v', '1')
        assert stable.returncode == 0
        assert '-- consume' in stable.stdout


# ---------------------------------------------------------------------------
# lifecycle matrix through the real runner


LIFECYCLE_DODO = '''
import json
import os

STATE = json.load(open('state.json'))
COUNT = {'produce': 0, 'consume': 0}

def task_produce():
    def produce():
        COUNT['produce'] += 1
        return dict(STATE['values'])
    return {'actions': [(produce,), (lambda: 'trailer',)]}

def task_consume():
    def consume(v):
        COUNT['consume'] += 1
        with open('out.txt', 'w') as handle:
            handle.write(json.dumps(v, sort_keys=True))
        with open('counts.json', 'w') as handle:
            handle.write(json.dumps(COUNT))
        if STATE.get('fail'):
            raise RuntimeError('deliberate failure')
    return {'actions': [(consume,)],
            'getargs': {'v': tuple(STATE['getargs'])},
            'verbosity': 2}
'''


class TestLifecycle:

    def _run(self, project, dep_file, state, sel=('produce', 'consume')):
        write('state.json', json.dumps(state))
        write('dodo.py', LIFECYCLE_DODO)
        module = load_module(str(project / 'dodo.py'))
        return run_tasks(load_tasks(module), dep_file, sel=list(sel))

    def test_key_lifecycle(self, project, depfile_name):
        state = {'values': {'a': 1, 'b': 2}, 'getargs': ['produce', 'a']}
        rc, out = self._run(project, depfile_name, state)
        assert rc == 0 and read('out.txt') == '1'
        # stable
        rc, out = self._run(project, depfile_name, state)
        assert '-- consume' in out
        # unselected value changed: skip
        state['values']['b'] = 20
        rc, out = self._run(project, depfile_name, state)
        assert '-- consume' in out
        # selected value changed: run
        state['values']['a'] = 10
        rc, out = self._run(project, depfile_name, state)
        assert '.  consume' in out and read('out.txt') == '10'

    def test_whole_dict_lifecycle(self, project, depfile_name):
        state = {'values': {'a': 1, 'b': 2}, 'getargs': ['produce', None]}
        rc, out = self._run(project, depfile_name, state)
        assert rc == 0
        # same content, different insertion order: skip
        state['values'] = {'b': 2, 'a': 1}
        rc, out = self._run(project, depfile_name, state)
        assert '-- consume' in out
        # any value change: run
        state['values'] = {'b': 2, 'a': 9}
        rc, out = self._run(project, depfile_name, state)
        assert '.  consume' in out
        # empty dict is a valid whole-dict value
        state['values'] = {}
        rc, out = self._run(project, depfile_name, state)
        assert '.  consume' in out and read('out.txt') == '{}'
        rc, out = self._run(project, depfile_name, state)
        assert '-- consume' in out

    def test_failure_does_not_advance_basis(self, project, depfile_name):
        state = {'values': {'a': 1, 'b': 2}, 'getargs': ['produce', 'a']}
        rc, out = self._run(project, depfile_name, state)
        assert rc == 0 and read('out.txt') == '1'
        # change value and fail
        state = {'values': {'a': 2, 'b': 2}, 'getargs': ['produce', 'a'],
                 'fail': True}
        rc, out = self._run(project, depfile_name, state)
        assert rc != 0
        # the success basis still describes the old delivered value; retry
        # without failure must run again (not be treated as up-to-date).
        state.pop('fail')
        rc, out = self._run(project, depfile_name, state)
        assert '.  consume' in out and read('out.txt') == '2'
        rc, out = self._run(project, depfile_name, state)
        assert '-- consume' in out


GROUP_DODO = '''
import json

STATE = json.load(open('state.json'))

def task_g():
    def make(name):
        def value():
            return {'x': STATE['members'][name]}
        return value
    for name in STATE['members']:
        yield {'basename': 'g', 'name': name, 'actions': [(make(name),)]}

def task_consume():
    def consume(v):
        with open('out.txt', 'w') as handle:
            handle.write(json.dumps(v, sort_keys=True))
    return {'actions': [(consume,)],
            'getargs': {'v': ('g', STATE['key'])},
            'verbosity': 2}
'''


class TestGroupLifecycle:

    def _run(self, project, dep_file, state):
        write('state.json', json.dumps(state))
        write('dodo.py', GROUP_DODO)
        module = load_module(str(project / 'dodo.py'))
        return run_tasks(load_tasks(module), dep_file, sel=['g', 'consume'])

    def test_member_add_remove_and_key(self, project, depfile_name):
        state = {'members': {'a': 1, 'b': 2}, 'key': 'x'}
        rc, out = self._run(project, depfile_name, state)
        assert rc == 0
        assert json.loads(read('out.txt')) == {'a': 1, 'b': 2}
        rc, out = self._run(project, depfile_name, state)
        assert '-- consume' in out
        # member value change autoloaded by group
        state['members']['b'] = 3
        rc, out = self._run(project, depfile_name, state)
        assert json.loads(read('out.txt')) == {'a': 1, 'b': 3}
        # add member
        state['members']['c'] = 4
        rc, out = self._run(project, depfile_name, state)
        assert json.loads(read('out.txt')) == {'a': 1, 'b': 3, 'c': 4}
        # remove member
        del state['members']['a']
        rc, out = self._run(project, depfile_name, state)
        assert json.loads(read('out.txt')) == {'b': 3, 'c': 4}

    def test_group_whole_dict(self, project, depfile_name):
        state = {'members': {'a': 1, 'b': 2}, 'key': None}
        rc, out = self._run(project, depfile_name, state)
        assert json.loads(read('out.txt')) == {'a': {'x': 1}, 'b': {'x': 2}}
        rc, out = self._run(project, depfile_name, state)
        assert '-- consume' in out


SETUP_DODO = '''
import json

STATE = json.load(open('state.json'))
RUNS = []

def task_produce():
    def produce():
        RUNS.append('produce')
        with open('runs.txt', 'w') as handle:
            handle.write(json.dumps(RUNS))
        return {'a': STATE['a']}
    return {'actions': [(produce,)]}

def task_consume():
    def consume(a):
        RUNS.append('consume')
        with open('runs.txt', 'w') as handle:
            handle.write(json.dumps(RUNS))
        with open('out.txt', 'w') as handle:
            handle.write(str(a))
    return {'actions': [(consume,)],
            'getargs': {'a': ('produce', 'a')},
            'uptodate': [True],
            'verbosity': 2}
'''


class TestSetupRhythm:

    def test_uptodate_consumer_does_not_probe_producer(
            self, project, depfile_name):
        from doit.task import Task
        # up-to-date consumer means the setup (producer) must not be run just
        # to read a value.  Use the runner directly to observe ordering.
        write('state.json', json.dumps({'a': 1}))
        write('dodo.py', SETUP_DODO)
        module = load_module(str(project / 'dodo.py'))
        tasks = {t.name: t for t in load_tasks(module)}
        dep = open_db(depfile_name)
        # first: force success of both
        rc, out = run_tasks(load_tasks(module), depfile_name,
                            sel=['produce', 'consume'])
        assert rc == 0
        dep.close()
        # second run, only consume selected: uptodate[True] means skip and
        # the producer is not scheduled on demand.
        os.remove('runs.txt')
        rc, out = run_tasks(list(tasks.values()), depfile_name, sel=['consume'])
        assert '-- consume' in out
        assert '.  produce' not in out


# ---------------------------------------------------------------------------
# concurrency determinism: threads interleaved with a barrier


BARRIER_DODO = '''
import json
import threading

EVENTS = threading.Barrier(2, timeout=10)

def task_produce():
    return {'actions': [(lambda: {'batch': {'parts': [1, 2]}},)]}

def _consumer(name):
    def consume(batch):
        # both consumers are inside their action simultaneously
        EVENTS.wait()
        batch['parts'].append(name)
        with open('out_%s.txt' % name, 'w') as handle:
            handle.write(json.dumps(batch, sort_keys=True))
    return consume

def task_c1():
    return {'actions': [(_consumer('one'),)],
            'getargs': {'batch': ('produce', 'batch')},
            'verbosity': 2}

def task_c2():
    return {'actions': [(_consumer('two'),)],
            'getargs': {'batch': ('produce', 'batch')},
            'verbosity': 2}
'''


class TestThreadBarrier:

    def test_interleaved_consumers_are_independent(self, project,
                                                   depfile_name):
        write('dodo.py', BARRIER_DODO)
        module = load_module(str(project / 'dodo.py'))
        tasks = load_tasks(module)
        rc, out = run_tasks(tasks, depfile_name,
                            sel=['produce', 'c1', 'c2'],
                            num_process=2, par_type='thread')
        assert rc == 0, out
        # neither consumer saw the other's mutation
        assert json.loads(read('out_one.txt')) == {'parts': [1, 2, 'one']}
        assert json.loads(read('out_two.txt')) == {'parts': [1, 2, 'two']}
        assert saved_values(depfile_name, 'produce') == \
            {'batch': {'parts': [1, 2]}}

    def test_interleaved_rerun_skips(self, project, depfile_name):
        write('dodo.py', BARRIER_DODO)
        module = load_module(str(project / 'dodo.py'))
        tasks = load_tasks(module)
        run_tasks(tasks, depfile_name, sel=['produce', 'c1', 'c2'],
                  num_process=2, par_type='thread')
        module = load_module(str(project / 'dodo.py'))
        tasks = load_tasks(module)
        rc, out = run_tasks(tasks, depfile_name, sel=['produce', 'c1', 'c2'],
                            num_process=2, par_type='thread')
        assert rc == 0, out
        assert '-- c1' in out
        assert '-- c2' in out


# ---------------------------------------------------------------------------
# wide values: in-flight retention and DB footprint


class TestWideValueCost:

    @staticmethod
    def _wide(n):
        return {('k%d' % i): {'parts': list(range(20)),
                              'nested': {'a': i, 'b': [i, i + 1]}}
                for i in range(n)}

    def test_basis_is_fixed_size(self, tmp_path):
        from doit.getargs import _basis, _document
        dep = Dependency(DbmDB, str(tmp_path / 'db'))
        producer = Task('produce', [(lambda: None,)])
        small = self._wide(1)
        large = self._wide(500)
        producer.values = {'v': small}
        dep.save_success(producer)
        small_basis = basis(dep, {'produce': producer}, {'v': ('produce', 'v')})
        producer.values = {'v': large}
        dep.save_success(producer)
        large_basis = basis(dep, {'produce': producer}, {'v': ('produce', 'v')})
        # same fixed size regardless of value width
        assert len(small_basis) == len(large_basis)
        assert large_basis.startswith('v1:')
        dep.close()

    def test_db_entry_does_not_embed_value(self, tmp_path):
        dep = Dependency(DbmDB, str(tmp_path / 'db'))
        producer = Task('produce', [(lambda: None,)])
        producer.values = {'v': self._wide(200)}
        dep.save_success(producer)
        consumer = Task('consume', [(lambda: None,)],
                        getargs={'v': ('produce', 'v')})
        dep.save_success(consumer, tasks_dict={'produce': producer})
        stored = dep._get('consume', GETARGS_KEY)
        assert stored.startswith('v1:') and len(stored) == 67  # 'v1:' + sha256
        dep.close()

    def test_many_consumers_fixed_storage(self, tmp_path):
        dep = Dependency(DbmDB, str(tmp_path / 'db'))
        producer = Task('produce', [(lambda: None,)])
        producer.values = {'v': self._wide(200)}
        dep.save_success(producer)
        consumers = []
        for i in range(20):
            c = Task('c%d' % i, [(lambda: None,)],
                     getargs={'v': ('produce', 'v')})
            dep.save_success(c, tasks_dict={'produce': producer})
            consumers.append(c)
        sizes = {len(dep._get(c.name, GETARGS_KEY)) for c in consumers}
        assert sizes == {67}
        dep.close()


class TestManyConsumers:

    @pytest.mark.parametrize('par_type,num', [(None, 0), ('thread', 4),
                                              ('process', 4)])
    def test_four_consumers_keep_pristine_values(self, project, depfile_name,
                                                 par_type, num):
        dodo = '''
import json

def task_produce():
    def produce():
        return {'batch': {'parts': [1, 2]}}
    def trailer():
        return 'same-trailer'
    return {'actions': [(produce,), (trailer,)]}

def make(name):
    def task():
        def consume(batch):
            batch['parts'].append(name)
            with open('out_%s.txt' % name, 'w') as handle:
                handle.write(json.dumps(batch, sort_keys=True))
        return {'actions': [(consume,)],
                'getargs': {'batch': ('produce', 'batch')},
                'verbosity': 2}
    return task

for _name in ('a', 'b', 'c', 'd'):
    globals()['task_c' + _name] = make(_name)
'''
        write('dodo.py', dodo)
        module = load_module(str(project / 'dodo.py'))
        tasks = load_tasks(module)
        sel = ['produce', 'ca', 'cb', 'cc', 'cd']
        rc, out = run_tasks(tasks, depfile_name, sel=sel,
                            num_process=num, par_type=par_type or 'process')
        assert rc == 0, out
        for name in 'abcd':
            assert json.loads(read('out_%s.txt' % name)) == \
                {'parts': [1, 2, name]}
        assert saved_values(depfile_name, 'produce') == \
            {'batch': {'parts': [1, 2]}}

        module = load_module(str(project / 'dodo.py'))
        tasks = load_tasks(module)
        rc, out = run_tasks(tasks, depfile_name, sel=sel,
                            num_process=num, par_type=par_type or 'process')
        assert rc == 0, out
        for name in 'abcd':
            assert '-- c%s' % name in out


class TestAliasWithinArgument:
    def test_internal_alias_preserved(self, tmp_path):
        dep = Dependency(DbmDB, str(tmp_path / 'db'))
        producer = Task('produce', [(lambda: None,)])
        shared = [1, 2]
        producer.values = {'v': {'left': shared, 'right': shared}}
        dep.save_success(producer)
        _, frozen = freeze(dep, {'produce': producer},
                           {'v': ('produce', 'v')})
        got = deliver(frozen)['v']
        # after a JSON freeze, aliases are independent values; content equal
        assert got['left'] == got['right'] == [1, 2]
        dep.close()


class TestProducerValueSnapshotted:
    def test_later_mutation_of_returned_object_does_not_change_saved_value(
            self, tmp_path):
        dep = Dependency(DbmDB, str(tmp_path / 'db'))
        returned = {'batch': {'parts': [1, 2]}}
        producer = Task('produce', [])
        producer.values = returned
        dep.save_success(producer)
        # the action may keep the returned object and mutate it after saving
        returned['batch']['parts'].append(99)
        assert dep.get_values('produce') == {'batch': {'parts': [1, 2]}}
        dep.close()
