"""End-to-end tests for glob_dep through the real doit commands/runner."""

import os
import subprocess
import sys
from io import StringIO

import pytest

from doit.cmd_run import Run
from doit.dependency import DbmDB, Dependency
from tests.conftest import CmdFactory


# ---------------------------------------------------------------------------
# helpers


def write(path, content, mtime=None):
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    with open(path, 'w') as handle:
        handle.write(content)
    if mtime is not None:
        os.utime(path, (mtime, mtime))


def read(path):
    with open(path) as handle:
        return handle.read()


@pytest.fixture
def project(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    return tmp_path


def make_etl_tasks(log, gen_ok=True):
    """producer/consumer ETL tasks, recording execution order."""
    from doit.task import Task

    def generate(targets):
        os.makedirs('landing/sub', exist_ok=True)
        write('landing/a.csv', 'a')
        write('landing/sub/b.csv', 'b')
        log.append('generate')
        return gen_ok

    def merge(dependencies):
        log.append(('merge', list(dependencies)))
        write('merged.txt', '\n'.join(dependencies))
        return True

    producer = Task('generate', [generate],
                    targets=['landing/a.csv', 'landing/sub/b.csv'])
    consumer = Task('merge', [merge], glob_dep=['landing/**/*.csv'],
                    targets=['merged.txt'])
    return [producer, consumer]


def run_tasks(tasks, dep_file, sel=None, num_process=0, par_type='process',
              continue_=False):
    output = StringIO()
    cmd = CmdFactory(Run, outstream=output, backend='dbm', dep_file=dep_file,
                     task_list=tasks, sel_tasks=sel)
    rc = cmd._execute(output, verbosity=0, num_process=num_process,
                      par_type=par_type, continue_=continue_)
    return rc, output.getvalue()


# ---------------------------------------------------------------------------
# ETL lifecycle


class TestEtlLifecycle:

    def test_generate_then_merge_then_skip(self, project, depfile_name):
        log = []
        tasks = make_etl_tasks(log)
        rc, out = run_tasks(tasks, depfile_name)
        assert rc == 0
        # producer ran first, consumer saw both partitions
        assert log[0] == 'generate'
        merge_deps = log[1][1]
        assert len(merge_deps) == 2
        assert all(os.path.isabs(p) for p in merge_deps)

        # second run: consumer skipped
        log2 = []
        tasks2 = make_etl_tasks(log2)
        rc, out = run_tasks(tasks2, depfile_name)
        assert rc == 0
        assert '-- merge' in out

    def test_add_partition_reruns_merge(self, project, depfile_name):
        log = []
        run_tasks(make_etl_tasks(log), depfile_name)
        write('landing/new.csv', 'n')
        log2 = []
        rc, out = run_tasks(make_etl_tasks(log2), depfile_name)
        assert rc == 0
        assert ('merge',) == tuple(x[0] for x in log2 if isinstance(x, tuple))
        assert len(log2[-1][1]) == 3

    def test_delete_partition_reruns(self, project, depfile_name):
        write('landing/a.csv', 'a')
        write('landing/b.csv', 'b')
        from doit.task import Task
        seen = []
        def make_merge():
            def merge(dependencies):
                seen.append(list(dependencies))
                write('m.txt', 'm')
                return True
            return merge
        run_tasks([Task('merge', [make_merge()], glob_dep=['landing/*.csv'])],
                  depfile_name)
        assert len(seen[-1]) == 2
        os.remove('landing/b.csv')
        rc, out = run_tasks(
            [Task('merge', [make_merge()], glob_dep=['landing/*.csv'])],
            depfile_name)
        assert rc == 0
        assert '.  merge' in out
        assert len(seen[-1]) == 1

    def test_rename_partition_reruns(self, project, depfile_name):
        write('landing/a.csv', 'a')
        write('landing/b.csv', 'b')
        from doit.task import Task
        def merge(dependencies):
            write('m.txt', 'm')
            return True
        consumer = Task('merge', [merge], glob_dep=['landing/*.csv'])
        run_tasks([consumer], depfile_name)
        os.rename('landing/a.csv', 'landing/renamed.csv')
        consumer2 = Task('merge', [merge], glob_dep=['landing/*.csv'])
        rc, out = run_tasks([consumer2], depfile_name)
        assert rc == 0
        assert '.  merge' in out

    def test_change_pattern_reruns(self, project, depfile_name):
        write('landing/a.csv', 'a')
        write('other/b.csv', 'b')
        from doit.task import Task
        def merge(dependencies):
            write('m.txt', 'm')
            return True
        run_tasks([Task('merge', [merge], glob_dep=['landing/*.csv'])],
                  depfile_name)
        rc, out = run_tasks(
            [Task('merge', [merge], glob_dep=['other/*.csv'])], depfile_name)
        assert rc == 0
        assert '.  merge' in out

    @pytest.mark.parametrize('num_process,par_type', [(0, 'process'),
                                                      (2, 'process'),
                                                      (2, 'thread')])
    def test_incremental_serial_and_parallel(self, project, depfile_name,
                                             num_process, par_type):
        write('landing/a.csv', 'a')
        from doit.task import Task
        def merge(dependencies):
            write('m.txt', '\n'.join(dependencies))
            return True
        run_tasks([Task('merge', [merge], glob_dep=['landing/*.csv'])],
                  depfile_name, num_process=num_process, par_type=par_type)
        rc, out = run_tasks(
            [Task('merge', [merge], glob_dep=['landing/*.csv'])],
            depfile_name, num_process=num_process, par_type=par_type)
        assert rc == 0
        assert '-- merge' in out


# ---------------------------------------------------------------------------
# producer ordering / failures


class TestProducerOrdering:

    def test_producer_runs_before_consumer(self, project, depfile_name):
        log = []
        tasks = make_etl_tasks(log)
        rc, out = run_tasks(tasks, depfile_name)
        assert rc == 0
        assert log[0] == 'generate'
        assert log[1][0] == 'merge'

    def test_producer_failure_blocks_consumer(self, project, depfile_name):
        log = []
        tasks = make_etl_tasks(log, gen_ok=False)
        rc, out = run_tasks(tasks, depfile_name)
        assert rc != 0
        # consumer never ran
        assert not any(isinstance(x, tuple) for x in log)

    def test_consumer_failure_does_not_save_success(self, project,
                                                    depfile_name):
        write('landing/a.csv', 'a')
        from doit.task import Task

        def fail(dependencies):
            raise Exception('boom')
        task = Task('merge', [fail], glob_dep=['landing/*.csv'])
        rc, out = run_tasks([task], depfile_name)
        assert rc != 0
        dep = Dependency(DbmDB, depfile_name)
        assert dep._get('merge', 'glob_dep:') is None
        dep.close()
        # a later successful run still computes
        def ok(dependencies):
            write('m.txt', 'm')
            return True
        task2 = Task('merge', [ok], glob_dep=['landing/*.csv'])
        rc, out = run_tasks([task2], depfile_name)
        assert rc == 0
        assert '.  merge' in out


# ---------------------------------------------------------------------------
# strong content guarantee (input changes while action runs)


def _mutate_midaction(task_name, mutate):
    """build a task that mutates its own input while running"""
    from doit.task import Task

    def action(dependencies):
        mutate()
        write('out.txt', 'o')
        return True
    return Task(task_name, [action], glob_dep=['landing/**/*.csv'],
                targets=['out.txt'])


class TestMidActionChanges:

    def _change_same_size(self, path):
        st = os.stat(path)
        data = open(path, 'rb').read()
        open(path, 'wb').write(bytes([data[0] ^ 0xFF]) + data[1:])
        os.utime(path, (st.st_atime, st.st_mtime))

    @pytest.mark.parametrize('num_process,par_type',
                             [(0, 'process'), (2, 'process'),
                              (2, 'thread')])
    def test_content_change_same_size_mtime(self, project, depfile_name,
                                            num_process, par_type):
        write('landing/a.csv', 'aaaa')
        task = _mutate_midaction(
            'merge', lambda: self._change_same_size('landing/a.csv'))
        rc, out = run_tasks([task], depfile_name, num_process=num_process,
                            par_type=par_type)
        assert rc != 0
        assert 'changed while it was running' in out
        dep = Dependency(DbmDB, depfile_name)
        assert dep._get('merge', 'glob_dep:') is None
        dep.close()

    @pytest.mark.parametrize('num_process,par_type', [(0, 'process'),
                                                      (2, 'process'),
                                                      (2, 'thread')])
    def test_empty_to_nonempty(self, project, depfile_name, num_process,
                               par_type):
        os.makedirs('landing')
        task = _mutate_midaction(
            'merge', lambda: write('landing/new.csv', 'n'))
        rc, out = run_tasks([task], depfile_name, num_process=num_process,
                            par_type=par_type)
        assert rc != 0
        assert 'changed while it was running' in out

    def test_add_file_midaction(self, project, depfile_name):
        write('landing/a.csv', 'a')
        task = _mutate_midaction(
            'merge', lambda: write('landing/b.csv', 'b'))
        rc, out = run_tasks([task], depfile_name)
        assert rc != 0

    def test_remove_file_midaction(self, project, depfile_name):
        write('landing/a.csv', 'a')
        write('landing/b.csv', 'b')
        task = _mutate_midaction(
            'merge', lambda: os.remove('landing/b.csv'))
        rc, out = run_tasks([task], depfile_name)
        assert rc != 0

    def test_rename_midaction(self, project, depfile_name):
        write('landing/a.csv', 'a')
        task = _mutate_midaction(
            'merge', lambda: os.rename('landing/a.csv', 'landing/z.csv'))
        rc, out = run_tasks([task], depfile_name)
        assert rc != 0

    def test_next_legal_run_still_computes(self, project, depfile_name):
        write('landing/a.csv', 'aaaa')
        task = _mutate_midaction(
            'merge', lambda: self._change_same_size('landing/a.csv'))
        run_tasks([task], depfile_name)
        from doit.task import Task

        def ok(dependencies):
            write('out.txt', 'o')
            return True
        task2 = Task('merge', [ok], glob_dep=['landing/**/*.csv'],
                     targets=['out.txt'])
        rc, out = run_tasks([task2], depfile_name)
        assert rc == 0
        assert '.  merge' in out


# ---------------------------------------------------------------------------
# fresh process sees the same state (native dependency DB)


class TestFreshProcess:

    def test_fresh_process_skips(self, project, depfile_name):
        write('landing/a.csv', 'a')
        from doit.task import Task
        def merge(dependencies):
            write('m.txt', '\n'.join(dependencies))
            return True
        run_tasks([Task('merge', [merge], glob_dep=['landing/*.csv'])],
                  depfile_name)
        # new Dependency instance simulates a fresh process reading the db
        dep = Dependency(DbmDB, depfile_name)
        task = Task('merge', [merge], glob_dep=['landing/*.csv'])
        assert dep.get_status(task, {}).status == 'up-to-date'
        dep.close()

    def test_fresh_process_detects_new_file(self, project, depfile_name):
        write('landing/a.csv', 'a')
        from doit.task import Task
        def merge(dependencies):
            write('m.txt', '\n'.join(dependencies))
            return True
        run_tasks([Task('merge', [merge], glob_dep=['landing/*.csv'])],
                  depfile_name)
        write('landing/b.csv', 'b')
        dep = Dependency(DbmDB, depfile_name)
        task = Task('merge', [merge], glob_dep=['landing/*.csv'])
        assert dep.get_status(task, {}).status == 'run'
        dep.close()


# ---------------------------------------------------------------------------
# real CLI in a separate python process (fresh dependency DB read)


DODO_ETL = '''
import os

def generate():
    os.makedirs('landing/sub', exist_ok=True)
    with open('landing/a.csv', 'w') as f:
        f.write('a')
    with open('landing/sub/b.csv', 'w') as f:
        f.write('b')
    return True

def merge(dependencies):
    with open('merged.txt', 'w') as f:
        f.write('\\n'.join(dependencies))
    return True

def task_generate():
    return {'actions': [generate],
            'targets': ['landing/a.csv', 'landing/sub/b.csv']}

def task_merge():
    return {'actions': [merge],
            'glob_dep': ['landing/**/*.csv'],
            'targets': ['merged.txt']}
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

    def test_etl_cycle_in_subprocesses(self, project):
        write('dodo.py', DODO_ETL)
        first = invoke_cli(project, 'run', '-v', '1')
        assert first.returncode == 0, first.stderr
        assert '.  generate' in first.stdout
        assert '.  merge' in first.stdout
        merged = read('merged.txt').splitlines()
        assert len(merged) == 2
        assert all(os.path.isabs(p) for p in merged)

        # fresh process: consumer is up to date
        second = invoke_cli(project, 'run', '-v', '1')
        assert second.returncode == 0
        assert '-- merge' in second.stdout

        # add a partition in yet another process
        write('landing/sub/new.csv', 'n')
        third = invoke_cli(project, 'run', '-v', '1')
        assert third.returncode == 0
        assert '.  merge' in third.stdout
        assert len(read('merged.txt').splitlines()) == 3

    def test_info_in_subprocess(self, project):
        write('dodo.py', DODO_ETL)
        assert invoke_cli(project, 'run', '-v', '1').returncode == 0
        write('landing/sub/new.csv', 'n')
        info = invoke_cli(project, 'info', 'merge')
        assert info.returncode == 1
        assert 'glob_dep files were added' in info.stdout

    def test_parallel_subprocesses(self, project):
        write('dodo.py', DODO_ETL)
        first = invoke_cli(project, 'run', '-n', '2')
        assert first.returncode == 0, first.stderr
        # reopen with a new process; both tasks handled consistently
        second = invoke_cli(project, 'run', '-n', '2')
        assert second.returncode == 0
        assert '-- merge' in second.stdout
