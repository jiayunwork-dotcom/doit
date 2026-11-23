"""Tests for the ``glob_dep`` pattern-based file dependency feature."""

import os
from io import StringIO

import pytest

from doit.dependency import (
    DbmDB, JsonDB, SqliteDB, GlobDependencyChanged,
    GLOB_DEP_KEY, observe_glob_state,
)
from doit.globdep import (
    GlobError, match_target, normalize_path, scan_state,
)
from doit.task import Task
from doit.control import TaskControl, TaskDispatcher
from doit.cmd_info import Info
from doit.cmd_list import List
from doit.task import InvalidTask
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


@pytest.fixture
def workdir(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    return tmp_path


@pytest.fixture(params=[JsonDB, DbmDB, SqliteDB])
def any_dep_manager(request, tmp_path_factory):
    from tests.conftest import dep_manager_fixture
    return dep_manager_fixture(request, request.param, tmp_path_factory)


# ---------------------------------------------------------------------------
# glob expansion semantics


class TestGlobScan:

    def test_recursive_matches_only_regular_files(self, workdir):
        write('landing/a.csv', 'a')
        write('landing/sub/b.csv', 'b')
        write('landing/sub/deep/c.csv', 'c')
        write('landing/notes.txt', 'x')
        os.makedirs('landing/emptydir')
        state = scan_state(['landing/**/*.csv'])
        expected = sorted(normalize_path(p) for p in
                          ['landing/a.csv', 'landing/sub/b.csv',
                           'landing/sub/deep/c.csv'])
        assert list(state.files) == expected
        assert set(state.manifest) == set(expected)

    def test_no_magic_star_question_brackets(self, workdir):
        write('data/a1.txt', 'x')
        write('data/a2.txt', 'x')
        write('data/b1.txt', 'x')
        state = scan_state(['data/a?.txt'])
        assert [os.path.basename(p) for p in state.files] == ['a1.txt', 'a2.txt']
        state = scan_state(['data/*[12].txt'])
        # '*' may match the 'b' of b1.txt, then '[12]' matches '1'
        assert len(state.files) == 3
        state = scan_state(['data/*1.txt'])
        assert len(state.files) == 2

    def test_empty_match_is_valid(self, workdir):
        os.makedirs('landing')
        state = scan_state(['landing/**/*.csv'])
        assert state.files == ()
        assert state.manifest == {}

    def test_duplicate_pattern_hits_counted_once(self, workdir):
        write('landing/a.csv', 'x')
        state = scan_state(['landing/**/*.csv', 'landing/a.csv',
                            'landing/*.csv'])
        assert len(state.files) == 1

    def test_relative_and_absolute_spellings_are_same_file(self, workdir):
        write('landing/a.csv', 'x')
        absdir = str(workdir / 'landing')
        state = scan_state(['landing/*.csv', absdir + '/*.csv',
                            './landing/../landing/*.csv'])
        assert len(state.files) == 1

    def test_directory_symlink_not_followed(self, workdir):
        write('real/a.csv', 'x')
        write('landing/local.csv', 'y')
        os.symlink(str(workdir / 'real'), str(workdir / 'landing' / 'linkdir'))
        state = scan_state(['landing/**/*.csv'])
        names = [os.path.basename(p) for p in state.files]
        assert names == ['local.csv']

    def test_file_symlink_rejected(self, workdir):
        write('target.csv', 'x')
        os.symlink(str(workdir / 'target.csv'), str(workdir / 'link.csv'))
        with pytest.raises(GlobError) as exc:
            scan_state(['link.csv'])
        assert 'symbolic link' in str(exc.value)

    def test_file_symlink_rejected_through_recursive(self, workdir):
        write('landing/real.csv', 'x')
        os.symlink(str(workdir / 'landing' / 'real.csv'),
                   str(workdir / 'landing' / 'alias.csv'))
        with pytest.raises(GlobError):
            scan_state(['landing/**/*.csv'])

    def test_hidden_files_not_matched_by_star(self, workdir):
        write('landing/.hidden.csv', 'h')
        write('landing/shown.csv', 's')
        state = scan_state(['landing/*.csv'])
        assert [os.path.basename(p) for p in state.files] == ['shown.csv']
        state = scan_state(['landing/.*.csv'])
        assert [os.path.basename(p) for p in state.files] == ['.hidden.csv']

    def test_permission_error_not_downgraded(self, workdir):
        if os.geteuid() == 0:  # pragma: no cover
            pytest.skip('root can read anything')
        write('locked/a.csv', 'x')
        os.chmod('locked', 0)
        try:
            with pytest.raises(GlobError):
                scan_state(['locked/*.csv'])
        finally:
            os.chmod('locked', 0o755)

    def test_scan_reads_in_chunks(self, workdir, monkeypatch):
        write('landing/big.csv', 'x' * 5000)
        calls = []
        import doit.dependency as dependency
        import doit.globdep as globdep
        real_md5 = dependency.get_file_md5

        def spy(path):
            calls.append(path)
            real_md5(path)
        # globdep imports the hash function from dependency lazily
        monkeypatch.setattr(dependency, 'get_file_md5', spy)
        monkeypatch.setattr(globdep, '_file_md5', spy)
        state = scan_state(['landing/**/*.csv'])
        # one hash per unique file, not per pattern
        assert len(calls) == 1
        assert len(state.files) == 1

    def test_one_hash_per_unique_path_overlapping_patterns(self, workdir,
                                                           monkeypatch):
        for name in ['a', 'b', 'c']:
            write('landing/sub/%s.csv' % name, name)
        calls = []
        import doit.dependency as dependency
        import doit.globdep as globdep
        real = dependency.get_file_md5

        def spy(path):
            calls.append(path)
            return real(path)
        monkeypatch.setattr(globdep, '_file_md5', spy)
        scan_state(['landing/**/*.csv', 'landing/sub/*.csv',
                    'landing/sub/**/*.csv'])
        assert len(calls) == 3
        assert len(calls) == len(set(calls))

    def test_memory_does_not_scale_with_file_bytes(self, workdir, monkeypatch):
        # one 4 MB file vs 4 x 1 MB files: resident observation memory is
        # proportional to paths, not to bytes.
        import tracemalloc
        with open('landing_big.bin', 'wb') as handle:
            handle.write(b'x' * (4 * 1024 * 1024))
        os.makedirs('landing_many')
        for index in range(4):
            with open('landing_many/f%d.bin' % index, 'wb') as handle:
                handle.write(bytes([index]) * (1024 * 1024))

        tracemalloc.start()
        scan_state(['landing_big.bin'])
        _cur_big, peak_big = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        tracemalloc.start()
        scan_state(['landing_many/*.bin'])
        _cur_many, peak_many = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        # both stay tiny compared to the byte volume; the single big file
        # must not be loaded whole (well below 1 MB of traced memory).
        assert peak_big < 1024 * 1024
        assert peak_many < 1024 * 1024


class TestPatternMatchTarget:

    def test_literal(self, workdir):
        assert match_target('landing/*.csv', 'landing/a.csv')
        assert not match_target('landing/*.csv', 'landing/sub/a.csv')

    def test_recursive(self, workdir):
        assert match_target('landing/**/*.csv', 'landing/sub/deep/a.csv')
        assert match_target('landing/**/*.csv', 'landing/a.csv')
        assert not match_target('landing/**/*.csv', 'landing/a.txt')

    def test_path_aliases(self, workdir):
        abs_target = str(workdir / 'landing' / 'a.csv')
        assert match_target('./landing/../landing/*.csv', 'landing/a.csv')
        assert match_target('landing/*.csv', abs_target)
        assert match_target(str(workdir / 'landing') + '/*.csv',
                            'landing/a.csv')

    def test_does_not_need_to_exist(self, workdir):
        assert match_target('landing/**/*.csv', 'landing/not/created/yet.csv')


# ---------------------------------------------------------------------------
# task attribute validation


class TestTaskAttribute:

    def test_default_empty(self):
        task = Task('t', [])
        assert task.glob_dep == ()
        assert task.glob_files is None
        assert task.glob_state is None

    def test_accepts_list_and_tuple(self):
        assert Task('t', [], glob_dep=['a/**/*.csv']).glob_dep == ('a/**/*.csv',)
        assert Task('t', [], glob_dep=('a/**/*.csv',)).glob_dep == ('a/**/*.csv',)

    def test_rejects_non_sequence(self):
        with pytest.raises(InvalidTask):
            Task('t', [], glob_dep='a/**/*.csv')
        with pytest.raises(InvalidTask):
            Task('t', [], glob_dep=42)

    def test_rejects_non_string_element(self):
        with pytest.raises(InvalidTask):
            Task('t', [], glob_dep=[123])

    def test_rejects_empty_string(self):
        with pytest.raises(InvalidTask):
            Task('t', [], glob_dep=[''])

    def test_deduplicates(self):
        task = Task('t', [], glob_dep=['a/*.csv', 'a/*.csv', 'b/*.csv'])
        assert task.glob_dep == ('a/*.csv', 'b/*.csv')

    def test_update_deps_extends(self):
        task = Task('t', [], glob_dep=['a/*.csv'])
        task.update_deps({'glob_dep': ['b/*.csv']})
        assert task.glob_dep == ('a/*.csv', 'b/*.csv')


# ---------------------------------------------------------------------------
# dependency status and persistence


class TestDependencyStatus:

    def _task(self, *patterns, file_dep=()):
        return Task('globtask', [], file_dep=list(file_dep),
                    glob_dep=list(patterns))

    def test_first_run(self, any_dep_manager, workdir):
        write('landing/a.csv', 'a')
        task = self._task('landing/**/*.csv')
        status = any_dep_manager.get_status(task, {})
        assert status.status == 'run'
        assert task.glob_files == (normalize_path('landing/a.csv'),)

    def test_up_to_date_then_unchanged(self, any_dep_manager, workdir):
        write('landing/a.csv', 'a')
        task = self._task('landing/**/*.csv')
        any_dep_manager.get_status(task, {})
        any_dep_manager.save_success(task)
        assert any_dep_manager.get_status(task, {}).status == 'up-to-date'

    def test_added_file(self, any_dep_manager, workdir):
        write('landing/a.csv', 'a')
        task = self._task('landing/**/*.csv')
        any_dep_manager.get_status(task, {})
        any_dep_manager.save_success(task)
        write('landing/b.csv', 'b')
        status = any_dep_manager.get_status(task, {})
        assert status.status == 'run'
        log = any_dep_manager.get_status(task, {}, get_log=True)
        assert log.reasons['glob_dep_added'] == [normalize_path('landing/b.csv')]

    def test_removed_file(self, any_dep_manager, workdir):
        write('landing/a.csv', 'a')
        write('landing/b.csv', 'b')
        task = self._task('landing/**/*.csv')
        any_dep_manager.get_status(task, {})
        any_dep_manager.save_success(task)
        os.remove('landing/b.csv')
        log = any_dep_manager.get_status(task, {}, get_log=True)
        assert log.status == 'run'
        assert log.reasons['glob_dep_removed'] == [normalize_path('landing/b.csv')]

    def test_renamed_file(self, any_dep_manager, workdir):
        write('landing/a.csv', 'a')
        task = self._task('landing/**/*.csv')
        any_dep_manager.get_status(task, {})
        any_dep_manager.save_success(task)
        os.rename('landing/a.csv', 'landing/renamed.csv')
        log = any_dep_manager.get_status(task, {}, get_log=True)
        assert log.status == 'run'
        assert log.reasons['glob_dep_added']
        assert log.reasons['glob_dep_removed']

    def test_pattern_changed(self, any_dep_manager, workdir):
        write('landing/a.csv', 'a')
        write('other/b.csv', 'b')
        task = self._task('landing/**/*.csv')
        any_dep_manager.get_status(task, {})
        any_dep_manager.save_success(task)
        new_task = self._task('other/**/*.csv')
        log = any_dep_manager.get_status(new_task, {}, get_log=True)
        assert log.status == 'run'
        assert log.reasons['glob_dep_patterns_changed']

    def test_content_change_same_size_same_mtime(self, any_dep_manager, workdir):
        write('landing/a.csv', 'aaaa')
        task = self._task('landing/**/*.csv')
        any_dep_manager.get_status(task, {})
        any_dep_manager.save_success(task)
        stat = os.stat('landing/a.csv')
        write('landing/a.csv', 'bbbb', mtime=stat.st_mtime)
        log = any_dep_manager.get_status(task, {}, get_log=True)
        assert log.status == 'run'
        assert log.reasons['glob_dep_changed'] == [normalize_path('landing/a.csv')]

    def test_mtime_only_change_up_to_date(self, any_dep_manager, workdir):
        write('landing/a.csv', 'aaaa')
        task = self._task('landing/**/*.csv')
        any_dep_manager.get_status(task, {})
        any_dep_manager.save_success(task)
        stat = os.stat('landing/a.csv')
        os.utime('landing/a.csv', (stat.st_atime, stat.st_mtime + 1000))
        assert any_dep_manager.get_status(task, {}).status == 'up-to-date'

    def test_enumeration_order_does_not_matter(self, any_dep_manager, workdir):
        write('landing/a.csv', 'a')
        write('landing/b.csv', 'b')
        task = self._task('landing/**/*.csv')
        any_dep_manager.get_status(task, {})
        any_dep_manager.save_success(task)
        # rewriting in a different creation order keeps identical state
        # (the manifest and sorted file list are order-independent)
        assert any_dep_manager.get_status(task, {}).status == 'up-to-date'

    def test_empty_pattern_is_persisted_and_detects_new(self, any_dep_manager,
                                                        workdir):
        os.makedirs('landing')
        task = self._task('landing/**/*.csv')
        assert any_dep_manager.get_status(task, {}).status == 'run'
        any_dep_manager.save_success(task)
        assert any_dep_manager.get_status(task, {}).status == 'up-to-date'
        write('landing/new.csv', 'n')
        log = any_dep_manager.get_status(task, {}, get_log=True)
        assert log.status == 'run'
        assert log.reasons['glob_dep_added']

    def test_file_dep_still_checked(self, any_dep_manager, workdir):
        write('landing/a.csv', 'a')
        write('extra.txt', 'x')
        task = self._task('landing/**/*.csv', file_dep=['extra.txt'])
        any_dep_manager.get_status(task, {})
        any_dep_manager.save_success(task)
        write('extra.txt', 'changed')
        assert any_dep_manager.get_status(task, {}).status == 'run'

    def test_missing_explicit_file_dep_still_errors(self, any_dep_manager,
                                                    workdir):
        write('landing/a.csv', 'a')
        task = self._task('landing/**/*.csv', file_dep=['missing.txt'])
        status = any_dep_manager.get_status(task, {})
        assert status.status == 'error'
        assert 'missing.txt' in status.get_error_message()

    def test_old_database_without_glob_info_triggers_once(self, any_dep_manager,
                                                          workdir):
        write('landing/a.csv', 'a')
        task = self._task('landing/**/*.csv')
        # simulate an old record: deps saved but no glob_dep key
        any_dep_manager._set(task.name, 'deps:', tuple())
        assert any_dep_manager.get_status(task, {}).status == 'run'
        any_dep_manager.save_success(task)
        assert any_dep_manager.get_status(task, {}).status == 'up-to-date'

    def test_save_only_attests_given_state(self, any_dep_manager, workdir):
        write('landing/a.csv', 'a')
        task = self._task('landing/**/*.csv')
        start = observe_glob_state(task.glob_dep)
        write('landing/a.csv', 'bbbb')
        # save the start observation explicitly, not a fresh scan
        any_dep_manager.save_success(task, glob_state=start)
        assert any_dep_manager.get_status(task, {}).status == 'run'

    def test_scan_error_is_not_up_to_date(self, any_dep_manager, workdir,
                                          monkeypatch):
        write('landing/a.csv', 'a')
        task = self._task('landing/**/*.csv')
        any_dep_manager.get_status(task, {})
        any_dep_manager.save_success(task)
        import doit.dependency as dependency
        def boom(patterns):
            raise GlobError('cannot read', 'landing')
        monkeypatch.setattr(dependency, 'scan_state', boom)
        status = any_dep_manager.get_status(task, {})
        assert status.status == 'error'
        assert 'cannot read' in status.get_error_message()

    def test_task_without_glob_unchanged_behaviour(self, any_dep_manager,
                                                   workdir):
        write('extra.txt', 'x')
        task = Task('plain', [], file_dep=['extra.txt'])
        assert any_dep_manager.get_status(task, {}).status == 'run'
        any_dep_manager.save_success(task)
        assert any_dep_manager.get_status(task, {}).status == 'up-to-date'

    def test_glob_record_cleared_when_declaration_removed(self, any_dep_manager,
                                                          workdir):
        write('landing/a.csv', 'a')
        task = self._task('landing/**/*.csv')
        any_dep_manager.get_status(task, {})
        any_dep_manager.save_success(task)
        assert any_dep_manager._get(task.name, GLOB_DEP_KEY) is not None
        plain = Task('globtask', [], file_dep=[])
        any_dep_manager.save_success(plain)
        assert any_dep_manager._get('globtask', GLOB_DEP_KEY) is None

    def test_verify_glob_unchanged_start_state(self, any_dep_manager, workdir):
        write('landing/a.csv', 'aaaa')
        task = self._task('landing/**/*.csv')
        start = any_dep_manager.observe_glob(task)
        # no change
        any_dep_manager.verify_glob_unchanged(task, start)
        # add a file mid-action
        write('landing/b.csv', 'b')
        with pytest.raises(GlobDependencyChanged):
            any_dep_manager.verify_glob_unchanged(task, start)

    def test_verify_detects_empty_to_nonempty(self, any_dep_manager, workdir):
        os.makedirs('landing')
        task = self._task('landing/**/*.csv')
        start = any_dep_manager.observe_glob(task)
        write('landing/new.csv', 'n')
        with pytest.raises(GlobDependencyChanged):
            any_dep_manager.verify_glob_unchanged(task, start)


# ---------------------------------------------------------------------------
# graph construction (potential producers)


class TestGraph:

    def test_glob_creates_implicit_task_dep(self):
        consumer = Task('consumer', [], glob_dep=['landing/**/*.csv'])
        producer = Task('producer', [], targets=['landing/a.csv'],
                        uptodate=[False])
        control = TaskControl([consumer, producer])
        assert 'producer' in control.tasks['consumer'].task_dep

    def test_implicit_dep_even_target_not_created(self):
        consumer = Task('consumer', [], glob_dep=['landing/**/*.csv'])
        producer = Task('producer', [], targets=['landing/deep/new.csv'])
        control = TaskControl([consumer, producer])
        assert 'producer' in control.tasks['consumer'].task_dep

    def test_path_aliases_match_producer(self):
        consumer = Task('consumer', [], glob_dep=['./landing/../landing/*.csv'])
        producer = Task('producer', [], targets=['landing/a.csv'])
        control = TaskControl([consumer, producer])
        assert 'producer' in control.tasks['consumer'].task_dep

    def test_absolute_target_matches_relative_pattern(self, tmp_path,
                                                      monkeypatch):
        monkeypatch.chdir(tmp_path)
        consumer = Task('consumer', [], glob_dep=['landing/*.csv'])
        producer = Task('producer', [], targets=[str(tmp_path / 'landing' / 'a.csv')])
        control = TaskControl([consumer, producer])
        assert 'producer' in control.tasks['consumer'].task_dep

    def test_no_self_dependency(self):
        task = Task('self', [], glob_dep=['landing/*.csv'],
                    targets=['landing/a.csv'])
        control = TaskControl([task])
        assert 'self' not in control.tasks['self'].task_dep

    def test_shared_producer_runs_once(self):
        p1 = Task('producer', [], targets=['landing/a.csv', 'landing/b.csv'],
                  uptodate=[False])
        c1 = Task('c1', [], glob_dep=['landing/a.csv'])
        c2 = Task('c2', [], glob_dep=['landing/b.csv'])
        control = TaskControl([c1, c2, p1])
        assert control.tasks['c1'].task_dep == ['producer']
        assert control.tasks['c2'].task_dep == ['producer']
        # walking the dependency graph creates a single node for the producer
        dispatcher = TaskDispatcher(control.tasks, control.targets,
                                    ['c1', 'c2'])
        n1 = dispatcher._gen_node(None, 'c1')
        n2 = dispatcher._gen_node(None, 'c2')
        producer_node = dispatcher._gen_node(n1, 'producer')
        assert dispatcher._gen_node(n2, 'producer') is None
        assert dispatcher.nodes['producer'] is producer_node

    def test_cycle_detected(self):
        from doit.exceptions import InvalidDodoFile
        a = Task('a', [], glob_dep=['out/b/*.csv'], targets=['out/a/x.csv'])
        b = Task('b', [], glob_dep=['out/a/*.csv'], targets=['out/b/x.csv'])
        control = TaskControl([a, b])
        dispatcher = TaskDispatcher(control.tasks, control.targets, ['a'])
        with pytest.raises(InvalidDodoFile):
            list(dispatcher.generator)

    def test_file_dep_and_glob_dep_coexist(self):
        consumer = Task('consumer', [], file_dep=['fixed.txt'],
                        glob_dep=['landing/*.csv'])
        producer = Task('producer', [], targets=['landing/a.csv'])
        control = TaskControl([consumer, producer])
        assert 'producer' in control.tasks['consumer'].task_dep
        assert control.tasks['consumer'].file_dep == {'fixed.txt'}


# ---------------------------------------------------------------------------
# actions see the actual input list


class TestActualDependencies:

    def test_union_file_dep_and_glob(self, workdir):
        write('fixed.txt', 'f')
        write('landing/a.csv', 'a')
        task = Task('t', [], file_dep=['fixed.txt'],
                    glob_dep=['landing/*.csv'])
        task.set_glob_observation(scan_state(task.glob_dep))
        deps = task.actual_dependencies
        # explicit file_dep keeps its declared spelling, glob files are
        # normalized; sorted by normalized key.
        assert deps == ['fixed.txt', normalize_path('landing/a.csv')]

    def test_sorted_by_normalized_path(self, workdir):
        for name in ['c.csv', 'a.csv', 'b.csv']:
            write('landing/' + name, name)
        task = Task('t', [], glob_dep=['landing/*.csv'])
        task.set_glob_observation(scan_state(task.glob_dep))
        assert task.actual_dependencies == sorted(task.actual_dependencies)

    def test_does_not_mutate_file_dep(self, workdir):
        write('landing/a.csv', 'a')
        task = Task('t', [], file_dep=['fixed.txt'],
                    glob_dep=['landing/*.csv'])
        before = set(task.file_dep)
        task.set_glob_observation(scan_state(task.glob_dep))
        _ = task.actual_dependencies
        assert task.file_dep == before

    def test_candidates_not_included_before_production(self, workdir):
        # only actual files, not potential targets
        task = Task('t', [], glob_dep=['landing/*.csv'])
        task.set_glob_observation(scan_state(task.glob_dep))
        assert task.actual_dependencies == []

    def test_python_action_receives_actual_dependencies(self, workdir):
        write('landing/a.csv', 'a')
        got = []

        def action(dependencies):
            got.append(list(dependencies))
        task = Task('t', [action], glob_dep=['landing/*.csv'])
        task.set_glob_observation(scan_state(task.glob_dep))
        task.options = {}
        task.actions[0].execute()
        assert got == [[normalize_path('landing/a.csv')]]

    def test_cmd_action_percent_dependencies(self, workdir):
        write('landing/a.csv', 'a')

        def echo(*args):
            pass
        # just check expansion, not running a shell
        task = Task('t', ["cat %(dependencies)s"], glob_dep=['landing/*.csv'])
        task.set_glob_observation(scan_state(task.glob_dep))
        task.options = {}
        assert task.actions[0].expand_action() == \
            'cat ' + normalize_path('landing/a.csv')


# ---------------------------------------------------------------------------
# info command reasons


class TestInfoReasons:

    def test_get_reasons_mentions_glob(self):
        reasons = {
            'glob_dep_added': ['/x/a.csv'],
            'glob_dep_removed': ['/x/b.csv'],
            'glob_dep_changed': ['/x/c.csv'],
            'glob_dep_patterns_changed': {'previous': ['a/*.csv'],
                                          'current': ['b/*.csv']},
        }
        text = Info.get_reasons(reasons)
        assert 'added' in text
        assert '/x/a.csv' in text
        assert 'removed' in text
        assert 'changed' in text
        assert 'patterns changed' in text

    def test_info_reports_glob_files(self, dep_manager, workdir):
        write('landing/a.csv', 'a')
        task = Task('t', [], glob_dep=['landing/*.csv'])
        output = StringIO()
        cmd = CmdFactory(Info, outstream=output,
                         dep_file=dep_manager.name, task_list=[task],
                         dep_manager=dep_manager)
        cmd._execute(['t'])
        assert 'glob_dep' in output.getvalue()
        assert 'glob_files' in output.getvalue()


class TestListDependencies:

    def test_list_shows_glob_files(self, dep_manager, workdir):
        write('landing/a.csv', 'a')
        task = Task('t', [], glob_dep=['landing/*.csv'])
        output = StringIO()
        cmd = CmdFactory(List, outstream=output, task_list=[task],
                         dep_manager=dep_manager)
        cmd._execute(list_deps=True)
        assert 'landing/a.csv' in output.getvalue() or \
            normalize_path('landing/a.csv') in output.getvalue()


class TestResetDep:

    def test_reset_dep_records_glob_state(self, dep_manager, workdir):
        from doit.cmd_resetdep import ResetDep
        write('landing/a.csv', 'a')
        task = Task('t', [], glob_dep=['landing/*.csv'])
        output = StringIO()
        cmd = CmdFactory(ResetDep, outstream=output, task_list=[task],
                         dep_manager=dep_manager)
        cmd._execute()
        assert 'processed t' in output.getvalue()
        # after reset-dep the task is up-to-date and the record persists
        fresh = Task('t', [], glob_dep=['landing/*.csv'])
        assert dep_manager.get_status(fresh, {}).status == 'up-to-date'

    def test_reset_dep_reports_scan_error(self, dep_manager, workdir):
        from doit.cmd_resetdep import ResetDep
        if os.geteuid() == 0:  # pragma: no cover
            pytest.skip('root can read anything')
        write('locked/a.csv', 'a')
        os.chmod('locked', 0)
        try:
            task = Task('t', [], glob_dep=['locked/*.csv'])
            output = StringIO()
            cmd = CmdFactory(ResetDep, outstream=output, task_list=[task],
                             dep_manager=dep_manager)
            cmd._execute()
            assert 'failed t' in output.getvalue()
        finally:
            os.chmod('locked', 0o755)
