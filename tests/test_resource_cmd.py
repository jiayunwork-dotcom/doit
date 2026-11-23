"""Command/configuration-level tests for resource admission.

These exercise the full ``DoitMain`` -> loader -> ``Run`` command path,
including validation of the DOIT_CONFIG values, ``doit info`` output,
glob_dep integration and re-running inside one Python process.
"""

import os
from io import StringIO

import pytest

from doit.doit_cmd import DoitMain
from doit.cmd_base import ModuleTaskLoader
from doit.cmd_info import Info
from doit.cmd_run import Run
from doit.task import Task
from doit.exceptions import InvalidDodoFile, InvalidTask
from tests.conftest import CmdFactory


def write(path, content):
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    with open(path, 'w') as handle:
        handle.write(content)


def run_main(namespace, args, config_filenames=(), extra_config=None):
    main = DoitMain(ModuleTaskLoader(namespace),
                    config_filenames=config_filenames,
                    extra_config=extra_config)
    return main.run(args)


def simple_namespace(tmp_path, resources=None, extra_config=None):
    """a namespace with one action plus DOIT_CONFIG."""
    marker = tmp_path / 'marker.txt'

    def task_hello():
        return {
            'actions': [lambda: write(str(marker), 'done')],
            'targets': [str(marker)],
            **({'resources': resources} if resources else {}),
        }

    namespace = {'task_hello': task_hello}
    if extra_config is not None:
        namespace['DOIT_CONFIG'] = extra_config
    return namespace, str(marker)


class TestConfigurationValidation:

    def test_missing_limit_rejected_via_config(
            self, tmp_path, depfile_name):
        namespace, marker = simple_namespace(
            tmp_path, resources={'mem': 1},
            extra_config={})  # no resource_limits at all
        # caught by the runner preflight and reported as a run error,
        # without any action executing
        rc = run_main(namespace, ['run', '--db-file', depfile_name])
        assert rc == 2
        assert not os.path.exists(marker)

    def test_bad_limit_value_via_config(self, tmp_path, depfile_name):
        namespace, _ = simple_namespace(
            tmp_path, resources={'mem': 1},
            extra_config={'resource_limits': {'mem': 0}})
        rc = run_main(namespace, ['run', '--db-file', depfile_name])
        assert rc == 3

    def test_bad_bypass_limit_via_config(self, tmp_path, depfile_name):
        namespace, marker = simple_namespace(
            tmp_path, extra_config={'resource_bypass_limit': -1})
        rc = run_main(namespace, ['run', '--db-file', depfile_name])
        assert rc == 3
        assert not os.path.exists(marker)  # no action ran

    def test_bad_bypass_bool_rejected(self, tmp_path, depfile_name):
        namespace, _ = simple_namespace(
            tmp_path, extra_config={'resource_bypass_limit': True})
        rc = run_main(namespace, ['run', '--db-file', depfile_name])
        assert rc == 3

    def test_valid_config_runs(self, tmp_path, depfile_name):
        namespace, marker = simple_namespace(
            tmp_path, resources={'mem': 2},
            extra_config={'resource_limits': {'mem': 4},
                          'resource_bypass_limit': 1})
        rc = run_main(namespace, ['run', '--db-file', depfile_name])
        assert rc == 0
        assert os.path.exists(marker)

    def test_empty_limits_dict_legal(self, tmp_path, depfile_name):
        namespace, marker = simple_namespace(
            tmp_path, extra_config={'resource_limits': {}})
        rc = run_main(namespace, ['run', '--db-file', depfile_name])
        assert rc == 0
        assert os.path.exists(marker)

    def test_callers_dict_not_rewritten(self, tmp_path, depfile_name):
        limits = {'mem': 4}
        bypass = 2
        namespace, _ = simple_namespace(
            tmp_path, resources={'mem': 1},
            extra_config={'resource_limits': limits,
                          'resource_bypass_limit': bypass})
        run_main(namespace, ['run', '--db-file', depfile_name])
        assert limits == {'mem': 4}
        assert bypass == 2


class TestTaskDeclarationValidation:

    def test_bool_requirement_rejected_at_construction(self):
        with pytest.raises(InvalidTask):
            Task('t', [], resources={'mem': True})

    def test_negative_requirement_rejected(self):
        with pytest.raises(InvalidTask):
            Task('t', [], resources={'mem': -1})

    def test_empty_resource_name_rejected(self):
        with pytest.raises(InvalidTask):
            Task('t', [], resources={'': 1})

    def test_default_is_empty_dict(self):
        task = Task('t', [])
        assert task.resources == {}

    def test_zero_demand_kept_but_occupies_nothing(self):
        task = Task('t', [], resources={'mem': 0})
        assert task.resources == {'mem': 0}

    def test_input_dict_not_retained(self):
        declaration = {'mem': 2}
        task = Task('t', [], resources=declaration)
        task.resources['mem'] = 99
        assert declaration == {'mem': 2}


class TestInfoCommand:

    def test_info_shows_resources(self, tmp_path, dep_manager):
        task = Task('etl', [lambda: None], resources={'mem': 4, 'conn': 2})
        output = StringIO()
        cmd = CmdFactory(Info, outstream=output,
                         dep_file=dep_manager.name, task_list=[task])
        cmd._execute(['etl'], hide_status=True)
        text = output.getvalue()
        assert 'resources' in text
        assert "'mem': 4" in text
        assert "'conn': 2" in text

    def test_info_omits_when_empty(self, tmp_path, dep_manager):
        task = Task('plain', [lambda: None])
        output = StringIO()
        cmd = CmdFactory(Info, outstream=output,
                         dep_file=dep_manager.name, task_list=[task])
        cmd._execute(['plain'], hide_status=True)
        assert 'resources' not in output.getvalue()


class TestGlobDepWithResources:

    def test_aggregate_waits_for_real_products(
            self, tmp_path, depfile_name):
        landing = tmp_path / 'landing'
        landing.mkdir()

        def generate():
            for i in range(2):
                write(str(landing / 'p{}.csv'.format(i)), str(i))

        seen = []

        def merge(dependencies):
            seen.append(sorted(dependencies))

        producer = Task('generate', [generate],
                        targets=[str(landing / 'p0.csv'),
                                 str(landing / 'p1.csv')],
                        resources={'mem': 4})
        consumer = Task('merge', [merge],
                        glob_dep=[str(landing / '*.csv')],
                        resources={'conn': 1})
        output = StringIO()
        cmd = CmdFactory(Run, outstream=output, backend='dbm',
                         dep_file=depfile_name,
                         task_list=[producer, consumer])
        cmd._resource_limits = {'mem': 4, 'conn': 1}
        cmd._resource_bypass_limit = 3
        rc = cmd._execute(output, verbosity=0)
        assert rc == 0
        # merge observed the files the producer really created
        assert seen and len(seen[-1]) == 2


class TestReRunSameProcess:

    def test_state_resets_between_runs(self, tmp_path, depfile_name):
        namespace, marker = simple_namespace(
            tmp_path, resources={'mem': 1},
            extra_config={'resource_limits': {'mem': 1}})

        rc1 = run_main(namespace, ['run', '--db-file', depfile_name])
        assert rc1 == 0

        # second DoitMain in the same Python process: the marker already
        # exists so the task is up-to-date, but no stale allocation may leak
        rc2 = run_main(namespace, ['run', '--db-file', depfile_name])
        assert rc2 == 0

        # force re-execution with a fresh manager again
        os.remove(marker)
        rc3 = run_main(namespace, ['run', '-a', '--db-file',
                                   depfile_name])
        assert rc3 == 0


class TestFailureThenRerun:

    def test_failed_run_releases_then_rerun_succeeds(
            self, tmp_path, depfile_name):
        target = str(tmp_path / 'out.txt')
        state = {'fail': True}

        def flaky():
            if state['fail']:
                state['fail'] = False
                raise RuntimeError('first attempt fails')
            write(target, 'ok')

        task = Task('flaky', [flaky], targets=[target],
                    resources={'mem': 2})
        output = StringIO()
        cmd = CmdFactory(Run, outstream=output, backend='dbm',
                         dep_file=depfile_name, task_list=[task])
        cmd._resource_limits = {'mem': 2}
        rc1 = cmd._execute(output, verbosity=0)
        assert rc1 != 0
        # rebuild a fresh command for the rerun (fresh ResourceManager)
        cmd2 = CmdFactory(Run, outstream=output, backend='dbm',
                          dep_file=depfile_name, task_list=[task])
        cmd2._resource_limits = {'mem': 2}
        rc2 = cmd2._execute(output, verbosity=0)
        assert rc2 == 0
        assert os.path.exists(target)
