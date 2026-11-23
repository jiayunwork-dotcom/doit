"""Unit tests for resource declaration validation and admission control."""

import pytest

from doit.exceptions import InvalidTask, InvalidDodoFile
from doit.resources import (
    ResourceManager, DEFAULT_BYPASS_LIMIT,
    validate_resources, validate_resource_limits, validate_bypass_limit,
)
from doit.task import Task


class FakeNode(object):
    """stand-in for control.ExecNode in admission tests"""
    def __init__(self, name, resources=None):
        self.task = Task(name, [], resources=resources)


# --------------------------------------------------------------------------
# validation

class TestValidateResources:
    def test_defaults_to_empty(self):
        assert validate_resources(None) == {}
        assert validate_resources({}) == {}

    def test_ok_values(self):
        assert validate_resources({'mem': 3, 'conn': 0}) == {'mem': 3,
                                                              'conn': 0}

    @pytest.mark.parametrize('bad', [
        {'': 1},
        {'mem': -1},
        {'mem': True},
        {'mem': False},
        {'mem': 1.5},
        {'mem': '2'},
    ])
    def test_bad_values(self, bad):
        with pytest.raises(InvalidTask):
            validate_resources(bad, 'tX')

    def test_bad_type(self):
        with pytest.raises(InvalidTask):
            validate_resources([('mem', 1)], 'tX')

    def test_non_string_key(self):
        with pytest.raises(InvalidTask):
            validate_resources({1: 2}, 'tX')

    def test_caller_dict_not_mutated(self):
        original = {'mem': 2}
        result = validate_resources(original, 'tX')
        result['mem'] = 99
        assert original == {'mem': 2}


class TestValidateLimits:
    def test_empty_legal(self):
        assert validate_resource_limits(None) == {}
        assert validate_resource_limits({}) == {}

    @pytest.mark.parametrize('bad', [
        {'mem': 0},
        {'mem': -1},
        {'mem': True},
        {'mem': 1.0},
        {'mem': '4'},
    ])
    def test_bad_capacity(self, bad):
        with pytest.raises(InvalidDodoFile):
            validate_resource_limits(bad)

    def test_bad_key_and_type(self):
        with pytest.raises(InvalidDodoFile):
            validate_resource_limits({'': 1})
        with pytest.raises(InvalidDodoFile):
            validate_resource_limits("nope")

    def test_bypass_limit(self):
        assert validate_bypass_limit(0) == 0
        assert validate_bypass_limit(3) == 3
        for bad in (-1, True, False, 1.0, '3'):
            with pytest.raises(InvalidDodoFile):
                validate_bypass_limit(bad)

    def test_default_bypass(self):
        assert DEFAULT_BYPASS_LIMIT == 3

    def test_caller_dict_not_mutated(self):
        original = {'mem': 2}
        result = validate_resource_limits(original)
        result['mem'] = 99
        assert original == {'mem': 2}


# --------------------------------------------------------------------------
# allocation basics

class TestAdmissionBasics:
    def test_commit_and_release(self):
        rm = ResourceManager({'mem': 4})
        n1 = FakeNode('a', {'mem': 3})
        assert rm.try_admit(n1)
        assert rm.allocated['mem'] == 3
        n2 = FakeNode('b', {'mem': 2})
        assert not rm.try_admit(n2)  # parks
        assert rm.allocated['mem'] == 3
        assert rm.has_waiters()
        rm.release(n1)
        assert rm.allocated['mem'] == 0
        node = rm.pump()
        assert node.task.name == 'b'
        assert rm.allocated['mem'] == 2

    def test_release_idempotent(self):
        rm = ResourceManager({'mem': 4})
        n1 = FakeNode('a', {'mem': 1})
        rm.try_admit(n1)
        rm.release(n1)
        rm.release(n1)  # no error
        assert rm.allocated['mem'] == 0

    def test_zero_needs_never_wait(self):
        rm = ResourceManager({'mem': 1})
        heavy = FakeNode('h', {'mem': 1})
        assert rm.try_admit(heavy)
        zero = FakeNode('z', {'mem': 0})
        assert rm.try_admit(zero)
        assert not rm.has_waiters()
        rm.release(zero)
        assert rm.allocated['mem'] == 1

    def test_no_limits_but_positive_need_raises(self):
        rm = ResourceManager({})
        node = FakeNode('a', {'mem': 1})
        with pytest.raises(InvalidTask, match='no capacity'):
            rm.try_admit(node)

    def test_need_over_capacity_raises(self):
        rm = ResourceManager({'mem': 2})
        node = FakeNode('a', {'mem': 3})
        with pytest.raises(InvalidTask, match='capacity is 2'):
            rm.try_admit(node)

    def test_unknown_resource_raises(self):
        rm = ResourceManager({'mem': 2})
        node = FakeNode('a', {'mem': 1, 'conn': 1})
        with pytest.raises(InvalidTask, match='conn'):
            rm.try_admit(node)

    def test_multi_resource_all_or_nothing(self):
        # mem fits, conn does not: nothing may be committed
        rm = ResourceManager({'mem': 4, 'conn': 1})
        holder = FakeNode('h', {'conn': 1})
        rm.try_admit(holder)
        node = FakeNode('a', {'mem': 1, 'conn': 1})
        assert not rm.try_admit(node)
        assert rm.allocated['mem'] == 0
        assert rm.allocated['conn'] == 1

    def test_multi_resource_admitted_together(self):
        rm = ResourceManager({'mem': 4, 'conn': 2})
        node = FakeNode('a', {'mem': 3, 'conn': 2})
        assert rm.try_admit(node)
        assert rm.allocated == {'mem': 3, 'conn': 2}


# --------------------------------------------------------------------------
# fairness boundary

class TestFairness:
    def test_small_tasks_jump_while_budget_remains(self):
        rm = ResourceManager({'mem': 4}, bypass_limit=3)
        # A occupies everything, B (needs 3) parks first
        a = FakeNode('A', {'mem': 4})
        assert rm.try_admit(a)
        assert not rm.try_admit(FakeNode('B0', {'mem': 3}))

        # release A; three later smalls fitting 1 unit each bypass B while
        # B keeps waiting because 3 units never accumulate at once.
        rm.release(a)
        for i in range(3):
            small = FakeNode('s{}'.format(i), {'mem': 1})
            assert rm.try_admit(small), i
        b_waiter = next(w for w in rm.waiters if w.name == 'B0')
        assert b_waiter.bypass_total == 3

        # fourth overlapping later task is blocked by fairness even though
        # one unit is still free
        blocked = FakeNode('s3', {'mem': 1})
        assert not rm.try_admit(blocked)
        assert rm.waiters[-1].reason.startswith('fairness')

        # once enough capacity frees, B gets its real chance (FIFO among
        # the remaining waiters)
        rm.release([n for n in rm.commitments
                    if n.task.name == 's0'][0])
        rm.release([n for n in rm.commitments
                    if n.task.name == 's1'][0])
        rm.release([n for n in rm.commitments
                    if n.task.name == 's2'][0])
        node = rm.pump()
        assert node.task.name == 'B0'

    def test_budget_spent_gives_big_waiter_a_real_chance(self):
        rm = ResourceManager({'mem': 4}, bypass_limit=1)
        a = FakeNode('A', {'mem': 3})
        rm.try_admit(a)
        big = FakeNode('B', {'mem': 3})
        assert not rm.try_admit(big)
        # one bypass allowed
        small1 = FakeNode('s1', {'mem': 1})
        assert rm.try_admit(small1)
        # next small cannot jump; releasing A must admit B, not a new small
        small2 = FakeNode('s2', {'mem': 1})
        assert not rm.try_admit(small2)
        rm.release(a)
        node = rm.pump()
        assert node.task.name == 'B'

    def test_limit_zero_forbids_any_bypass(self):
        rm = ResourceManager({'mem': 4}, bypass_limit=0)
        a = FakeNode('A', {'mem': 3})
        rm.try_admit(a)
        big = FakeNode('B', {'mem': 3})
        assert not rm.try_admit(big)
        small = FakeNode('s', {'mem': 1})
        assert not rm.try_admit(small)
        assert rm.waiters[-1].reason.startswith('fairness')
        # disjoint resource still flows
        other = FakeNode('o', {'gpu': 1})
        rm2 = ResourceManager({'mem': 4, 'gpu': 1}, bypass_limit=0)
        rm2.try_admit(FakeNode('A2', {'mem': 3}))
        assert not rm2.try_admit(FakeNode('B2', {'mem': 3}))
        assert rm2.try_admit(other)

    def test_disjoint_tasks_never_count_or_block(self):
        rm = ResourceManager({'mem': 4, 'conn': 4}, bypass_limit=0)
        rm.try_admit(FakeNode('A', {'mem': 3}))
        waiter_node = FakeNode('W', {'mem': 3})
        assert not rm.try_admit(waiter_node)
        # conn-only tasks share nothing with W and fit the conn capacity
        for i in range(4):
            assert rm.try_admit(FakeNode('c{}'.format(i), {'conn': 1}))
        assert rm.waiters[0].bypass_total == 0
        assert rm.waiters[0].name == 'W'

    def test_partial_overlap_counts(self):
        rm = ResourceManager({'mem': 4, 'conn': 4}, bypass_limit=3)
        rm.try_admit(FakeNode('A', {'mem': 3, 'conn': 3}))
        assert not rm.try_admit(FakeNode('W', {'mem': 3, 'conn': 3}))
        assert rm.try_admit(FakeNode('x', {'mem': 1}))  # overlaps mem
        assert rm.waiters[0].bypass_total == 1

    def test_one_admission_bypasses_each_overlapping_waiter_once(self):
        # a single later admission that jumps several earlier waiters must
        # charge each of them exactly once (no double counting per resource)
        rm = ResourceManager({'mem': 4}, bypass_limit=10,
                             record_events=True)
        rm.try_admit(FakeNode('hold', {'mem': 3}))
        assert not rm.try_admit(FakeNode('W1', {'mem': 3}))
        assert not rm.try_admit(FakeNode('W2', {'mem': 3}))
        rm.release([n for n in rm.commitments][0])
        # the 1-unit later task fits now and bypasses both earlier waiters
        small = FakeNode('later', {'mem': 1})
        assert rm.try_admit(small)
        assert all(w.bypass_total == 1 for w in rm.waiters)
        # one admit event, naming both bypassed waiters
        admit = rm.events[-1]
        assert admit['kind'] == 'admit'
        assert sorted(admit['bypassed']) == ['W1', 'W2']

    def test_liveness_waiter_eventually_runs(self):
        # no matter how many disjoint tasks flow, an overlapping waiter runs
        # once enough capacity frees, proving fairness cannot deadlock.
        rm = ResourceManager({'mem': 2, 'gpu': 10}, bypass_limit=1)
        holder = FakeNode('h', {'mem': 2})
        rm.try_admit(holder)
        waiter = FakeNode('W', {'mem': 2})
        assert not rm.try_admit(waiter)
        # disjoint gpu tasks (within gpu capacity) never touch W's budget
        for i in range(10):
            assert rm.try_admit(FakeNode('g{}'.format(i), {'gpu': 1}))
        assert rm.waiters[0].bypass_total == 0
        rm.release(holder)
        node = rm.pump()
        assert node.task.name == 'W'
        assert rm.allocated['mem'] == 2

    def test_no_circular_wait_possible(self):
        # waiters never hold any capacity, so a resource cycle (A waits on
        # resources held by B while B waits on A's) cannot form.
        rm = ResourceManager({'mem': 4, 'conn': 4})
        rm.try_admit(FakeNode('running', {'mem': 4}))
        assert not rm.try_admit(FakeNode('W', {'mem': 2, 'conn': 2}))
        # parked waiter holds nothing
        assert rm.allocated == {'mem': 4, 'conn': 0}
        assert rm.commitments  # only the running task
        for node in rm.commitments:
            assert node.task.name == 'running'

    def test_disjoint_admission_bypasses_no_one(self):
        rm = ResourceManager({'mem': 4, 'gpu': 1}, bypass_limit=5)
        assert rm.try_admit(FakeNode('g', {'gpu': 1}))
        assert not rm.try_admit(FakeNode('W1', {'gpu': 1}))
        assert not rm.try_admit(FakeNode('W2', {'gpu': 1}))
        # mem-only task is disjoint from the gpu waiters
        assert rm.try_admit(FakeNode('m', {'mem': 1}))
        assert all(w.bypass_total == 0 for w in rm.waiters)


class TestPump:
    def test_pump_only_touches_dirty_waiters(self):
        rm = ResourceManager({'mem': 4, 'conn': 4})
        mem_holder = FakeNode('mh', {'mem': 1})
        conn_holder = FakeNode('ch', {'conn': 1})
        rm.try_admit(mem_holder)
        assert not rm.try_admit(FakeNode('Wm', {'mem': 4}))
        rm.try_admit(conn_holder)
        assert not rm.try_admit(FakeNode('Wc', {'conn': 4}))
        # a conn release wakes only the conn waiter; the mem waiter stays
        rm.release(conn_holder)
        node = rm.pump()
        assert node.task.name == 'Wc'
        assert [w.name for w in rm.waiters] == ['Wm']

    def test_pump_preserves_arrival_order(self):
        rm = ResourceManager({'mem': 2})
        holder_node = FakeNode('h', {'mem': 2})
        assert rm.try_admit(holder_node)
        assert not rm.try_admit(FakeNode('w1', {'mem': 2}))
        assert not rm.try_admit(FakeNode('w2', {'mem': 2}))
        rm.release(holder_node)
        first = rm.pump()
        assert first.task.name == 'w1'
        rm.release(first)
        second = rm.pump()
        assert second.task.name == 'w2'

    def test_pump_returns_none_when_nothing_fits(self):
        rm = ResourceManager({'mem': 2})
        assert rm.try_admit(FakeNode('h', {'mem': 2}))
        assert not rm.try_admit(FakeNode('w', {'mem': 2}))
        # nothing was released, so pumping cannot admit the waiter
        assert rm.pump() is None


class TestDiagnostics:
    def test_events_and_snapshot(self):
        rm = ResourceManager({'mem': 2}, record_events=True)
        node = FakeNode('a', {'mem': 2})
        rm.try_admit(node)
        rm.release(node)
        kinds = [e['kind'] for e in rm.events]
        assert kinds == ['admit', 'release']
        snap = rm.snapshot()
        assert snap['limits'] == {'mem': 2}
        assert snap['allocated'] == {'mem': 0}
        assert snap['running'] == []

    def test_events_disabled_by_default(self):
        rm = ResourceManager({'mem': 2})
        rm.try_admit(FakeNode('a', {'mem': 1}))
        assert rm.events is None

    def test_fresh_state_per_instance(self):
        rm1 = ResourceManager({'mem': 1})
        rm1.try_admit(FakeNode('a', {'mem': 1}))
        rm2 = ResourceManager({'mem': 1})
        assert rm2.allocated == {'mem': 0}
        assert rm2.waiters == []
        assert rm2.admit_count == 0
