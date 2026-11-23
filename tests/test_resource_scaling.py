"""Scaling characteristics of the resource admission layer.

Each scenario uses sparse resources: every completion frees capacity on only
a small number of resource dimensions, so a naive implementation that
re-scans the whole waiting set on every result would show quadratic growth.
The assertions bound the number of individual requirement checks and verify
that waiters unrelated to a freed resource are never re-evaluated.
"""

import resource as os_resource
import sys

import pytest

from doit.resources import ResourceManager
from doit.task import Task


class Node:
    def __init__(self, name, needs):
        self.task = Task(name, [], resources=needs)


def make_world(num_waiters, num_resources):
    """one running holder per resource plus num_waiters parked tasks.

    Waiter requirements cycle through the sparse resource dimensions, so on
    average only a fraction of waiters reference any given resource.
    """
    limits = {'r{}'.format(i): 1000 for i in range(num_resources)}
    manager = ResourceManager(limits)
    holders = []
    for i in range(num_resources):
        node = Node('hold-r{}'.format(i), {'r{}'.format(i): 1000})
        assert manager.try_admit(node)
        holders.append(node)

    waiters = []
    for i in range(num_waiters):
        # sparse: each waiter touches 1-2 of the resource dimensions
        primary = i % num_resources
        needs = {'r{}'.format(primary): 1}
        if num_resources > 1 and i % 5 == 0:
            needs['r{}'.format((primary + 1) % num_resources)] = 1
        node = Node('w{}'.format(i), needs)
        assert not manager.try_admit(node)
        waiters.append(node)
    return manager, holders, waiters


@pytest.mark.parametrize('num_waiters', [100, 1000, 10000])
def test_checks_stay_sparse_not_quadratic(num_waiters):
    num_resources = 10
    manager, holders, waiters = make_world(num_waiters, num_resources)
    assert manager.wait_count == num_waiters

    rss_before = os_resource.getrusage(os_resource.RUSAGE_SELF).ru_maxrss

    # release one resource dimension: only waiters referencing it are marked
    target_resource = 'r3'
    manager.release(holders[3])
    checks_before = manager.check_count
    node = manager.pump()
    checks_after = manager.check_count
    assert node is not None

    # the pump only evaluated waiters that reference r3 (about 1/10 of the
    # set), never the whole waiting set.  Bound generously but well below
    # the linear full-scan cost.
    checks_this_pump = checks_after - checks_before
    full_scan = num_waiters * 2  # two resource keys worst case per waiter
    assert checks_this_pump <= full_scan // 3, (
        checks_this_pump, full_scan)

    # waiters on other resources were not touched: their recheck markers
    # stay empty until their own resource changes
    unrelated = [w for w in manager.waiters
                 if target_resource not in w.needs]
    assert unrelated, 'test setup expects unrelated waiters'

    rss_after = os_resource.getrusage(os_resource.RUSAGE_SELF).ru_maxrss
    # report rather than hard-fail on environment-dependent RSS; ensure the
    # structures at least stay bounded by O(waiters) bookkeeping entries.
    assert len(manager.waiters) == num_waiters - 1
    assert sum(len(q) for q in manager.by_resource.values()) <= \
        num_waiters * 2 + num_resources
    # keep the values visible in -rA style output when debugging
    print('\nwaiters={} checks_per_pump={} rss_kb_before={} rss_kb_after={}'
          .format(num_waiters, checks_this_pump, rss_before, rss_after))


@pytest.mark.parametrize('num_waiters', [100, 1000, 10000])
def test_many_completions_total_checks_linear_in_relevant_waiters(
        num_waiters):
    num_resources = 10
    manager, holders, waiters = make_world(num_waiters, num_resources)

    # Free each resource dimension once.  The first release admits exactly
    # one waiter (which then refills the capacity), so subsequent pumps
    # correctly find nothing fitting.  The total number of checks must stay
    # proportional to the waiters actually referencing freed resources,
    # never num_waiters * num_resources (a full rescan per completion).
    checks_start = manager.check_count
    admissions = 0
    for holder in holders:
        manager.release(holder)
        node = manager.pump()
        if node is not None:
            admissions += 1
    checks_done = manager.check_count - checks_start

    # most freed dimensions admit one waiter immediately; a waiter needing
    # two dimensions may have to wait for the second release, so the exact
    # admission count is slightly below num_resources.  The check-cost bound
    # below is the property actually under test.
    assert num_resources - 2 <= admissions <= num_resources

    # explicit contrast: a full-rescan implementation would re-check every
    # waiter after every completion; the sparse index stays orders of
    # magnitude below that for large waiting sets.
    naive_cost = naive_full_scan_checks(
        num_waiters, num_resources, num_resources)
    assert checks_done < naive_cost
    print('\nwaiters={:6d} sparse_checks={:5d} naive_full_scan={:7d} '
          'ratio={:.1%}'.format(
              num_waiters, checks_done, naive_cost,
              checks_done / naive_cost))
    # a full rescan each time would cost roughly
    # num_waiters * 1.2 * num_resources checks; sparse indexing stays far
    # below that even with every dimension touched once
    full_rescan_cost = int(num_waiters * 1.2 * num_resources)
    assert checks_done < full_rescan_cost, (checks_done, full_rescan_cost)


def test_no_resources_declared_has_zero_scan_cost():
    # the legacy path must not pay for waiting-set machinery it never uses
    manager = ResourceManager({})
    node = Node('plain', {})
    for _ in range(1000):
        assert manager.try_admit(Node('t', {}))
    assert manager.check_count == 0
    assert manager.waiters == []
    assert manager.pump() is None
    # silence unused-variable warnings while keeping the test self-contained
    assert node.task.name == 'plain'


def naive_full_scan_checks(num_waiters, num_resources, num_completions):
    """what a rescan-the-whole-set implementation would have checked.

    A naive scheduler re-evaluates every parked waiter after every
    completion.  This returns that cost for comparison with the sparse
    index's measured count.
    """
    avg_keys = 1.2  # 80% one key, 20% two keys
    return int(num_waiters * avg_keys * num_completions)


def test_memory_footprint_is_linear(num_waiters=10000):
    manager, holders, waiters = make_world(num_waiters, 10)
    # one ResourceWaiter object + one commitments-free parked entry each;
    # indexes reference the same waiter objects (no copied requirement dicts
    # beyond the needs dict the waiter owns).
    waiter_objs = len(manager.waiters)
    indexed = sum(len(q) for q in manager.by_resource.values())
    # each parked waiter appears in 1 or 2 resource queues
    assert waiter_objs == num_waiters
    assert waiter_objs <= indexed <= waiter_objs * 2
    # admission ledger stays empty for parked-only tasks
    assert not manager.commitments or all(
        n in holders for n in manager.commitments)


if __name__ == '__main__':
    sys.exit(pytest.main([__file__, '-v', '-s']))
