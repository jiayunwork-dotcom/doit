"""Resource based admission control for task scheduling.

A task may declare *resource* requirements (e.g. memory slots or database
connections) through its ``resources`` attribute and the run declares the
available capacity of each resource through the ``resource_limits``
configuration.

The :class:`ResourceManager` lives in the process that dispatches tasks
(the parent process).  Capacity is committed when a task is *admitted* into
the execution queue and released only when the parent has finished processing
the task's result.  Allocation is all-or-nothing: a task never holds part of
its requirements while waiting for the rest.

Fairness boundary
-----------------
Waiters are ordered by the moment they entered the resource wait (which can
only happen after all their task dependencies are ready).  If an earlier
waiter ``A`` and a later task ``B`` share at least one resource with a
positive requirement, admitting ``B`` before ``A`` counts as one bypass of
``A``.  Each waiter tolerates at most ``resource_bypass_limit`` such
bypasses; afterwards overlapping later tasks cannot jump ahead of it.  Tasks
whose requirements are disjoint from ``A`` neither consume its budget nor
are blocked by it.

Cost
----
Waiters are indexed per resource.  When capacity changes (a task completes)
only waiters referencing the changed resources are scheduled for recheck and
the recheck set is de-duplicated, so completions that touch few resources
never cause a scan of the whole waiting set.  The theoretical worst case is a
single saturated resource with many waiters (every completion is relevant to
every waiter); the sparse-resource workloads this targets stay linear.
"""

import threading

from .exceptions import InvalidTask, InvalidDodoFile


DEFAULT_BYPASS_LIMIT = 3


def _is_plain_int(value):
    """int but not bool (bool is a subclass of int and must be rejected)."""
    return isinstance(value, int) and not isinstance(value, bool)


def validate_resources(resources, task_name=None):
    """Validate and copy a task ``resources`` declaration.

    Keys must be non-empty strings; values must be non-negative plain ints
    (``bool`` is rejected).  A value of ``0`` is kept but occupies no
    capacity.  The caller's dictionary is never modified.

    :rtype: dict
    :raises InvalidTask: on any malformed declaration.
    """
    if resources is None:
        return {}
    if not isinstance(resources, dict):
        msg = ("Task '{}' attribute 'resources' must be a dict. "
               "Got {!r} ({}).").format(task_name, resources,
                                        type(resources).__name__)
        raise InvalidTask(msg)
    validated = {}
    for key, value in resources.items():
        if not isinstance(key, str) or not key:
            msg = ("Task '{}' resource names must be non-empty strings. "
                   "Got {!r}.").format(task_name, key)
            raise InvalidTask(msg)
        if not _is_plain_int(value) or value < 0:
            msg = ("Task '{}' resource '{}' requirement must be a non-negative "
                   "integer (bool is not accepted). Got {!r}.").format(
                       task_name, key, value)
            raise InvalidTask(msg)
        validated[key] = value
    return validated


def validate_resource_limits(resource_limits):
    """Validate and copy the ``resource_limits`` configuration.

    Keys must be non-empty strings; capacities must be positive plain ints.
    An empty dict / ``None`` is legal and means no resource has declared
    capacity.  The caller's dictionary is never modified.

    :rtype: dict
    :raises InvalidDodoFile: on malformed configuration.
    """
    if resource_limits is None:
        return {}
    if not isinstance(resource_limits, dict):
        msg = ("DOIT_CONFIG 'resource_limits' must be a dict. Got {!r} ({})."
               ).format(resource_limits, type(resource_limits).__name__)
        raise InvalidDodoFile(msg)
    validated = {}
    for key, value in resource_limits.items():
        if not isinstance(key, str) or not key:
            msg = ("resource_limits keys must be non-empty strings. "
                   "Got {!r}.").format(key)
            raise InvalidDodoFile(msg)
        if not _is_plain_int(value) or value <= 0:
            msg = ("resource_limits['{}'] must be a positive integer "
                   "(bool is not accepted). Got {!r}.").format(key, value)
            raise InvalidDodoFile(msg)
        validated[key] = value
    return validated


def validate_bypass_limit(bypass_limit):
    """Validate ``resource_bypass_limit`` (non-negative plain int)."""
    if not _is_plain_int(bypass_limit) or bypass_limit < 0:
        msg = ("DOIT_CONFIG 'resource_bypass_limit' must be a non-negative "
               "integer (bool is not accepted). Got {!r}.").format(
                   bypass_limit)
        raise InvalidDodoFile(msg)
    return bypass_limit


class ResourceWaiter(object):
    """A task whose dependencies are ready but is waiting for capacity.

    :ivar seq: monotonic insertion sequence, defines waiting order.
    :ivar name: task name.
    :ivar needs: dict resource -> positive requirement.
    :ivar node: the dispatcher ExecNode owning the task.
    :ivar reason: last diagnostic reason for still waiting.
    :ivar bypass_total: number of later overlapping admissions already
        granted ahead of this waiter.
    """
    __slots__ = ('seq', 'name', 'needs', 'node', 'reason', 'bypass_total')

    def __init__(self, seq, name, needs, node, reason):
        self.seq = seq
        self.name = name
        self.needs = needs
        self.node = node
        self.reason = reason
        self.bypass_total = 0

    def __repr__(self):
        return "ResourceWaiter(#{} {})".format(self.seq, self.name)


class ResourceManager(object):
    """Parent-side resource accounting, admission and fairness control.

    One instance is created per run, so a fresh ``DoitMain`` invocation
    always starts with empty allocation and reset waiting counters.

    :ivar resource_limits: validated capacity dict.
    :ivar bypass_limit: maximum number of overlapping bypasses per waiter.
    :ivar check_count: number of individual resource requirement checks
        performed (diagnostic counter).
    :ivar admit_count: number of admissions granted.
    :ivar wait_count: number of times tasks entered the wait set.
    :ivar events: optional list of diagnostic events, enabled with
        ``record_events=True``.
    """

    def __init__(self, resource_limits=None, bypass_limit=DEFAULT_BYPASS_LIMIT,
                 record_events=False):
        self.resource_limits = validate_resource_limits(resource_limits)
        self.bypass_limit = validate_bypass_limit(bypass_limit)
        # current committed allocation per resource
        self.allocated = {name: 0 for name in self.resource_limits}
        # active waiters in arrival order; ResourceWaiter instances
        self.waiters = []
        # resource name -> list of waiters needing it (arrival order)
        self.by_resource = {name: [] for name in self.resource_limits}
        # waiters whose blocking condition may have changed, by seq
        self._recheck = []
        self._in_recheck = set()
        self._seq = 0
        # committed tasks: ExecNode -> needs dict
        self.commitments = {}
        self.check_count = 0
        self.admit_count = 0
        self.wait_count = 0
        self.events = [] if record_events else None
        # The parent process/thread drives every state change; the lock
        # documents the critical section and protects the shared allocation
        # bookkeeping when thread-based parallelism is used.
        self._lock = threading.RLock()

    # ------------------------------------------------------------------
    # validation
    def validate_task(self, task):
        """Validate a task declaration against the configured capacities.

        Structural validation already happened at ``Task`` construction;
        here positive requirements are cross-checked against the limits.

        :raises InvalidTask: on an unknown resource or an oversized demand.
        """
        for resource, need in task.resources.items():
            if need == 0:
                continue
            if resource not in self.resource_limits:
                msg = ("Task '{}' requires resource '{}' but no capacity is "
                       "declared in DOIT_CONFIG 'resource_limits'.").format(
                           task.name, resource)
                raise InvalidTask(msg)
            if need > self.resource_limits[resource]:
                msg = ("Task '{}' requires {} of resource '{}' but its "
                       "capacity is {}.").format(
                           task.name, need, resource,
                           self.resource_limits[resource])
                raise InvalidTask(msg)

    # ------------------------------------------------------------------
    # diagnostics
    def _record(self, kind, name, **info):
        if self.events is not None:
            event = {'kind': kind, 'task': name}
            event.update(info)
            self.events.append(event)

    def has_waiters(self):
        return bool(self.waiters)

    def has_commitments(self):
        """True while at least one task holds committed resources.

        When nothing is committed but waiters remain, no future release can
        unblock them (their only releasers failed or were skipped), so the
        run must not keep workers alive on their account.
        """
        return bool(self.commitments)

    def snapshot(self):
        """return a point-in-time, independent copy of the scheduler state."""
        with self._lock:
            return {
                'limits': dict(self.resource_limits),
                'allocated': dict(self.allocated),
                'running': [node.task.name for node in self.commitments],
                'waiting': [(w.name, dict(w.needs), w.reason,
                             w.bypass_total) for w in self.waiters],
                'check_count': self.check_count,
                'admit_count': self.admit_count,
                'wait_count': self.wait_count,
            }

    # ------------------------------------------------------------------
    # internal helpers
    @staticmethod
    def _intersects(needs_a, needs_b):
        """True if two requirement dicts share any positive resource."""
        if len(needs_a) > len(needs_b):
            needs_a, needs_b = needs_b, needs_a
        for resource in needs_a:
            if resource in needs_b:
                return True
        return False

    def _fits(self, needs):
        """check all requirements at once; counts individual checks."""
        for resource, need in needs.items():
            self.check_count += 1
            if self.allocated[resource] + need > self.resource_limits[resource]:
                return False
        return True

    def _fairness_blocked(self, waiter):
        """return an earlier overlapping waiter whose bypass budget is spent.

        With ``bypass_limit == 0`` any earlier overlapping waiter blocks,
        i.e. overlapping tasks may never jump the queue.
        """
        for earlier in self.waiters:
            if earlier.seq >= waiter.seq:
                break
            if (earlier.bypass_total >= self.bypass_limit and
                    self._intersects(earlier.needs, waiter.needs)):
                return earlier
        return None

    def _mark_recheck(self, waiter):
        """schedule a waiter for re-evaluation after a relevant change."""
        if waiter.seq not in self._in_recheck:
            self._in_recheck.add(waiter.seq)
            self._recheck.append(waiter)

    def _enqueue(self, node, needs, reason):
        waiter = ResourceWaiter(self._seq, node.task.name, needs, node, reason)
        self._seq += 1
        self.waiters.append(waiter)
        for resource in needs:
            self.by_resource[resource].append(waiter)
        self.wait_count += 1
        self._mark_recheck(waiter)
        self._record('wait', node.task.name, needs=dict(needs), reason=reason)
        return waiter

    def _remove_waiter(self, waiter):
        self.waiters.remove(waiter)
        for resource in waiter.needs:
            queue = self.by_resource[resource]
            queue.remove(waiter)
            # the new head of this resource queue must get a chance next
            if queue:
                self._mark_recheck(queue[0])

    def _count_bypass(self, admitted_needs, admitted_seq):
        """charge one bypass to every earlier overlapping active waiter."""
        for earlier in self.waiters:
            if earlier.seq >= admitted_seq:
                break
            if self._intersects(earlier.needs, admitted_needs):
                earlier.bypass_total += 1

    def _commit(self, node, needs):
        for resource, need in needs.items():
            self.allocated[resource] += need
        self.commitments[node] = needs
        self.admit_count += 1
        # event recording is the caller's responsibility (context differs
        # between a fresh admission and a parked waiter being woken up)

    # ------------------------------------------------------------------
    # admission API
    def try_admit(self, node):
        """attempt to admit a dependency-ready, selected task node.

        :return: True if the task now holds its resources and may run.
                 False if it was added to the wait set.
        :raises InvalidTask: if the task declaration is invalid (delayed
                 tasks are validated here, at admission time).
        """
        task = node.task
        with self._lock:
            self.validate_task(task)
            needs = {r: n for r, n in task.resources.items() if n}

            # zero-footprint tasks never wait, never bypass anyone
            if not needs:
                self._commit(node, needs)
                self._record('admit', task.name, needs={},
                             allocated=dict(self.allocated))
                return True

            # fairness boundary: an earlier waiter with spent budget blocks
            # every later task sharing a resource with it
            probe = ResourceWaiter(self._seq, task.name, needs, node, 'probe')
            blocker = self._fairness_blocked(probe)
            if blocker is not None:
                self._enqueue(
                    node, needs,
                    'fairness: blocked by {}'.format(blocker.name))
                return False

            if not self._fits(needs):
                self._enqueue(node, needs, 'capacity')
                return False

            # grant admission, charging bypasses of every earlier waiter
            # sharing a resource with this task
            bypassed = [w.name for w in self.waiters
                        if self._intersects(w.needs, needs)]
            self._count_bypass(needs, self._seq)
            self._commit(node, needs)
            self._record('admit', task.name, needs=dict(needs),
                         bypassed=bypassed,
                         allocated=dict(self.allocated))
            return True

    def pump(self):
        """re-evaluate waiters whose blockers may have changed.

        Only waiters marked through the per-resource indexes are considered,
        so a completion touching few resources never scans the whole wait
        set.  Candidates keep their arrival order; at most one task is
        admitted per call (one free worker slot).  Candidates that still
        cannot run stay in the recheck set so they are retried on the next
        relevant change instead of being lost.

        :return: the admitted ExecNode or None if nothing can run now.
        """
        with self._lock:
            if not self._recheck:
                return None
            candidates = []
            while self._recheck:
                waiter = self._recheck.pop()
                self._in_recheck.discard(waiter.seq)
                if waiter in self.waiters:
                    candidates.append(waiter)
            if not candidates:
                return None
            candidates.sort(key=lambda w: w.seq)

            admitted = None
            deferred = []
            for waiter in candidates:
                if waiter not in self.waiters:
                    continue
                if admitted is not None:
                    # one slot per pump; keep every remaining candidate live
                    deferred.append(waiter)
                    continue
                # fairness still has priority over capacity
                blocker = self._fairness_blocked(waiter)
                if blocker is not None:
                    waiter.reason = 'fairness: blocked by {}'.format(
                        blocker.name)
                    deferred.append(waiter)
                    continue
                if not self._fits(waiter.needs):
                    waiter.reason = 'capacity'
                    deferred.append(waiter)
                    continue
                # admit the parked waiter
                self._remove_waiter(waiter)
                bypassed = [w.name for w in self.waiters
                            if self._intersects(w.needs, waiter.needs)]
                self._count_bypass(waiter.needs, waiter.seq)
                self._commit(waiter.node, waiter.needs)
                self._record('admit', waiter.name,
                             needs=dict(waiter.needs),
                             bypassed=bypassed,
                             allocated=dict(self.allocated))
                admitted = waiter.node
            for waiter in deferred:
                self._mark_recheck(waiter)
            return admitted

    def release(self, node):
        """return the resources committed for ``node`` (idempotent)."""
        with self._lock:
            needs = self.commitments.pop(node, None)
            if needs is None:
                return
            for resource, need in needs.items():
                self.allocated[resource] -= need
            self._record('release', node.task.name, needs=dict(needs),
                         allocated=dict(self.allocated))
            # only waiters of the resources that changed are woken up
            for resource in needs:
                for waiter in self.by_resource[resource]:
                    self._mark_recheck(waiter)
