"""Value selection, canonical comparison and delivery for ``task.getargs``.

``getargs`` transfers *saved values* from a producer task to a consumer task.
The consumer's up-to-date decision must be based on exactly the values it
actually consumes -- not on the producer's last action ``result`` (which may be
unchanged) and not on values it never selected.

A declaration is one of:

* a single key of a single task: ``('<task-id>', '<key>')``;
* the whole saved-values mapping of a single task: ``('<task-id>', None)``;
* the same, for every member of a group task, keyed by short sub-task name.

Canonical comparison
--------------------

A *snapshot* is built from the selected saved values and frozen through JSON:

* ``sort_keys=True`` -- dictionary insertion order is irrelevant;
* compact separators -- formatting noise is irrelevant;
* object identity never takes part (the snapshot is a JSON document);
* lists stay ordered; a missing key is distinct from a present ``None``;
* ``true``/``false`` stay distinct from ``1``/``0``;
* tuples are normalised to arrays (matching a DB round-trip);
* NaN and infinities are rejected -- their JSON spelling is not portable and
  they do not compare equal to themselves.

The payload also records the binding for every declared argument (source task,
selected key, group flag), so changing the bound task, the selected key, the
whole-dict selection or the set of declared arguments invalidates an old
success record.  Unselected keys of a producer never appear in the payload, so
changing them does not.

What is persisted
-----------------

Only a fixed-size digest (``v1:<sha256 hex>``) of the frozen snapshot is
stored, as the task's *success basis*.  The full snapshot is transient: it
lives only for the duration of one delivery, from which each consumer receives
an independent deep copy.  Compared with persisting the whole canonical
document this keeps DB growth constant per consumer (a wide value would
otherwise be copied into the DB once per consumer), while still attesting to
exactly the delivered content.  Absence of the digest (a fresh DB, or an old
DB written before this scheme) is "no reliable basis" and causes one safe
recomputation; the next success writes it.
"""

import copy
import hashlib
import json


# key used to persist the getargs success basis inside a task's DB entry
GETARGS_KEY = 'getargs:'
# bump when the snapshot layout changes: an unrecognised basis is treated as
# "no basis", which triggers one safe recomputation.
GETARGS_VERSION = 1


class GetargsError(Exception):
    """A getargs value can not be read or compared.

    Raised when a declared task/key is absent or a value is not canonical
    JSON.  During the up-to-date check it makes the consumer run (so the
    normal setup/recompute path can report the real problem); when the value
    is actually delivered it is reported as a dependency error.
    """


def canonical(value):
    """Return a deterministic, order/identity independent representation.

    ``sort_keys`` removes any dependency on dict insertion order; compact
    separators avoid formatting noise.  ``allow_nan=False`` rejects NaN and
    infinities.  Only acyclic dicts/lists and JSON scalars are supported.
    """
    try:
        return json.dumps(value, sort_keys=True, separators=(',', ':'),
                          allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise GetargsError(
            "getargs value is not canonical JSON (finite scalars, acyclic "
            "dicts and lists only): {}".format(exc)) from exc


def _basis(document):
    """Return the fixed-size, versioned success basis for a document."""
    digest = hashlib.sha256(document.encode('utf-8')).hexdigest()
    return 'v{}:{}'.format(GETARGS_VERSION, digest)


def is_group(tasks_dict, task_id):
    """Return True when ``task_id`` names a group task."""
    task = tasks_dict.get(task_id)
    return bool(task is not None and task.has_subtask)


def group_member_items(tasks_dict, group_id):
    """Return ``[(short_name, full_task_name), ...]`` sorted by short name.

    Only real sub-tasks belong to the mapping.  ``subtask_of`` is the
    authoritative marker; the group's ``task_dep`` may also contain external
    prerequisites (other tasks, setup tasks, ...) and those must never be
    folded into the value mapping.  The prefix fallback keeps compatibility
    with group tasks assembled by hand without ``subtask_of``.
    """
    names = set()
    for task in tasks_dict.values():
        if getattr(task, 'subtask_of', None) == group_id:
            names.add(task.name)

    if not names:
        group = tasks_dict.get(group_id)
        prefix = group_id + ':'
        known = set(tasks_dict) | {t.name for t in tasks_dict.values()}
        for dep in (group.task_dep if group is not None else ()):
            if dep in known and dep.startswith(prefix):
                names.add(dep)

    prefix = group_id + ':'
    items = []
    for name in names:
        short = name[len(prefix):] if name.startswith(prefix) else name
        items.append((short, name))
    items.sort()
    return items


def read_value(dep_manager, task_id, key_name):
    """Read the raw saved value selected by ``key_name``.

    The returned object belongs to the dependency manager's cache: callers
    must treat it as read-only and copy it before handing it to user code.
    A missing task or key is an error, never silently ``None``/``{}``.
    """
    if not dep_manager.has_task(task_id):
        raise GetargsError(
            "taskid '%s' has no computed value!" % task_id)
    values = dep_manager.get_values_raw(task_id)
    if key_name is None:
        return values
    if key_name not in values:
        raise GetargsError(
            "Invalid arg name. Task '%s' has no value for '%s'."
            % (task_id, key_name))
    return values[key_name]


def _payload(dep_manager, tasks_dict, source, key_name):
    """Build the canonical payload for one getargs declaration."""
    if is_group(tasks_dict, source):
        members = {}
        for short, name in group_member_items(tasks_dict, source):
            members[short] = read_value(dep_manager, name, key_name)
        return {
            'source': source,
            'key': key_name,
            'group': True,
            'members': members,
        }
    return {
        'source': source,
        'key': key_name,
        'group': False,
        'value': read_value(dep_manager, source, key_name),
    }


def _document(dep_manager, tasks_dict, getargs):
    """Build the canonical JSON document for all declared values.

    The payloads reference the raw saved objects; the returned string is a
    self-contained freeze of them, so it can not be affected by later
    mutation of the producer's saved values.
    """
    payloads = {}
    for arg_name, (source, key_name) in getargs.items():
        payloads[arg_name] = _payload(dep_manager, tasks_dict, source, key_name)
    return canonical({'version': GETARGS_VERSION, 'args': payloads})


def basis(dep_manager, tasks_dict, getargs):
    """Return the success basis of the current values (no delivery copy).

    This is the cheap path used by the up-to-date check: build the canonical
    document and hash it, without decoding it back into Python objects.
    """
    return _basis(_document(dep_manager, tasks_dict, getargs))


def freeze(dep_manager, tasks_dict, getargs):
    """Take one snapshot of all declared values and freeze it.

    :return: ``(basis, frozen)``.  ``basis`` is the fixed-size success basis
        persisted on success; ``frozen`` is the JSON-decoded form of the very
        document that was hashed, from which delivery copies are made.
    :raises GetargsError: a task/key is absent or a value is not canonical.
    """
    document = _document(dep_manager, tasks_dict, getargs)
    # Decode exactly what was hashed: this normalises tuples to lists and
    # guarantees delivered content and successive basis computations agree.
    return _basis(document), json.loads(document)


def deliver(frozen):
    """Return independent copies of a frozen snapshot's values, by arg name.

    A consumer may mutate whatever it receives without touching the saved
    values of the producer, another consumer's arguments or the success
    basis.  Aliases *inside* one argument are preserved by ``copy.deepcopy``;
    aliases shared by separate arguments are not required to be restored.
    """
    delivered = {}
    for arg_name, payload in frozen['args'].items():
        if payload['group']:
            delivered[arg_name] = copy.deepcopy(payload['members'])
        else:
            delivered[arg_name] = copy.deepcopy(payload['value'])
    return delivered


def derive_basis(dep_manager, tasks_dict, getargs):
    """Compute a success basis without delivering anything.

    Used by ``reset-dep``, which records the current values as the new
    baseline.  Returns ``None`` when a declared value is not available: no
    basis is stored and the task is safely recomputed on the next run.
    """
    if not getargs:
        return None
    try:
        return basis(dep_manager, tasks_dict, getargs)
    except GetargsError:
        return None
