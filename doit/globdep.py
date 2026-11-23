"""Support for ``glob_dep``: pattern-based file dependencies.

A ``glob_dep`` is a list of glob patterns.  At run time the patterns are
expanded against the current working directory and the *regular files* they
match become file dependencies of the task.

The module deliberately implements its own small tree walk instead of using
:func:`glob.glob`.  Python's ``glob`` silently swallows directory read
errors and follows directory symbolic links while expanding ``**``.  For
dependency checking both behaviours are unsafe: a read error must not look
like "nothing changed", and following directory links makes the result depend
on which spelling of a path happened to be enumerated.

Only lexical path normalization is performed (``abspath`` + ``normpath``),
symbolic links are never resolved.

Two different views are provided:

* :func:`scan_glob` returns the *actual* files currently visible on disk plus
  a manifest mapping each normalized path to its content digest.
* :func:`match_target` checks whether a *potential* path (a declared task
  target that might not exist yet) is lexically matched by a pattern.  This
  is used when building the task graph so a consumer depends on the producer
  even before the produced files exist.

Representation choice
---------------------

The persisted observation is *the complete sorted file list plus one content
digest per file* (a "manifest").  The alternatives considered:

* A single reusable summary (e.g. a rolling hash over all members and their
  content) is compact and cheap to compare, but it cannot say *which* file was
  added, removed or changed.  Diagnostics (``doit info``) would only be able
  to report "something changed", which the contract explicitly forbids.
  A rolling hash also conflates a rename with a content change.
* A per-file signature list without content digests relies on size+mtime and
  cannot honour the "same size and restored mtime still detects a content
  change" guarantee.
* The full manifest needs ``O(#matched paths)`` resident memory (one hex
  digest per path, never the file bytes) and one hash per unique path per
  observation.  That satisfies the memory bound (grows with the number of
  paths, not with the number of bytes) while giving exact, actionable
  diagnostics and the strong content guarantee.

Scanning is therefore repeated for each observation (start and end of an
action), but each matched path is hashed at most once per observation because
patterns are de-duplicated by normalized path before hashing.
"""

import fnmatch
import os
import stat
from collections import namedtuple
from functools import lru_cache


class GlobError(Exception):
    """Error while scanning glob_dep patterns.

    Scanning/reading errors are reported, never silently treated as an empty
    match set.
    """

    def __init__(self, message, path=None):
        super().__init__(message)
        self.path = path


#: Result of observing a ``glob_dep`` at one point in time.
#:
#: * ``patterns``: tuple of the declared patterns (declaration identity)
#: * ``files``: tuple of normalized absolute paths, sorted
#: * ``manifest``: dict normalized path -> content digest
GlobState = namedtuple('GlobState', 'patterns files manifest')


def normalize_path(path):
    """Return a lexical absolute normalized path.

    No symbolic link is resolved; ``./``, ``..`` and relative/absolute
    spellings that denote the same location collapse to the same string.
    """
    return os.path.normpath(os.path.abspath(path))


def normalize_patterns(patterns):
    """De-duplicate patterns preserving declaration order.

    Patterns are *not* lexically normalized: ``..`` next to a wildcard is
    meaningful for glob expansion (``a/*/../b``).  Duplicated identical
    declarations still count as one.
    """
    result = []
    seen = set()
    for pattern in patterns:
        if pattern not in seen:
            seen.add(pattern)
            result.append(pattern)
    return tuple(result)


def _has_magic(segment):
    # same set of meta characters accepted by Python's glob/fnmatch
    return any(char in segment for char in '*?[')


def _is_hidden(name):
    return name.startswith('.')


def _split_abs(path):
    """split an absolute path into non-empty segments (lexical only)"""
    drive, rest = os.path.splitdrive(path)
    return [part for part in rest.split(os.sep) if part]


def _segment_match(pattern, name):
    """match a single path name against a single glob segment"""
    # Like Python glob, '*' and '?' do not match a leading dot.
    if _is_hidden(name) and not _is_hidden(pattern):
        return False
    return fnmatch.fnmatchcase(name, pattern)


def _match_segments(pattern_segments, path_segments):
    """lexically match already split pattern/path segments"""
    @lru_cache(maxsize=None)
    def rec(pi, si):
        if pi == len(pattern_segments):
            return si == len(path_segments)
        segment = pattern_segments[pi]
        if segment == '**':
            # '**' matches zero directory levels...
            if rec(pi + 1, si):
                return True
            # ...or one more non-hidden level.
            if si < len(path_segments) and not _is_hidden(path_segments[si]):
                return rec(pi, si + 1)
            return False
        if si >= len(path_segments):
            return False
        if not _segment_match(segment, path_segments[si]):
            return False
        return rec(pi + 1, si + 1)

    return rec(0, 0)


def match_target(pattern, target):
    """Return True if `target` is lexically matched by `pattern`.

    Both sides are normalized to absolute lexical paths.  The target does not
    need to exist; this is used for graph construction (potential producers).
    Symbolic links are not resolved.
    """
    abs_pattern = normalize_path(pattern)
    abs_target = normalize_path(target)
    pattern_segments = _split_abs(abs_pattern)
    target_segments = _split_abs(abs_target)
    return _match_segments(tuple(pattern_segments), tuple(target_segments))


#### filesystem walk


def _join(dirname, name):
    if not dirname:
        return name
    return os.path.join(dirname, name)


def _scandir(dirname):
    """list a directory as sorted names.

    Missing directories simply have no entries.  Any other error (permission,
    I/O, ...) is raised instead of being downgraded to an empty directory.
    """
    target = dirname if dirname else os.curdir
    try:
        with os.scandir(target) as entries:
            return sorted(entry.name for entry in entries)
    except (FileNotFoundError, NotADirectoryError):
        return []
    except OSError as exc:
        raise GlobError(
            "Cannot scan directory '{}' for glob_dep: {}".format(
                dirname or os.curdir, exc), dirname) from exc


def _lstat(path):
    """lstat that maps "does not exist" to None and other errors to GlobError"""
    try:
        return os.lstat(path)
    except (FileNotFoundError, NotADirectoryError):
        return None
    except OSError as exc:
        raise GlobError(
            "Cannot stat '{}' for glob_dep: {}".format(path, exc), path) from exc


def _kind(path):
    """lexical kind of an entry: 'file', 'dir', 'link', 'other' or None"""
    info = _lstat(path)
    if info is None:
        return None
    if stat.S_ISLNK(info.st_mode):
        return 'link'
    if stat.S_ISDIR(info.st_mode):
        return 'dir'
    if stat.S_ISREG(info.st_mode):
        return 'file'
    return 'other'


def _is_real_dir(path):
    """True for a directory that is not a symbolic link"""
    return _kind(path) == 'dir'


def _add_file(path, matches, pattern):
    """add `path` to matches if it is a regular file.

    A symbolic link that resolves to a regular file is explicitly rejected:
    otherwise some runs would read the link target while others only record
    the directory entry.  Links to directories and broken links are not
    regular files and are simply ignored (directory links are never
    followed).
    """
    kind = _kind(path)
    if kind is None:
        return
    if kind == 'link':
        if os.path.isfile(path):  # link whose target is a regular file
            raise GlobError(
                "glob_dep pattern '{}' matched symbolic link '{}'. "
                "glob_dep only accepts regular files and does not follow "
                "symbolic links.".format(pattern, path), path)
        return
    if kind == 'file':
        matches.append(path)


def _recursive_entries(dirname):
    """yield (path, kind) below `dirname`, skipping hidden entries.

    Directory symbolic links are reported as ``'link'`` and never descended
    into.  File symbolic links are reported too, so a terminal match can
    reject them explicitly.
    """
    for name in _scandir(dirname):
        if _is_hidden(name):
            continue
        path = _join(dirname, name)
        kind = _kind(path)
        if kind is None:
            continue
        yield path, kind


def _recursive_dirs(dirname):
    """all descendant directories, not following symlinks, no hidden dirs"""
    result = []
    stack = [dirname]
    seen = set()
    while stack:
        current = stack.pop()
        for path, kind in _recursive_entries(current):
            if kind == 'dir' and path not in seen:
                seen.add(path)
                result.append(path)
                stack.append(path)
    return result


def _recursive_files(dirname):
    """all descendant regular files plus links, not following directory links"""
    result = []
    stack = [dirname]
    seen = set()
    while stack:
        current = stack.pop()
        for path, kind in _recursive_entries(current):
            if kind == 'dir':
                if path not in seen:
                    seen.add(path)
                    stack.append(path)
            elif kind in ('file', 'link'):
                result.append(path)
    return result


def _pattern_root(pattern):
    """return (root, segments) for a pattern, splitting lexically"""
    drive, rest = os.path.splitdrive(pattern)
    if os.path.isabs(pattern):
        root = drive + os.sep
        if rest.startswith(os.sep):
            rest = rest[len(os.sep):]
    else:
        root = ''
    segments = [seg for seg in rest.split(os.sep) if seg != '']
    return root, segments


def _walk(pattern):
    """return list of lexical paths matched by `pattern` that are regular files"""
    root, segments = _pattern_root(pattern)
    # a pattern denoting the cwd/root itself is a directory, never a file
    segments = [seg for seg in segments if seg != '.']
    if not segments:
        return []

    dirs = [root]
    matches = []

    for index, segment in enumerate(segments):
        last = index == len(segments) - 1
        new_dirs = []

        if segment == '**':
            if last:
                for dirname in sorted(set(dirs)):
                    for path in _recursive_files(dirname):
                        _add_file(path, matches, pattern)
            else:
                for dirname in sorted(set(dirs)):
                    new_dirs.append(dirname)
                    new_dirs.extend(_recursive_dirs(dirname))
            dirs = new_dirs
            continue

        if segment == '..':
            for dirname in sorted(set(dirs)):
                base = dirname if dirname else os.curdir
                parent = os.path.normpath(os.path.join(base, '..'))
                if _is_real_dir(parent):
                    new_dirs.append(parent)
            dirs = new_dirs
            continue

        magic = _has_magic(segment)
        for dirname in sorted(set(dirs)):
            if magic:
                candidates = [name for name in _scandir(dirname)
                              if _segment_match(segment, name)]
            else:
                candidates = [segment]

            for name in candidates:
                path = _join(dirname, name)
                if last:
                    _add_file(path, matches, pattern)
                elif _is_real_dir(path):
                    new_dirs.append(path)

        dirs = new_dirs

    return matches


def _file_md5(path):
    # imported lazily to avoid a circular import with dependency.py and so
    # that the chunked implementation used there is shared.
    from .dependency import get_file_md5
    return get_file_md5(path)


def scan_glob(patterns, hash_func=None):
    """Expand `patterns` and read the matched files.

    :return: ``(files, manifest)`` where ``files`` is a sorted tuple of
             normalized absolute paths and ``manifest`` maps each path to its
             content md5.
    :raises GlobError: on directory scan / stat / content read errors, or if a
             matched path is a symbolic link to a file.

    Each unique normalized path is hashed exactly once even if several
    patterns match it.  Hashing is delegated to the chunked ``get_file_md5``.
    """
    if hash_func is None:
        hash_func = _file_md5

    # de-duplicate by normalized path (not by inode)
    unique = {}
    for pattern in patterns:
        for raw_path in _walk(pattern):
            unique.setdefault(normalize_path(raw_path), raw_path)

    files = tuple(sorted(unique))
    manifest = {}
    for path in files:
        kind = _kind(path)
        if kind is None:
            raise GlobError(
                "File matched by glob_dep disappeared before it could be "
                "read: '{}'".format(path), path)
        if kind == 'link':
            raise GlobError(
                "glob_dep matched symbolic link '{}'. glob_dep only accepts "
                "regular files and does not follow symbolic links.".format(
                    path), path)
        if kind != 'file':
            raise GlobError(
                "glob_dep matched a non regular file: '{}'".format(path), path)
        try:
            manifest[path] = hash_func(path)
        except OSError as exc:
            raise GlobError(
                "Cannot read file matched by glob_dep '{}': {}".format(
                    path, exc), path) from exc
    return files, manifest


def scan_state(patterns, hash_func=None):
    """Return a :class:`GlobState` for `patterns`."""
    patterns = normalize_patterns(patterns)
    files, manifest = scan_glob(patterns, hash_func=hash_func)
    return GlobState(patterns=patterns, files=files, manifest=manifest)


def manifest_diff(start, end):
    """Return ``(added, removed, changed)`` between two manifests."""
    start = start or {}
    end = end or {}
    added = sorted(set(end) - set(start))
    removed = sorted(set(start) - set(end))
    changed = sorted(path for path in set(start) & set(end)
                     if start[path] != end[path])
    return added, removed, changed
