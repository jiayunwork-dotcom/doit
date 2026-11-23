.. meta::
   :description: pydoit guide - core concepts: tasks, actions, dependencies and targets.
   :keywords: python, doit, documentation, guide, introduction, task, actions, dependencies, targets, cmd action

.. title:: Introduction to Tasks, basic metadata and operations

========
Tasks
========

Intro
-------

`doit` is all about automating task dependency management and execution.
Tasks can execute external shell commands/scripts or python functions
(actually any callable).
So a task can be anything you can code :)

Tasks are defined in plain `python <http://python.org/>`_ module with some
conventions.

.. note::

    You should be comfortable with python basics. If you don't know python yet
    check `Python tutorial <http://docs.python.org/tut/>`_.


A function that starts with the name `task_` defines a *task-creator* recognized
by `doit`.
These functions must return (or yield) dictionaries representing a *task*.
A python module/file that defines *tasks* for `doit` is called **dodo** file
(that is something like a `Makefile` for `make`).

Take a look at this example (file dodo.py):

.. literalinclude:: samples/hello.py

When `doit` is executed without any parameters it will look for tasks in a
file named `dodo.py` in the current folder and execute its tasks.


.. code-block:: console

  $ doit
  .  hello

On the output it displays which tasks were executed.
In this case the `dodo` file has only one task, `hello`.


Actions
--------

Every *task* must define `actions`.
It can optionally define other attributes like `targets`, `file_dep`,
`verbosity`, `doc` ...

`actions` define what the task actually does.
`actions` is always a list that can have any number of elements.
The actions of a task are always run sequentially.
There are 2 basic kinds of `actions`: *cmd-action* and *python-action*.
The action "result" is used to determine if task execution was successful or not.


python-action
^^^^^^^^^^^^^^

If `action` is a python callable or a tuple `(callable, *args, **kwargs)`
- only `callable` is required.
The callable must be a function, method or callable object.
Classes and built-in functions are not allowed.
``args`` is a sequence and ``kwargs`` is a dictionary that will be used
as positional and keywords arguments for the callable.
See `Keyword Arguments <http://docs.python.org/tutorial/controlflow.html#keyword-arguments>`_.

The result of the task is given by the returned value of the
``action`` function.

For **successful** completion it must return one of:

* `True`
* `None`
* a dictionary
* a string

For **unsuccessful** completion it must return one of:

* `False` indicates the task generally failed
* if it raises any exception, it will be considered an error
* it can also explicitly return an instance of :py:class:`TaskFailed`
  or :py:class:`TaskError`

If the action returns a type other than the types already discussed, the action
will be considered a failure, although this behavior might change in future
versions.

.. literalinclude:: samples/tutorial_02.py

The function `task_hello` is a *task-creator*, not the task itself.
The body of the task-creator function is always executed when the dodo
file is loaded.

.. topic:: task-creators vs actions

  The body of task-creators are executed even if the task is not going
  to be executed.
  The body of task-creators should be used to create task metadata only,
  not execute tasks!
  From now on when the  documentation says that a *task* is executed,
  read "the task's actions are executed".


`action` parameters can be passed as ``kwargs``.

.. literalinclude:: samples/task_kwargs.py


cmd-action
^^^^^^^^^^^

CmdAction's are executed in a subprocess (using python
`subprocess.Popen <http://docs.python.org/library/subprocess.html#popen-constructor>`_).

If `action` is a string, the command will be executed through the shell.
(Popen argument shell=True).

It is easy to include dynamic (on-the-fly) behavior to your tasks with
python code from the `dodo` file. Let's take a look at another example:


.. literalinclude:: samples/cmd_actions.py

.. note::

  The body of the *task-creator* is always executed,
  so in this example the line `msg = 3 * "hi! "` will always be executed.


If `action` is a list of strings and instances of any Path class from
`pathlib <https://docs.python.org/3/library/pathlib.html>`_, by default it will
be executed **without the shell** (Popen argument shell=False).

.. literalinclude:: samples/cmd_actions_list.py


For complex commands it is also possible to pass a callable that returns
the command string. In this case you must explicit import CmdAction.

.. literalinclude:: samples/cmd_from_callable.py


You might also explicitly import ``CmdAction`` in case you want to pass extra
parameters to ``Popen`` like ``cwd``.
All keyword parameter from ``Popen`` can be used on ``CmdAction`` (except
``stdout`` and ``stderr``).

.. note::

  Different from `subprocess.Popen`, `CmdAction` `shell` argument defaults to
  `True`. All other `Popen` arguments can also be passed in `CmdAction` except
  `stdout` and `stderr`



The result of the task follows the shell convention.
If the process exits with the value `0` it is successful.
Any other value means the task failed.


.. _custom-actions:

custom actions
^^^^^^^^^^^^^^^^^^^

It is possible to create other type of actions,
check :ref:`tools.LongRunning<tools.LongRunning>` as an example.



task name
------------

By default a task name is taken from the name of the python function
that generates the task.
For example a `def task_hello` would create a task named ``hello``.

It is possible to explicitly set a task name with the parameter ``basename``.

.. literalinclude:: samples/task_name.py


.. code-block:: console

  $ doit
  .  hello
  .  hello2


When explicit using ``basename`` the task-creator is not limited
to create only one task.
Using ``yield`` it can generate several tasks at once.
It is also possible to ``yield`` a generator that generate tasks.
This is useful to write some generic/reusable task-creators.

.. literalinclude:: samples/task_reusable.py

.. code-block:: console

  $ doit
  .  t2
  .  t1


listing tasks
-------------

``doit`` has built-in sub-command that can be used to list/print all tasks.

.. code-block:: console

   $ doit list
   hello    say hello
   hello2   say hello again


doc
---

Every task has an associated documentation.
By default this documentation is taken from the task-creator function's docstring.
It can also be set by the ``doc`` attribute.

.. literalinclude:: samples/task_doc.py

.. code-block:: console

  $ doit list
  .  hello  say hello



sub-tasks
---------

Most of the time we want to apply the same task several times
in different contexts.

The task function can return a python-generator that yields dictionaries.
Since each sub-task must be uniquely identified it requires an
additional field ``name``.

.. literalinclude:: samples/subtasks.py


.. code-block:: console

    $ doit
    .  create_file:file0.txt
    .  create_file:file1.txt
    .  create_file:file2.txt



avoiding empty sub-tasks
^^^^^^^^^^^^^^^^^^^^^^^^^^

If you are not sure sub-tasks will be created for a given ``basename``
but you want to make sure that a task exists,
you can yield a sub-task with ``name`` equal to ``None``.
This can also be used to set the task ``doc`` and ``watch`` attributes.

.. literalinclude:: samples/empty_subtasks.py

.. code-block:: console

  $ doit
  $ doit list
  do_x   docs for X



Dependencies & Targets
-------------------------

One of the main ideas of `doit` (and other build-tools) is to check if the
tasks/targets are **up-to-date**. In case there is no modification in the
dependencies and the targets already exist, it skips the task execution to
save time, as it would produce the same output from the previous run.

Dependency
  A dependency indicates an input to the task execution.

Target
  A *target* is the result/output file produced by the task execution.


i.e. In a compilation task the source file is a *file_dep*,
the object file is a *target*.

.. literalinclude:: samples/compile.py


`doit` automatically keeps track of file dependencies. It saves the
signature (MD5) of the dependencies every time the task is completed successfully.

So if there are no modifications to the dependencies and you run `doit` again.
The execution of the task's actions is skipped.


.. code-block:: console

  $ doit
  .  compile
  $ doit
  -- compile

Note the ``--`` (2 dashes, one space) on the command output on the second
time it is executed. It means, this task was up-to-date and not executed.


.. _file-dep:

file_dep (file dependency)
-----------------------------

Different from most build-tools dependencies are on tasks, not on targets.
So `doit` can take advantage of the "execute only if not up-to-date" feature
even for tasks that don't define targets.

Let's say you work with a dynamic language (python in this example).
You don't need to compile anything but you probably want to apply a lint-like
tool (i.e. `pyflakes <http://pypi.python.org/pypi/pyflakes>`_) to your
source code files. You can define the source code as a dependency to the task.

Every dependency in file_dep list should be a string or an instance of any
Path class from `pathlib <https://docs.python.org/3/library/pathlib.html>`_.


.. literalinclude:: samples/checker.py

.. code-block:: console

   $ doit
   .  checker
   $ doit
   -- checker


`doit` checks if `file_dep` was modified or not
(by comparing the file content's MD5). If there are no changes the action
is not executed again as it would produce the same result.

Note the ``--`` again to indicate the execution was skipped.

Traditional build-tools can only handle files as "dependencies".
`doit` has several ways to check for dependencies, those will be introduced later.


.. note::

   `doit` saves the MD5 of a `file_dep` after the actions are executed.
   Be careful about editing a `file_dep` while a task is running because
   `doit` might save the MD5 of a version of the file that is different
   than it actually used to execute the task.


.. _glob-dep:

glob_dep (pattern file dependency)
----------------------------------

When the set of input files is dynamic (for example, partitions that are
created and deleted every day), list glob patterns in ``glob_dep`` instead of
spelling out every path in ``file_dep``.  Patterns are expanded at run time,
right before the task actions start, so partitions produced by a previous task
in the same run are picked up.

.. code-block:: python

   def task_merge():
       return {
           'actions': [merge],
           'glob_dep': ['landing/**/*.csv'],
           'targets': ['merged.txt'],
       }

A complete example, including a producer of partitions:

.. literalinclude:: samples/glob_dep.py

Rules:

 * ``glob_dep`` is a list or tuple of non-empty strings.  When omitted it is
   equivalent to an empty list.  Elements follow ``file_dep``'s string rule.
 * Patterns are interpreted relative to the current working directory and use
   the same syntax as Python's :mod:`glob`: ``*``, ``?``, ``[]`` and the
   recursive ``**``.
 * Only **regular files** are matched.  Directory symbolic links are never
   followed.  A matched file that is a symbolic link is an error (some runs
   would otherwise read the link target while others only recorded the
   directory entry).
 * The same path matched by several patterns counts once.  Patterns are
   de-duplicated by their declaration.
 * An empty match is a valid empty set, and the patterns are still stored so a
   file added later is discovered next run.
 * ``file_dep`` may be used together with ``glob_dep``; a missing explicit
   ``file_dep`` still raises the usual error.

How the match set is decided
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The stored state of a task contains three things: the declared patterns, the
sorted list of matched paths, and a content digest (MD5) for each matched file.
A task runs again when any of these changes:

 * the declared patterns changed,
 * a matched file was added, removed or renamed,
 * the content of a matched file changed.

Content is always compared, even when file size and mtime are unchanged; a
change that keeps size and restores mtime is still detected.  Conversely, a
file whose bytes did not change is up-to-date even if only its mtime moved.
Files are read in chunks, so memory does not grow with file size, and each
unique path is hashed at most once per observation even with overlapping
patterns.

.. note::

   Explicit ``file_dep`` entries keep their usual checker (``md5`` or
   ``timestamp``).  The stronger always-compare-content guarantee applies to
   files discovered through ``glob_dep``.

Cost
~~~~

The stored observation is the sorted list of matched paths plus one MD5 per
path (a manifest), and it does not keep file contents.  Resident memory is
therefore proportional to the number of matched paths, never to the total
number of bytes; deterministic chunked hashing keeps a single large file from
being read into memory.  Each unique (normalized) path is hashed at most once
per observation even when several patterns overlap.  Two observations are made
for a task that runs: one immediately before its actions and one immediately
after, to detect inputs that changed mid-run.

Inputs handed to actions
~~~~~~~~~~~~~~~~~~~~~~~~

Python actions receive the actual files in the ``dependencies`` argument, and
``CmdAction`` string formatting can use ``%(dependencies)s``.  The list is the
union of the explicit ``file_dep`` and the current ``glob_dep`` matches,
sorted by normalized path.  It never contains targets that have not been
produced yet, and it does not modify the task's ``file_dep``.

Producers of potential targets
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A ``glob_dep`` pattern also participates in graph construction.  If a declared
``targets`` path of another task would be matched by one of the patterns, that
task becomes an implicit ``task_dep`` even before the target file exists.  The
producer runs first, then the consumer's actual match set is observed.

Paths are compared with the same lexical normalization used for scanning, so
``./x``, ``a/../x`` and absolute spellings all refer to the same file.
Symbolic links are not resolved for this comparison.  Only producers in the
same loaded static task graph are considered; existing cycle detection and
"shared producer runs once" behaviour are preserved.

Failure behaviour
~~~~~~~~~~~~~~~~~

If a file is added, removed, renamed or rewritten while the actions are
running, the run is reported as a ``DependencyError`` even if the actions
returned success, and no success record is saved.  The next legal run
recomputes.  A failed action likewise never advances the success record.
Directory scan or file read errors are reported as errors, never treated as
"nothing changed".

To re-run after changing inputs, simply run ``doit`` again (or use
``doit forget <task>`` / ``doit reset-dep <task>``).  ``doit info <task>``
reports whether the task is up to date and why (pattern change, added/removed
members or content changes).


.. _task-resources:

resources (capacity-aware scheduling)
-------------------------------------

The ``-n`` option only limits *how many* tasks run at the same time; it cannot
tell three light tasks from three heavy ones.  A task can instead declare how
much of each named *resource* it needs, and the run declares each resource's
capacity.  doit then admits tasks only when all of their requirements fit at
once, while still letting smaller, independent tasks move ahead of a blocked
heavy task within a fairness boundary.

Declare requirements on the task dictionary.  Keys are non-empty resource
names; values are non-negative integers (``bool`` is not accepted).  A value
of ``0`` is allowed and occupies nothing.  Omitting ``resources`` is
equivalent to an empty dictionary.

.. code-block:: python

    def task_partition():
        return {
            'actions': ['convert partition/*.raw'],
            'resources': {'memory_slots': 3, 'db_connections': 1},
        }

Declare the capacities in ``DOIT_CONFIG``.  Capacities are positive integers;
an empty dictionary (or omitting the option) means no resource has declared
capacity.

.. code-block:: python

    DOIT_CONFIG = {
        'resource_limits': {
            'memory_slots': 8,
            'db_connections': 4,
        },
        # optional, default 3; non-negative integer
        'resource_bypass_limit': 3,
    }

Rules and errors
~~~~~~~~~~~~~~~~

* A positive requirement for a resource that has no declared
  ``resource_limits`` entry, or a requirement larger than that resource's
  capacity, aborts the run with a clear error.  Errors in the *statically
  loaded* task graph are rejected before any action or setup task runs;
  tasks created later by a delayed loader are checked when they are admitted.
* The caller's dictionaries are validated into copies and never rewritten.
* ``-n`` still independently caps the total number of simultaneously running
  tasks.

When capacity is committed and released
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Capacity is reserved in the parent process at the moment a task is *admitted*
into the execution queue — not when a worker happens to start it — so tasks
already dispatched but not yet started are correctly counted.  A task never
holds part of its requirements while waiting for the rest (all-or-nothing).
The reservation is released when the parent has finished processing the
task's result, whether the outcome is success, an action failure, or a
failure while saving the success record.

Tasks that are up-to-date, ignored, or skipped because a dependency failed
neither consume resources nor count as an admission.  Setup tasks are
independent tasks that use their own requirements.  Teardown actions keep
their existing execution model and do not participate in admission control.

Fairness boundary
~~~~~~~~~~~~~~~~~

Waiters are ordered by when they entered the resource wait, which can only
happen after all their task dependencies are ready; tasks whose dependencies
are not ready never take a queue number early.

If an earlier waiter ``A`` and a later task ``B`` share at least one resource
with a positive requirement, admitting ``B`` first counts as one bypass of
``A``.  Each waiter tolerates at most ``resource_bypass_limit`` such
bypasses (the default is 3); once the budget is spent, later tasks sharing a
resource with it can no longer jump ahead, which guarantees the heavy task a
real chance at the capacity it needs.  One admission can bypass several
waiters and is counted against each of them separately.  A limit of ``0``
forbids any overlapping task from jumping the queue.

Tasks whose requirements are completely disjoint from a waiting task neither
consume its bypass budget nor are blocked by it, so unrelated work keeps
flowing.  Because parked tasks hold no capacity while waiting, a circular
wait (resource deadlock) cannot form.

Serial, ``-n`` process and ``-n`` thread execution all use the same
admission logic.  A failed run leaves nothing reserved: invoking
``DoitMain`` again in the same Python process starts from empty allocation
and fresh waiting counters.

Diagnostics
~~~~~~~~~~~

``doit info <task>`` prints the task's ``resources`` declaration.  For
programmatic inspection, the runner keeps a ``ResourceManager`` on
``runner.resource_manager`` with:

* ``snapshot()`` — current limits, committed allocation, running tasks and
  waiting tasks together with each waiter's last reason
  (``capacity`` or ``fairness: blocked by <task>``) and bypass count;
* ``events`` (when event recording is enabled) — ordered
  ``admit`` / ``wait`` / ``release`` records, including the requirements,
  the resulting allocation and the waiters each admission bypassed;
* the counters ``check_count``, ``admit_count`` and ``wait_count``.

The default reporter output is unchanged when event recording is not
enabled.


targets
-------

Targets can be any file path (a file or folder). If a target does not exist
the task will be executed. There is no limitation on the number of targets
a task may define. Two different tasks can not have the same target. Target
can be specified as a string or as an instance of any Path class from
`pathlib <https://docs.python.org/3/library/pathlib.html>`_.

Lets take the compilation example again.

.. literalinclude:: samples/compile.py

* If there are no changes in the dependency the task execution is skipped.
* But if the target is removed the task is executed again.
* But only if it does not exist. If the target is modified but the dependencies
  do not change the task is not executed again.

.. code-block:: console

    $ doit
    .  compile
    $ doit
    -- compile
    $ rm main.o
    $ doit
    .  compile
    $ echo xxx > main.o
    $ doit
    -- compile



execution order
-----------------

If your tasks interact in a way where the target (output) of one task is a
file_dep (input) of another task, `doit` will make sure your tasks are
executed in the correct order.

.. literalinclude:: samples/taskorder.py

.. code-block:: console

  $ doit
  .  create
  .  modify


.. note::

  For ``file_dep``/``targets`` `doit` compares the path (string) of the file.
  So although `my_file` and `./my_file` are actually the same file, `doit`
  will think they are different files.

  For ``glob_dep`` the comparison is done on lexically normalized absolute
  paths (``abspath`` + ``normpath``, without resolving symbolic links), so
  ``./my_file``, ``a/../my_file`` and an absolute spelling produced by a
  pattern do refer to the same file.


.. _task-selection:

Task selection
----------------


By default all tasks are executed in the same order as they were defined (the order may change to satisfy dependencies). You can control which tasks will run in 2 ways.

Another example

.. literalinclude:: samples/selecttasks.py

DOIT_CONFIG -> default_tasks
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

*dodo* file defines a dictionary ``DOIT_CONFIG`` with ``default_tasks``, a list of strings where each element is a task name.

.. code-block:: console

    $ doit
    .  t1
    .  t3

Note that only the task *t3* was specified to be executed by default.
But its dependencies include a target of another task (t1).
So that task was automatically executed also.


command line selection
^^^^^^^^^^^^^^^^^^^^^^^

From the command line you can control which tasks are going to be execute by passing its task name. Any number of tasks can be passed as positional arguments.

.. code-block:: console

    $ doit t2
    .  t2


You can also specify which task to execute by its target:

.. code-block:: console

    $ doit task1
    .  t1


sub-task selection
^^^^^^^^^^^^^^^^^^^^^

You can select sub-tasks from the command line specifying its full name.

.. literalinclude:: samples/subtasks.py


.. code-block:: console

    $ doit create_file:file2.txt
    .  create_file:file2.txt


wildcard selection
^^^^^^^^^^^^^^^^^^^^

You can also select tasks to be executed using a `glob <http://docs.python.org/library/glob.html>`_ like syntax (it must contains a ``*``).

.. code-block:: console

    $ doit "create_file:file*"
    .  create_file:file1.txt
    .  create_file:file2.txt
    .  create_file:file3.txt



parameters on actions
---------------------

Actions may take optional parameters provided as function keywords arguments for *python-action*. Or values for *printf-style* formatting on *cmd-action*.

There are three sources of parameter values:

 - specified on action's `kwargs` definition
 - keywords with task metadata such as `dependencies`, `changed`, `targets` and `task`
 - values computed in other tasks. :ref:`getargs` describes, how a task can calculate and store values and how other tasks can refer to them.


keywords with task metadata
^^^^^^^^^^^^^^^^^^^^^^^^^^^

These values are automatically calculated by `doit`:

 - `dependencies`: list of `file_dep`
 - `changed`: list of `file_dep` that changed since last
   successful execution
 - `targets`: list of `targets`
 - `task`: only available to *python-action*.
   Note: the value is a `Task` object instance, not the metadata *dict*.


.. _keywords-on-cmd-action-string:

keywords on cmd-action string
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For *cmd-action* you can take advantage of implicit keyword
substitution on *cmd-action* strings using python formatting.

Both format-string-syntax_ and old-string-formatting_ are available and
are controlled by ``DOIT_CONFIG`` value of ``action_string_formatting``.
It can be:

  * ``'old'`` uses old-string-formatting_ (``%``)
  * ``'new'`` uses  format-string-syntax_ (``{{}}``)
  * ``'both'`` both old and new style

The keyword value is a string containing all respective
file names separated by a space (" ").

.. _format-string-syntax: https://docs.python.org/3/library/string.html#format-string-syntax
.. _old-string-formatting: http://docs.python.org/3/library/stdtypes.html#old-string-formatting


.. literalinclude:: samples/report_deps.py

.. warning:: ``action_string_formatting`` default value (as of `doit` version **0.32**) is ``'old'``. This default value might change in future versions, it is advised to always explicitly state a value.

Before the string is actually executed, it is always formatted using
the formatters specified in `DOIT_CONFIG`. Make sure to escape your formatters
control characters, namely `{` and `}` for format-string-syntax_ and `%` for
old-string-formatting_. This is done by doubling them, so `%` becomes `%%` and so on.

.. note:: *cmd-action* may have the form of a string (as `"echo hello world"`)
    or list of arguments (as `["echo", "hello", "world"]`).
    Implicit keywords substitution applies only to the string form
    and does not affect the list form. This means `{}` and `%`
    need not be escaped when using this form.



keywords on *python-action*
^^^^^^^^^^^^^^^^^^^^^^^^^^^

For *python-action* add a keyword parameter in the function,
`doit` will take care of passing the value when the function is called.
`dependencies`, `changed`, `targets` are passed as list of strings.


.. literalinclude:: samples/hello.py


You can also pass the keyword `task` to have a reference to all task
metadata.

.. literalinclude:: samples/meta.py


.. note::

  It is possible not only to retrieve task's attributes but also to
  modify them while the action is running!




private/hidden tasks
---------------------

If task name starts with an underscore '_', it will not be included in the output.


title
-------

By default when you run `doit` only the task name is printed out on the output.
You can customize the output passing a ``title`` function to the task:

.. literalinclude:: samples/title.py

.. code-block:: console

    $ doit
    .  executing... Cmd: echo abc efg



.. _verbosity:


verbosity
-----------

By default the stdout from a task is captured and its stderr is sent to the
console. If the task fails or there is an error the stdout and a traceback
(if any) is displayed.

There are 3 levels of verbosity:

0:
  capture (do not print) stdout/stderr from task.

1 (default):
  capture stdout only.

2:
  do not capture anything (print everything immediately).


You can control the verbosity by:

* task attribute verbosity

.. literalinclude:: samples/verbosity.py

.. code-block:: console

    $ doit
    .  print
    hello

* from command line, see :ref:`verbosity option<verbosity_option>`.


meta
----

You can pass *extra* metadata (dict) to a task through the **meta** attribute.
This could be used by custom commands or plugins.

.. literalinclude:: samples/metadata.py


pathlib
--------

`doit` supports `pathlib <https://docs.python.org/3/library/pathlib.html>`_:
file_dep, targets and CmdAction specified as a list can take as elements not
only strings but also instances of any Path class from pathlib.

Lets take the compilation example and modify it to work with any number
of header and source files in current directory using pathlib.

.. literalinclude:: samples/compile_pathlib.py
