"""
The standard module ``multiprocessing`` is complicated by the so-called "start method" or "context" (ctx).
The commonly used classes like ``Queue``, ``Lock``, ``Event``, etc. have a **required** parameter ``ctx``, 
yet we usually do not use these classes (residing in submodules of ``multiprocessing``) directly.
Instead, we import them from ``multiprocessing``, which has plugged in a "default context" for us.
This is not all good. One problem is the following:

    If we import ``Process`` (or ``Queue``, or many others) from ``multiprocessing``,
    this is **not** a class, but rather a factory method. As a result, although we use
    ``Process(...)`` to create a process instance, we can **not** use ``Process`` to annotate the type
    of this object.

This is both inconvenient and confusing.

The module ``mpservice.multiprocessing`` breaks from some of the ``multiprocessing`` design to alleviate this problem.
Most of the more useful symbols that you can import from ``multiprocessing`` can be imported from 
``mpservice.multiprocessing``, such as ``Queue``, ``Lock``, ``Condition``, ``Semaphore``, etc.
However, unlike from ``multiprocessing`` where these are *functions*,
they classes, hence can be used in type annotations.
In the meatime, their parameter ``ctx`` is **optional** (as opposed to **required**), and defaults to
a spawn context--``MP_SPAWN_CTX`` to be specific. As a result, user is encouraged to use these classes directly
and leave out the ``ctx`` argument.

In addition, ``mpservice.multiprocessing`` provides some enhancements to the standard ``multiprocessing``.

First, it is a good idea to always use the non-default (on Linux) "spawn" method to start a process.
:data:`~mpservice.multiprocessing.MP_SPAWN_CTX` is provided to make this easier.

Second, in well structured code, a **spawned** process will not get the logging configurations that have been set
in the main process. On the other hand, we should definitely not separately configure logging in
non-main processes. The class :class:`~mpservice.multiprocessing.SpawnProcess` addresses this issue. In fact,
``MP_SPAWN_CTX.Process`` is a reference to ``SpawnProcess``. Therefore, when you use ``MP_SPAWN_CTX``,
logging in the non-main processes are covered---log messages are sent to the main process to be handled,
all transparently.

Third, one convenience of `concurrent.futures`_ compared to `multiprocessing`_ is that the former
makes it easy to get the results or exceptions of the child process via the object returned from job submission.
With `multiprocessing`_, in contrast, we have to pass the results or explicitly captured exceptions
to the main process via a queue. :class:`~mpservice.multiprocessing.SpawnProcess` has this covered as well.
It can be used in the ``concurrent.futures`` way.

Third, if exception happens in a child process and we don't want the program to crash right there,
instead we send it to the main or another process to be investigated when/where we are ready to,
the traceback info will be lost in pickling. :class:`~mpservice.multiprocessing.RemoteException` helps on this.

Besides these fixes to "pain points", the module ``mpservice.server_process`` provide some new capabilities
for the "manager" facility in ``multiprocessing``.

.. note:: Recommendations on the use of ``MP_SPAWN_CTX``: use the classes ``Process``, ``Manager``, ``Lock``,
  ``RLock``, ``Condition``, ``Semaphore``, ``BoundedSemaphore``, ``Event``, ``Barrier``,
  ``Queue``, ``JoinableQueue``, ``SimpleQueue``, ``Pool`` diretly to create objects and type-annote them;
  this is preferred over ``MP_SPAWN_CTX.Process``, ``MP_SPAWN_CTX.Manager``, etc, although they would work, too.
  A few other methods of ``SpawnContext`` are not exposed in ``mpservice.multiprocessing``;
  you may use them via the object ``MP_SPAWN_CTX``.
"""
import concurrent.futures
import warnings
from collections.abc import Iterator, Sequence
from concurrent.futures import ALL_COMPLETED, FIRST_COMPLETED, FIRST_EXCEPTION
from importlib import import_module

from mpservice.threading import Thread

from ._context import (
    MP_SPAWN_CTX,
    Barrier,
    BoundedSemaphore,
    Condition,
    Event,
    JoinableQueue,
    Lock,
    Pool,
    Queue,
    RLock,
    Semaphore,
    SimpleQueue,
    SpawnContext,
    SpawnProcess,
)
from ._context import (
    SyncManager as Manager,
)

Process = SpawnProcess
# ``SpawnProcess`` can be imported and used, but ``Process`` is preferred.

RawValue = MP_SPAWN_CTX.RawValue
RawArray = MP_SPAWN_CTX.RawArray
Value = MP_SPAWN_CTX.Value
Array = MP_SPAWN_CTX.Array
# These are functions, not classes!

cpu_count = MP_SPAWN_CTX.cpu_count

__all__ = [
    'SpawnContext',
    'MP_SPAWN_CTX',
    'Process',
    'Manager',
    'Lock',
    'RLock',
    'Condition',
    'Semaphore',
    'BoundedSemaphore',
    'Event',
    'Barrier',
    'Queue',
    'JoinableQueue',
    'SimpleQueue',
    'Pool',
    'RawValue',
    'RawArray',
    'Value',
    'Array',
    'cpu_count',
    'wait',
    'as_completed',
    'ALL_COMPLETED',
    'FIRST_COMPLETED',
    'FIRST_EXCEPTION',
]


def wait(
    workers: Sequence[Thread | SpawnProcess], /, timeout=None, return_when=ALL_COMPLETED
) -> tuple[set[Thread | SpawnProcess], set[Thread | SpawnProcess]]:
    '''
    ``workers`` is a sequence of ``Thread`` or ``SpawnProcess`` that have been started.
    It can be a mix of the two types.

    See ``concurrent.futures.wait``.
    '''

    futures = [t._future_ for t in workers]
    future_to_thread = {id(t._future_): t for t in workers}
    done, not_done = concurrent.futures.wait(
        futures,
        timeout=timeout,
        return_when=return_when.upper(),
    )
    if done:
        done = set(future_to_thread[id(f)] for f in done)
    if not_done:
        not_done = set(future_to_thread[id(f)] for f in not_done)
    return done, not_done


def as_completed(
    workers: Sequence[Thread | SpawnProcess], /, timeout=None
) -> Iterator[Thread | SpawnProcess]:
    '''See ``concurrent.futures.as_completed``.'''

    futures = [t._future_ for t in workers]
    future_to_thread = {id(t._future_): t for t in workers}
    for f in concurrent.futures.as_completed(futures, timeout=timeout):
        yield future_to_thread[id(f)]


def __getattr__(name):
    if name in ('RemoteException', 'get_remote_traceback', 'is_remote_exception'):
        mname = 'mpservice.multiprocessing.remote_exception'
    elif name in ('ServerProcess',):
        mname = 'mpservice.multiprocessing.server_process'
    else:
        raise AttributeError(name)

    m = import_module(mname)
    o = getattr(m, name)
    warnings.warn(
        f"'mpservice.multiprocessing.{name}' is deprecated in 0.14.3 and may be removed in the future. Please import from '{mname}' instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    return o
