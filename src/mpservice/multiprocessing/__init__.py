"""
The module ``mpservice.multiprocessing`` provides some customizations and enhancements to the standard module `multiprocessing`_.
Most of the customizations are drop-in replacements.

First, the standard package ``multiprocessing`` has a
`"context" <https://docs.python.org/3/library/multiprocessing.html#contexts-and-start-methods>`_,
which has to do with how a process is created and started.
Multiprocessing objects like ``Queue``, ``Event``, etc., must be created from a context that matches
the process in order to be used with the process.
The default context on **Linux** is a "fork" one. However, it's recommended to use a "spawn" context.
The :class:`mpservice.multiprocessing.SpawnContext` customizes the standard counterpart.

Second, in well structured code, a **spawned** process will not get the logging configurations that have been set
in the main process. On the other hand, we should definitely not separately configure logging in
child processes. The class :class:`mpservice.multiprocessing.SpawnProcess` addresses this issue by
sending log messages if child processes to the main process for handling, all transparently.

Third, one convenience of `concurrent.futures`_ compared to `multiprocessing`_ is that the former
makes it easy to get the results or exceptions of the child process via the object returned from job submission.
With `multiprocessing`_, in contrast, we have to pass the results or explicitly captured exceptions
to the main process via a queue. The custom :class:`~mpservice.multiprocessing.SpawnProcess` has this covered
as well--it can be used in the ``concurrent.futures`` way.

Fourth, if an Exception object is pickled, its traceback info is lost.
A consequence of this is that
if exception happens in a child process and we don't want the program to crash right there,
instead we send it to the main process to be investigated or raised when/where we are ready to,
we won't have the traceback info. For example, the printout of ``raise ..`` in another process
will not be very informative.
The module :mod:`mpservice.multiprocessing.remote_exception` helps on this.

Besides these fixes to "pain points", the module :mod:`mpservice.multiprocessing.server_process` provides some new capabilities
to the "manager" facility in the standard ``multiprocessing``, especially about "shared memory".
"""

# The directory structure in this subpackage mirrors that of the standard `multiprocessing`.
# There are a few modules that do not exist in the standard `multiprocessing`.
# For all symbols that are importable from here, it's recommended to import them from `mpservice.multiprocessing`
# instead of from the individual modules (e.g., instead of from `mpservice.multiprocessing.synchronize`).
# For content in the few modules that do not have counterparts in the standard `multiprocessing`,
# e.g. `remote_exception` and `server_process`,
# you need to import from the individual modules.

import concurrent.futures
from collections.abc import Iterator, Sequence
from concurrent.futures import ALL_COMPLETED, FIRST_COMPLETED, FIRST_EXCEPTION

from mpservice.threading import Thread

from .context import (
    MP_SPAWN_CTX,
    SpawnContext,
    SpawnProcess,
)
from .managers import SyncManager
from .pool import Pool
from .queues import JoinableQueue, Queue, SimpleQueue
from .synchronize import (
    Barrier,
    BoundedSemaphore,
    Condition,
    Event,
    Lock,
    RLock,
    Semaphore,
)

Process = SpawnProcess
# ``SpawnProcess`` can be imported and used, but ``Process`` is preferred.

Manager = SyncManager

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
    'SyncManager',
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
    """
    ``workers`` is a sequence of ``Thread`` or ``SpawnProcess`` that have been started.
    It can be a mix of the two types.

    See ``concurrent.futures.wait``.
    """

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
    """See ``concurrent.futures.as_completed``."""

    futures = [t._future_ for t in workers]
    future_to_thread = {id(t._future_): t for t in workers}
    for f in concurrent.futures.as_completed(futures, timeout=timeout):
        yield future_to_thread[id(f)]
