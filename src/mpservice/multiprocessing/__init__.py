"""
`multiprocessing`_ can be tricky.
``mpservice`` provides help to address several common difficulties.

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

Last but not least, if exception happens in a child process and we don't want the program to crash right there,
instead we send it to the main or another process to be investigated when/where we are ready to,
the traceback info will be lost in pickling. :class:`~mpservice.multiprocessing.RemoteException` helps on this.
"""

from ._multiprocessing import (
    MP_SPAWN_CTX,
    CpuAffinity,
    SpawnContext,
    SpawnProcess,
    TimeoutError,
)
from ._remote_exception import (
    RemoteException,
    RemoteTraceback,
    get_remote_traceback,
    is_remote_exception,
)
from ._server_process import (
    ServerProcess,
    hosted,
)

__all__ = [
    'RemoteException',
    'RemoteTraceback',
    'get_remote_traceback',
    'is_remote_exception',
    'TimeoutError',
    'SpawnProcess',
    'SpawnContext',
    'MP_SPAWN_CTX',
    'ServerProcess',
    'CpuAffinity',
    'hosted',
]


_names_ = [
    x for x in dir(MP_SPAWN_CTX) if not x.startswith('_') and x != 'TimeoutError'
]
globals().update((name, getattr(MP_SPAWN_CTX, name)) for name in _names_)
# Names like `Process`, `Queue`, `Pool`, `Event`, `Manager` etc are directly import-able from this module.
# But they are not classes; rather they are bound methods of the context `MP_SPAWN_CTX`.
# This is the same behavior as the standard `multiprocessing`.
# With this, you can usually replace
#
#    from multiprocessing import ...
#
# by
#
#    from mpservice.multiprocessing import ...
