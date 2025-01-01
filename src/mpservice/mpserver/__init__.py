"""
``mpservice.mpserver`` provides classes that use `multiprocessing`_ to perform CPU-bound operations
taking advantage of all the CPUs (i.e. cores) on the machine.
Using `threading`_ to perform IO-bound operations is equally supported, although it was not the initial focus.

There are three levels of constructs.

1. On the lowest level is :class:`Worker`. This defines operations on a single input item
   or a batch of items in usual sync code. This is supposed to run in its own process (or thread)
   and use that single process (or thread) only. In other words, to keep things simple, the user-defined
   behavior of :class:`Worker` should not launch processes or threads.

2. On the middle level is :class:`Servlet`. A basic form of Servlet arranges to execute a :class:`Worker` in one or more
   processes (or threads). More advanced forms of Servlet arrange to executive multiple
   Servlets as a sequence or an ensemble, or select a Servlet (from a set of Servlets) to process
   a particular input element based on certain conditions.

3. On the top level is :class:`Server` (or :class:`AsyncServer`). A Server
   handles interfacing with the outside world, while passing the "real work" to
   a :class:`Servlet` and relays the latter's result back to the outside world.
"""

__all__ = [
    'Worker',
    'make_worker',
    'PassThrough',
    'Servlet',
    'ProcessServlet',
    'ThreadServlet',
    'SequentialServlet',
    'EnsembleServlet',
    'SwitchServlet',
    'Server',
    'AsyncServer',
    'EnsembleError',
    'ServerBacklogFull',
    'TimeoutError',
]

import logging

# This modules uses the 'spawn' method to create processes.
# Note on the use of RemoteException:
# The motivation of RemoteException is to wrap an Exception object to go through
# pickling (process queue) and pass traceback info (as a str) along with it.
# The unpickling side will get an object of the original Exception class rather
# than a RemoteException object.
# As a result, objects taken off of a process queue will never be RemoteException objects.
# However, if the queue is a thread queue, then a RemoteException object put in it
# will come out as a RemoteException unchanged.
# Set level for logs produced by the standard `multiprocessing` module.
import multiprocessing

from ._server import AsyncServer, Server, ServerBacklogFull, TimeoutError
from ._servlet import (
    EnsembleError,
    EnsembleServlet,
    ProcessServlet,
    SequentialServlet,
    Servlet,
    SwitchServlet,
    ThreadServlet,
)
from ._worker import PassThrough, Worker, make_worker

multiprocessing.log_to_stderr(logging.WARNING)
