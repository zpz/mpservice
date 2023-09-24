=============================
Utilities for multiprocessing
=============================

.. automodule:: mpservice.multiprocessing
    :no-members:
    :no-special-members:
    :no-undoc-members:


The "spawn" process and context
===============================

.. autoclass:: mpservice.multiprocessing.SpawnProcess

.. autoclass:: mpservice.multiprocessing.SpawnContext


Standard classes with customizations
====================================

The following classes are subclasses of their counterparts in the standard ``multiprocessing``.
The typical customization is that by default they use :class:`SpawnContext` as the "context".
In addition, the queue types (:class:`~mpservice.multiprocessing.Queue`,
:class:`~mpservice.multiprocessing.SimpleQueue`, :class:`~mpservice.multiprocessing.JoinableQueue`)
are made `Generic <https://docs.python.org/3/library/typing.html#typing.Generic>`_ to enable annotating
the type of the elements contained in them.

.. autoclass:: mpservice.multiprocessing.Barrier

.. autoclass:: mpservice.multiprocessing.Condition

.. autoclass:: mpservice.multiprocessing.Event

.. autoclass:: mpservice.multiprocessing.Lock

.. autoclass:: mpservice.multiprocessing.RLock

.. autoclass:: mpservice.multiprocessing.Semaphore

.. autoclass:: mpservice.multiprocessing.BoundedSemaphore

.. autoclass:: mpservice.multiprocessing.Queue

.. autoclass:: mpservice.multiprocessing.SimpleQueue

.. autoclass:: mpservice.multiprocessing.JoinableQueue

.. autoclass:: mpservice.multiprocessing.Pool

.. autoclass:: mpservice.multiprocessing.Manager


Waiting for processes to finish
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: mpservice.multiprocessing.wait

.. autofunction:: mpservice.multiprocessing.as_completed


Remote exception
================

.. automodule:: mpservice.multiprocessing.remote_exception
    :no-members:
    :no-special-members:

.. autoclass:: mpservice.multiprocessing.remote_exception.RemoteException

.. autoexception:: mpservice.multiprocessing.remote_exception.RemoteTraceback

.. autofunction:: mpservice.multiprocessing.remote_exception.is_remote_exception

.. autofunction:: mpservice.multiprocessing.remote_exception.get_remote_traceback


Server Process
==============

.. automodule:: mpservice.multiprocessing.server_process
    :no-members:
    :no-special-members:
    :no-undoc-members:

.. autoclass:: mpservice.multiprocessing.server_process.ServerProcess

.. autoclass:: mpservice.multiprocessing.server_process.MemoryBlock

.. autoclass:: mpservice.multiprocessing.server_process.MemoryBlockProxy

.. autoclass:: mpservice.multiprocessing.server_process.hosted
