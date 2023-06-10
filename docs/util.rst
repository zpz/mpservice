=========
Utilities
=========

Multiprocessing
===============

.. automodule:: mpservice.multiprocessing
    :no-members:
    :no-special-members:
    :no-undoc-members:

.. autoclass:: mpservice.multiprocessing.RemoteException

.. autoexception:: mpservice.multiprocessing.RemoteTraceback

.. autofunction:: mpservice.multiprocessing.is_remote_exception

.. autofunction:: mpservice.multiprocessing.get_remote_traceback

.. autoclass:: mpservice.multiprocessing.SpawnProcess

.. autoclass:: mpservice.multiprocessing.SpawnContext

.. autodata:: mpservice.multiprocessing.MP_SPAWN_CTX

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


Threading
=========

.. autoclass:: mpservice.threading.Thread


Concurrent Futures
==================

.. autoclass:: mpservice.concurrent.futures.ProcessPoolExecutor

.. autoclass:: mpservice.concurrent.futures.ThreadPoolExecutor

.. autofunction:: mpservice.concurrent.futures.get_shared_thread_pool

.. autofunction:: mpservice.concurrent.futures.get_shared_process_pool


