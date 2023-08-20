=============================
Utilities for multiprocessing
=============================

.. automodule:: mpservice.multiprocessing
    :no-members:
    :no-special-members:
    :no-undoc-members:

Remote exception
================

.. autoclass:: mpservice.multiprocessing.RemoteException

.. autoexception:: mpservice.multiprocessing.RemoteTraceback

.. autofunction:: mpservice.multiprocessing.is_remote_exception

.. autofunction:: mpservice.multiprocessing.get_remote_traceback


The "spawn" context
===================

.. autoclass:: mpservice.multiprocessing.SpawnProcess

.. autoclass:: mpservice.multiprocessing.SpawnContext

.. autodata:: mpservice.multiprocessing.MP_SPAWN_CTX


Waiting for processes to finish
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: mpservice.multiprocessing.wait

.. autofunction:: mpservice.multiprocessing.as_completed


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
