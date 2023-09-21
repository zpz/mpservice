=============================
Utilities for multiprocessing
=============================

.. automodule:: mpservice.multiprocessing
    :no-members:
    :no-special-members:
    :no-undoc-members:


Remote exception
================

.. automodule:: mpservice.multiprocessing.remote_exception
    :no-members:
    :no-special-members:


.. autoclass:: mpservice.multiprocessing.remote_exception.RemoteException

.. autoexception:: mpservice.multiprocessing.remote_exception.RemoteTraceback

.. autofunction:: mpservice.multiprocessing.remote_exception.is_remote_exception

.. autofunction:: mpservice.multiprocessing.remote_exception.get_remote_traceback


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
