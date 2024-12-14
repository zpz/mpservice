====================================
Stream processing using ``streamer``
====================================

.. testsetup::

   from mpservice.streamer import Stream


.. automodule:: mpservice.streamer
    :no-members:
    :no-undoc-members:
    :no-special-members:


API reference
=============


.. autoclass:: mpservice.streamer.Stream

.. autoclass:: mpservice.streamer.Batcher

.. autoclass:: mpservice.streamer.Unbatcher

.. autoclass:: mpservice.streamer.Parmapper

.. autoclass:: mpservice.streamer.EagerBatcher

.. autoclass:: mpservice.streamer.RateLimiter

.. autoclass:: mpservice.streamer.AsyncRateLimiter

.. autoclass:: mpservice.streamer.IterableQueue

.. autoclass:: mpservice.streamer.ProcessRunner

.. autoclass:: mpservice.streamer.ProcessRunnee

.. autofunction:: mpservice.streamer.fifo_stream

.. autofunction:: mpservice.streamer.async_fifo_stream

.. autofunction:: mpservice.streamer.tee



.. Reference (for an early version of the code): https://zpz.github.io/blog/stream-processing/