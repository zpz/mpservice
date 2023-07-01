"""
The module ``mpservice.streamer`` provides utilities for stream processing with threading, multiprocessing, or asyncio concurrencies.

An input data stream goes through a series of operations.
The output from one operation becomes the input to the next operation.
One or more "primary" operations are so heavy
that they can benefit from concurrency via threading or asyncio
(if they are I/O bound) or multiprocessing (if they are CPU bound).
The other operations are typically light weight, although important in their own right.
These operations perform batching, unbatching, buffering, mapping, filtering, grouping, etc.

To fix terminology, we'll call the main methods of the class ``Stream`` "operators" or "operations".
Each operator adds a "streamlet". The behavior of a Stream object is embodied by its chain of
streamlets, which is accessible via the public attribute ``Stream.streamlets``
(although there is little need to access it).
"Consumption" of the stream entails "pulling" at the end of the last streamlet and,
in a chain reaction, consequently pulls each data element through the entire series
of streamlets or operators.

Both sync and async programming modes are supported. For the most part,
the usage of Stream is one and the same in both modes.
"""
import queue
from collections.abc import Iterable
from time import perf_counter

from mpservice.multiprocessing.queues import IterableQueue as IterableMpQueue
from mpservice.queue import Finished, IterableQueue

from ._streamer import (
    Batcher,
    Buffer,
    Stream,
    Unbatcher,
    tee,
)

__all__ = [
    'Stream',
    'tee',
    'Batcher',
    'Unbatcher',
    'Buffer',
    'EagerBatcher',
]


class EagerBatcher(Iterable):
    '''
    ``EagerBatcher`` collects items from the incoming stream towards a target batch size and yields the batches.
    For each batch, after getting the first item, it will yield the batch either it has collected enough items
    or has reached ``timeout``. Note, the timer starts upon getting the first item, whereas getting the first item
    for a new batch may take however long.
    '''

    def __init__(
        self,
        instream: IterableQueue | IterableMpQueue,
        /,
        batch_size: int,
        timeout: float = None,
    ):
        # ``timeout`` can be 0.
        self._instream = instream
        self._batch_size = batch_size
        if timeout is None:
            timeout = 3600 * 24  # effectively unlimited wait
        self._timeout = timeout

    def __iter__(self):
        q_in = self._instream
        batchsize = self._batch_size
        timeout = self._timeout

        while True:
            try:
                z = q_in.get()  # wait as long as it takes to get one item.
            except Finished:
                return

            batch = [z]
            n = 1
            deadline = perf_counter() + timeout
            # Timeout starts after the first item is obtained.

            while n < batchsize:
                t = deadline - perf_counter()
                try:
                    # If `t <= 0`, still get the next item
                    # if it's already available.
                    # In other words, if data elements are already here,
                    # get more towards the target batch-size
                    # even if it's already past the timeout deadline.
                    z = q_in.get(timeout=max(0, t))
                except (queue.Empty, Finished):
                    break
                else:
                    batch.append(z)
                    n += 1

            yield batch
