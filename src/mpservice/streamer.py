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
]
