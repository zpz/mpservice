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


Introduction
============

Let's make up an I/O-bound operation which takes an input and produces an output.
(Think calling a remote service with an input and wait to get a result.)

>>> from time import sleep
>>> from random import random
>>> def double(x):
...     sleep(random() * 0.1)
...     return x * 2

Suppose we have a long stream of input values we want to process.
We feed this stream into a :class:`Stream` object:

>>> from mpservice.streamer import Stream
>>> data_stream = Stream(range(100))

The input stream is often a list, but more generally, it can be any
`Iterable`_, possibly unlimited.
Since ``double`` is an I/O-bound operation, let's use multiple threads to speed up
the processing of the input stream.
For this purpose, we add a :meth:`~Stream.parmap` (or "parallel map") operator to the stream:

>>> data_stream.parmap(double, executor='thread', num_workers=8)  # doctest: +ELLIPSIS
<mpservice._streamer.Stream object at 0x7...>

This requests the function ``double`` to be run in 8 threads;
they will collectively process the input stream.
Adding the operator is just "setup"--nothing runs until we start to retrieve results.
Later we'll see that we can add more than one operator, and there are other types of operators.
Because a :class:`Stream` is an `Iterator`_,
"retrieving the results" usually amounts to iterating over it:

>>> total = 0
>>> for y in data_stream:
...     total += y
>>> total
9900

What is the expected result?

>>> sum((v*2 for v in range(100)))
9900

Despite the concurrency in the operation, the order of the input elements is preserved.
In other words, the output elements correspond to the input elements in order.
Let's verify:

>>> data_stream = Stream(range(100)).parmap(double, executor='thread', num_workers=8)
>>> for k, y in enumerate(data_stream):  # doctest: +SKIP
...     print(y, end='  ')  # doctest: +SKIP
...     if (k + 1) % 10 == 0:  # doctest: +SKIP
...         print('')  # doctest: +SKIP
... print('')  # doctest: +SKIP
0  2  4  6  8  10  12  14  16  18
20  22  24  26  28  30  32  34  36  38
40  42  44  46  48  50  52  54  56  58
60  62  64  66  68  70  72  74  76  78
80  82  84  86  88  90  92  94  96  98
100  102  104  106  108  110  112  114  116  118
120  122  124  126  128  130  132  134  136  138
140  142  144  146  148  150  152  154  156  158
160  162  164  166  168  170  172  174  176  178
180  182  184  186  188  190  192  194  196  198


Note that we had to re-create the streamer object because,
after the first iteration, the stream was "consumed" and gone.
Also note that we can either add an operator in a statement, or call it as a function, often in a "chained" fashion.
(An operator method modifies ``self``, but also returns ``self`` in the end, hence facilitating chained calls.)

Suppose we want to follow the heavy ``double`` operation by a shift to each element:

>>> def shift(x, amount):
...     return x + amount

This is quick and easy; we decide do it "in-line" by :meth:`~Stream.map`:

>>> data_stream = Stream(range(20))
>>> data_stream.parmap(double, executor='thread', num_workers=8)  # doctest: +ELLIPSIS
<mpservice._streamer.Stream object at 0x7...>
>>> data_stream.map(shift, amount=0.8)  # doctest: +SKIP
<mpservice._streamer.Stream object at 0x7...>
>>> for k, y in enumerate(data_stream):  # doctest: +SKIP
...     print(y, end='  ')  # doctest: +SKIP
...     if (k + 1) % 10 == 0:  # doctest: +SKIP
...         print('')  # doctest: +SKIP
... print('')  # doctest: +SKIP
0.8  2.8  4.8  6.8  8.8  10.8  12.8  14.8  16.8  18.8
20.8  22.8  24.8  26.8  28.8  30.8  32.8  34.8  36.8  38.8

The first three lines are equivalent to this one line:

>>> data_stream = Stream(range(20)).parmap(double, executor='thread', num_workers=8).map(shift, amount=0.8)

Operators
=========

:class:`Stream` has many "operators". They can be characterised in a few ways:

One-to-one (will not change the elements' count or order):
    - :meth:`~Stream.map`
    - :meth:`~Stream.accumulate`
    - :meth:`~Stream.peek`
    - :meth:`~Stream.parmap`
    - :meth:`~Stream.buffer`

One-to-one (will change the elements' order, but not count):
    - :meth:`~Stream.shuffle`

Many-to-one (may shrink the stream):
    - :meth:`~Stream.groupby`
    - :meth:`~Stream.batch`

One-to-many (may expand the stream):
    - :meth:`~Stream.unbatch`

Selection or filtering (may drop elements):
    - :meth:`~Stream.filter`
    - :meth:`~Stream.filter_exceptions`
    - :meth:`~Stream.head`
    - :meth:`~Stream.tail`

Concurrent (using threads, processes, or asyncio):
    - :meth:`~Stream.parmap`

Read-only (will not change the elements):
    - :meth:`~Stream.buffer` (speed stabilizing)
    - :meth:`~Stream.peek` (info printing)

All these methods preserve the order of the elements, with the only exception
:meth:`~Stream.shuffle`.

The operation in ``parmap`` is supposedly heavy and expensive.
All the other operations are meant to be ligthweight and simple.

These methods can be called either as a single statement, or in a "chained" fashion.
They "set up", or "add", operators to the streamer.
However, they do not *start* the operators.

The operators that have been added to a streamer will start once we start to "consume" the stream,
that is, to retrieve elements of the stream.
Compared to thinking "the operators start", it's more intuitive to think
"the elements start to flow" through the operators in the order they have been added.
The "consuming" methods are "pulling" at the end of the final operator.

There are several ways to consume the stream:

- Iterate over the :class:`Stream` object, because it implements :meth:`~Stream.__iter__` or :meth:`~Stream.__aiter__`.
- Call the method :meth:`~Stream.collect` to get all the elements in a list---if you know there are not too many of them!
- Call the method :meth:`~Stream.drain` to "finish off" the operations. This does not return the elements of the stream, but rather
  just the count of them. This is used when the final operator exists mainly for a side effect, such as saving things to a database.

The latter two methods are trivial wrappers of the first.

Two particular operators, namely :class:`Batcher` and :class:`Unbatcher` (the workhorses behind the methods
:meth:`Stream.batch` and :meth:`Stream.unbatch`), are exposed on the module level so that they can be used standalone.

Sync vs async
=============

Both sync and async programming modes are supported. For the most part,
the usage of Stream is one and same in both modes.
This host or calling-site "mode" refers to the "sync-ness" or "async-ness" of the function in which
the Stream object is being used.
Related to the host mode, a Stream object may be "sync" or "async".
The former means the object is "iterable", i.e. has method :meth:`~Stream.__iter__`, hence is to be consumed by::

    for x in stream:
        ...

whereas the latter means the object is "async iterable", i.e. has method :meth:`~Stream.__aiter__`,
hence is to be consumed by::

    async for x in stream:
        ...

While you may use a sync Stream in an async function, you probably won't use an async Stream
in a sync function.

A streamlet is either sync or async. The sync- or async-ness of Stream is simply that of its final streamlet.
When an operator is called, it adds a sync streamlet or async streamlet based on the sync-ness of the Stream
(or equivalently, of the previous streamlet). For example,

::

    stream.map(...)

will add a :class:`~mpservice._streamer.Mapper` streamlet if ``stream`` (at this point) is sync,
or a :class:`~mpservice._streamer.AsyncMapper` otherwise.

There is a special case, though.
The first streamlet is the input data, which, beding defined externally, could be iterable, async iterable, or even both.
(This is the only streamlet that can be both sync and async.)
If it is both, is the Stram object (when it has only one streamlet) sync or async?
Well, if you don't add any operator, you can use this stream in either sync or async iterations.
If you add another operator, however, the stream is considered "sync", hence a sync streamlet is added.
This decision is easy to see in the method :meth:`Stream._choose_by_mode`.

To disambiguate, or to switch, the methods :meth:`~Stream.to_sync` and :meth:`~Stream.to_async` can be used.

The usage of the operators is the same in sync and async modes, with two exceptions:
:meth:`~Stream.drain` and :meth:`~Stream.collect`.
In sync mode, both do the usualy thing to return a result.
In async mode, both return a coroutine, which is to be "awaited".
For example,

::

    def main():
        data = range(1000)
        stream = Stream(data).map(...).batch(...).filter(...).peek(...).parmap(...)
        n = stream.drain()

    async def main():
        data = range(1000)
        stream = Stream(data).to_async().map(...).batch(...).filter(...).peek(...).parmap(...)
        n = await stream.drain()


Additional utilities separate from the class `Stream`
=====================================================

There is a module function :func:`tee`, which is analogous to the standard
`itertools.tee <https://docs.python.org/3/library/itertools.html#itertools.tee>`_.

The class :class:`EagerBatcher` is analogous to :class:`Batcher` but has a timeout,
which controls how long to wait before yielding an under-sized batch.
"""

from ._streamer import (
    Batcher,
    EagerBatcher,
    IterableQueue,
    Parmapper,
    StopRequested,
    Stream,
    Unbatcher,
    isasynciterable,
    isiterable,
    tee,
)

__all__ = [
    'Batcher',
    'EagerBatcher',
    'IterableQueue',
    'Parmapper',
    'StopRequested',
    'Stream',
    'Unbatcher',
    'isasynciterable',
    'isiterable',
    'tee',
]
