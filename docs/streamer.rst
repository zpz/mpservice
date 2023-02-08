====================================
Stream processing using ``streamer``
====================================

.. automodule:: mpservice.streamer
    :no-members:
    :no-undoc-members:
    :no-special-members:


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
We feed this stream into a :class:`Streamer` object:

>>> from mpservice.streamer import Streamer
>>> data_stream = Streamer(range(100))

The input stream is often a list, but more generally, it can be any
`Iterable`_, possibly unlimited.
Since ``double`` is an I/O-bound operation, let's use multiple threads to speed up
the processing of the input stream.
For this purpose, we add a :meth:`~Streamer.parmap` (or "parallel map") operator to the stream:

>>> data_stream.parmap(double, executor='thread', num_workers=8)  # doctest: +ELLIPSIS
<mpservice.streamer.Streamer object at 0x7...>

This requests the function ``double`` to be run in 8 threads;
they will collectively process the input stream.
Adding the operator is just "setup"--nothing runs until we start to retrieve results.
Later we'll see that we can add more than one operator, and there are other types of operators.
Because a :class:`Streamer` is an `Iterator`_,
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

>>> data_stream = Streamer(range(100)).parmap(double, executor='thread', num_workers=8)
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

This is quick and easy; we decide do it "in-line" by :meth:`~Streamer.map`:

>>> data_stream = Streamer(range(20))
>>> data_stream.parmap(double, executor='thread', num_workers=8)  # doctest: +ELLIPSIS
<mpservice.streamer.Streamer object at 0x7...>
>>> data_stream.map(shift, amount=0.8)  # doctest: +ELLIPSIS
<mpservice.streamer.Streamer object at 0x7...>
>>> for k, y in enumerate(data_stream):  # doctest: +SKIP
...     print(y, end='  ')  # doctest: +SKIP
...     if (k + 1) % 10 == 0:  # doctest: +SKIP
...         print('')  # doctest: +SKIP
... print('')  # doctest: +SKIP
0.8  2.8  4.8  6.8  8.8  10.8  12.8  14.8  16.8  18.8
20.8  22.8  24.8  26.8  28.8  30.8  32.8  34.8  36.8  38.8

The first three lines are equivalent to this one line:

>>> data_stream = Streamer(range(20)).parmap(double, executor='thread', num_workers=8).map(shift, amount=0.8)

:class:`Streamer` has many other "operators". They can be characterised in a few ways:

One-to-one (will not change the elements' count or order):
    - :meth:`~Streamer.map`
    - :meth:`~Streamer.accumulate`
    - :meth:`~Streamer.peek`
    - :meth:`~Streamer.parmap`
    - :meth:`~Streamer.buffer`

Many-to-one (may shrink the stream):
    - :meth:`~Streamer.groupby`
    - :meth:`~Streamer.batch`

One-to-many (may expand the stream):
    - :meth:`~Streamer.unbatch`

Selection or filtering (may drop elements):
    - :meth:`~Streamer.filter`
    - :meth:`~Streamer.filter_exceptions`
    - :meth:`~Streamer.head`
    - :meth:`~Streamer.tail`

Concurrent (will create other threads or processes):
    - :meth:`~Streamer.parmap`
    - :meth:`~Streamer.buffer`

Read-only (will not change the elements):
    - :meth:`~Streamer.buffer`
    - :meth:`~Streamer.peek`

These methods can be called either as a statement, or as a function, often in a "chained" fashion.
They "set up", or "add", operators to the streamer.
However, they do not *start* the operators.

The operators that have been added to a streamer will start once we start to "consume" the stream,
that is, to retrieve elements of the stream.
Compared to thinking "the operators start", it's more intuitive to think
"the elements start to flow" through the operators in the order they have been added.
The "consuming" methods are "pulling" at the end of the final operator.

There are several ways to consume the stream:

- Iterate over the :class:`Streamer` object, because it implements :meth:`~Streamer.__iter__` and :meth:`~Streamer.__next__`.
- Call the method :meth:`~Streamer.collect` to get all the elements in a list---if you know there are not too many of them!
- Call the method :meth:`~Streamer.drain` to "finish off" the operations. This does not return the elements of the stream, but rather
  just the count of them. This is used when the final operator exists mainly for a side effect, such as saving things to a database.


The latter two methods are trivial wrappers of the first.

Finally, :class:`~Streamer` is a context manager.
If you have added a concurrent operator,
and the cunsumption may end prematurely (you break out of the iteration) or abnormally due to exception
raised in any component,
then you should consume the stream with context management, like this::

    stream = Streamer(...).map(...)...parmap(...)...
    with stream:
        for x in stream:
            ...
            if some_condition:
                break

If there are no such concerns, you can consume the stream without context management.


API reference
=============


.. autoclass:: mpservice.streamer.Streamer


Reference (for an early version of the code): https://zpz.github.io/blog/stream-processing/