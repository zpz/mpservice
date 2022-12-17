========
streamer
========

.. automodule:: mpservice.streamer
    :no-members:
    :no-undoc-members:
    :no-special-members:


Basic usage
===========

Let's make up an I/O-bound operation which takes an input and produces an output.
(Think calling a remote service with an input and wait to get a result.)

>>> from time import sleep
>>> from random import random
>>> def double(x):
...     sleep(random() * 0.1)
...     return x * 2

Suppose we have a long stream of input values we want to process.
We feed this stream into a ``Streamer`` object::

>>> from mpservice.streamer import Streamer
>>> data_stream = Streamer(range(100))
  
The input stream if often a list, but more generally, it can be any
`Iterable <https://docs.python.org/3/library/collections.abc.html#collections.abc.Iterable>`_,
possibly unlimited.
Since ``double`` is an I/O-bound operation, let's use multiple threads to speed up
the processing of the input stream.
We need to use the ``Streamer`` in a context manager,
add the worker to it as a *transform*,
then retrieve results out of it.

>>> with data_stream:
...     data_stream.parmap(double, executor='thread', concurrency=8)

For this particular transform, we requested the function ``double`` to be run in 8 threads;
they will collectively process the input stream.
Adding the transform is just "setup"--nothing runs until we start to retrive results.
Later we'll see that we can add more than one transform, as well as other lighter-weight operations.
Because a ``Streamer`` is an `Iterator <https://docs.python.org/3/library/collections.abc.html#collections.abc.Iterator>`_,
"retrieving the results" usually amounts to iterating over it.
All of this must be done with the context manager.
Let's complete the code block:

>>> with data_stream:
...     data_stream.parmap(double, executor='thread', concurrency=8)
...     total = 0
...     for y in data_stream:
...         total += y
>>> total
9900

What is the expected result?

>>> sum((v*2 for v in range(100)))
9900

Despite the concurrency in the transform, the order of the input elements is preserved.
In other words, the output elements correspond to the input elements in order.
Let's print out the output elements to verify:

>>> with Streamer(range(100)) as data_stream:
...     data_stream.parmap(double, executor='thread', concurrency=8)
...     for k, y in enumerate(data_stream):
...         print(y, end='  ')
...         if (k + 1) % 10 == 0:
...             print('')
...     print('')
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
>>>

There can be many transforms in a "chain".
Suppose we want the output of ``double`` to go through another operation, like this:

>>> def shift(x, amount):
...     return x + amount
>>>
>>> with Streamer(range(20)) as data_stream:
...     data_stream.parmap(double, executor='thread', concurrency=8)
...     data_stream.parmap(shift, executor='thread', concurrency=3, amount=0.8)
...     for k, y in enumerate(data_stream):
...         print(y, end='  ')
...         if (k + 1) % 10 == 0:
...             print('')
...     print('')
0.8  2.8  4.8  6.8  8.8  10.8  12.8  14.8  16.8  18.8
20.8  22.8  24.8  26.8  28.8  30.8  32.8  34.8  36.8  38.8

If the operation is CPU-intensive, we should set ``executor='process'``.
Unfortunately we can not demo it in an interactive session because it uses "spawned processes".


The methods ``batch``, ``parmap``, and ``unbatch`` (and some others)
all modify the object ``data_stream`` "in-place" and return the original object,
hence it's fine to add these operations one at a time,
and it's not necessary to assign the intermediate results to new identifiers.
The above is equivalent to the following::

    with Streamer(range(100)) as data_stream:
        data_stream.batch(10)
        data_stream.parmap(my_op_that_takes_a_batch, concurrency=4)
        data_stream.unbatch()
        pipeline = data_stream

As the Streamer object undergoes the series of operations, it remains a "stream"
of elements but the content of the elements is changing. At any moment,
the object represents the state of the final operation up to that time.
The stream may consist of the same number of elements as the very original input stream of data,
where each element has gone through a series of operations, or, if
``batch`` and ``unbatch`` have been applied, the stream may consist more or less elements
than the original input.

After this setup, there are several ways to use the object ``data_stream`` (or ``pipeline``).

-  Since ``data_stream`` is an Iterable and an Iterator, we can use it as such.
   Most naturally, iterate over it and process each element however we like.

-  We can of course also provide ``data_stream`` as a parameter where an iterable
   or iterator is expected. For example, the ``mpservice.mpserver.Server``
   class has a method ``stream`` that expects an iterable, hence
   we can do things like

   ::

        server = Server(...)
        with server:
            for y in server.stream(data_stream):
                ...

   Note that ``server.stream(...)`` does not produce a ``Streamer`` object.
   If we want to put it in subsequent operations, simply turn it into a
   ``Streamer`` object::

            pipeline = Streamer(server.stream(data_stream))
            pipeline.parmap(yet_another_io_op)
            ...

-  If the stream is not too long (not "big data"), we can pass it to ``list`` to
   convert it to a list::

        result = list(data_stream)

Of all the methods on a ``Streamer`` object, two will start new threads, namely
``.buffer()`` and ``.parmap()``. (The latter may also start new processes.)

Hooks
=====

There are several "hooks" that allow user to pass in custom functions to
perform operations tailored to their need. Check out the following functions:

- ``map``
- ``filter``
- ``parmap``

``filter`` accept a function that evaluates a data element
and return a boolean value. Depending on the return value, the element
is dropped from or kept in the data stream.

``peek`` accepts a function that takes a data element and usually does
informational printing or logging (including persisting info to files).
This function may check conditions on the data element to decide whether
to do the printing or do nothing. (Usually we don't want to print for
every element; that would be overwhelming.)
This function is called for the side-effect;
it does not affect the flow of the data stream. The user-provided operator
should not modify the data element.

``parmap`` accepts a function that takes a data element, does something
about it, and returns a value. For example, modify the element and return
a new value, or call an external service with the data element as part of
the payload. Each input element will produce a new element, becoming the
resultant stream. This method can not "drop" a data element (i.e do not
produce a result corresponding to an input element), neither can it produce
multiple results for a single input element (if it produces a list, say,
that list would be the result for the single input.)
If the operation is mainly for the side effect, e.g.
saving data in files or a database, hence there isn't much useful result,
then the result could be ``None``, which is perfectly valid. Regardless,
the returned ``None``\s will still become the resultant stream.


Handling of exceptions
======================

There are two modes of exception handling.
In the first mode, exception propagates and, as it should, halts the program with
a printout of traceback. Any not-yet-processed data is discarded.

In the second mode, exception object is passed on in the pipeline as if it is
a regular data item. Subsequent data items are processed as usual.
This mode is enabled by ``return_exceptions=True`` to the function ``parmap``.
However, to the next operation, the exception object that is coming along
with regular data elements (i.e. regular output of the previous operation)
is most likely a problem. One may want to call ``drop_exceptions`` to remove
exception objects from the data stream before they reach the next operation.
In order to be knowledgeable about exceptions before they are removed,
the function ``log_exceptions`` can be used. Therefore, this is a useful pattern::

    (
        data_stream
        .parmap(func1,..., return_exceptions=True)
        .log_exceptions()
        .drop_exceptions()
        .parmap(func2,..., return_exceptions=True)
    )

Bear in mind that the first mode, with ``return_exceptions=False`` (the default),
is a totally legitimate and useful mode.


API reference
=============


.. autoclass:: mpservice.streamer.Streamer


Reference (for an early version of the code): https://zpz.github.io/blog/stream-processing/