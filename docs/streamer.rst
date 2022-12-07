========
streamer
========

.. automodule:: mpservice.streamer
    :no-members:
    :no-undoc-members:
    :no-special-members:


Basic usage
===========

In a typical use case, one starts with a ``Streamer`` object, places it under
context management, and calls its methods in a "chained" fashion::

    data = range(100)
    with Streamer(data) as stream:
        pipeline = (
            stream
            .batch(10)
            .transform(my_op_that_takes_a_batch, concurrency=4)
            .unbatch()
            )

The methods ``batch``, ``transform``, and ``unbatch`` (and some others)
all modify the object ``stream`` "in-place" and return the original object,
hence it's fine to add these operations one at a time,
and it's not necessary to assign the intermediate results to new identifiers.
The above is equivalent to the following::

    with Streamer(range(100)) as stream:
        stream.batch(10)
        stream.transform(my_op_that_takes_a_batch, concurrency=4)
        stream.unbatch()
        pipeline = stream

As the Streamer object undergoes the series of operations, it remains a "stream"
of elements but the content of the elements is changing. At any moment,
the object represents the state of the final operation up to that time.
The stream may consist of the same number of elements as the very original input stream of data,
where each element has gone through a series of operations, or, if
``batch`` and ``unbatch`` have been applied, the stream may consist more or less elements
than the original input.

After this setup, there are several ways to use the object ``stream`` (or ``pipeline``).

1. Since ``stream`` is an Iterable and an Iterator, we can use it as such.
   Most naturally, iterate over it and process each element however we like.

   We can of course also provide ``stream`` as a parameter where an iterable
   or iterator is expected. For example, the ``mpservice.mpserver.Server``
   class has a method ``stream`` that expects an iterable, hence
   we can do things like

   ::

        server = Server(...)
        with server:
            for y in server.stream(stream):
                ...

   Note that ``server.stream(...)`` does not produce a ``Streamer`` object.
   If we want to put it in subsequent operations, simply turn it into a
   ``Streamer`` object::

            pipeline = Streamer(server.stream(stream))
            pipeline.transform(yet_another_io_op)
            ...

   If the stream is not too long (not "big data"), we can pass it to ``list`` to
   convert it to a list::

        result = list(stream)

2. If we don't need the elements coming out of ``stream``, but rather
   just need the original data to flow through all the operations
   of the pipeline (e.g. if the last "substantial" operation is inserting
   the data into a database), we can "drain" the stream::

        stream.drain()

3. We can continue to add more operations to the pipeline, for example,

   ::

        stream.transform(another_op, concurrency=3)

Of all the methods on a Streamer object, two will start new threads, namely
``.buffer()`` and ``.transform()``. (The latter may also start new processes.)


Handling of exceptions
======================

There are two modes of exception handling.
In the first mode, exception propagates and, as it should, halts the program with
a printout of traceback. Any not-yet-processed data is discarded.

In the second mode, exception object is passed on in the pipeline as if it is
a regular data item. Subsequent data items are processed as usual.
This mode is enabled by ``return_exceptions=True`` to the function ``transform``.
However, to the next operation, the exception object that is coming along
with regular data elements (i.e. regular output of the previous operation)
is most likely a problem. One may want to call ``drop_exceptions`` to remove
exception objects from the data stream before they reach the next operation.
In order to be knowledgeable about exceptions before they are removed,
the function ``log_exceptions`` can be used. Therefore, this is a useful pattern::

    (
        data_stream
        .transform(func1,..., return_exceptions=True)
        .log_exceptions()
        .drop_exceptions()
        .transform(func2,..., return_exceptions=True)
    )

Bear in mind that the first mode, with ``return_exceptions=False`` (the default),
is a totally legitimate and useful mode.

Hooks
=====

There are several "hooks" that allow user to pass in custom functions to
perform operations tailored to their need. Check out the following functions:

- ``drop_if``
- ``keep_if``
- ``peek``
- ``transform``

Both ``drop_if`` and ``keep_if`` accept a function that evaluates a data element
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

``transform`` accepts a function that takes a data element, does something
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


End-user API
============

.. autoclass:: mpservice.streamer.Streamer

Individual operators
====================

.. autoclass:: mpservice.streamer.Stream

.. autoclass:: mpservice.streamer.Batcher

.. autoclass:: mpservice.streamer.Unbatcher

.. autoclass:: mpservice.streamer.Dropper

.. autoclass:: mpservice.streamer.Header

.. autoclass:: mpservice.streamer.Peeker

.. autoclass:: mpservice.streamer.Buffer

.. autoclass:: mpservice.streamer.Transformer


Reference (for an early version of the code): https://zpz.github.io/blog/stream-processing/