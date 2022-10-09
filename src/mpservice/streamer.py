"""Process a data stream through operations with concurrency.

An input data stream goes through a series of operations.
The target use case is that one or more operations is I/O bound,
hence can benefit from concurrency via threading.
These operations are called `transform`s.

CPU-bound transforms are also supported, via multiprocessing.

The other operations are typically light weight and supportive
of the main (concurrent) transforms. These operations perform batching,
unbatching, buffering, filtering, logging, etc.

===========
Basic usage
===========

In a typical use case, one starts with a `Streamer` object, places it under
context management, and calls its methods in a "chained" fashion:

    data = range(100)
    with Streamer(data) as stream:
        pipeline = (
            stream
            .batch(10)
            .transform(my_op_that_takes_a_batch, concurrency=4)
            .unbatch()
            )

The methods `batch`, `transform`, and `unbatch` (and some others)
all modify the object `stream` "in-place" and return the original object,
hence it's fine to add these operations one at a time,
and it's not necessary to assign the intermediate results to new identifiers.
The above is equivalent to the following:

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
`batch` and `unbatch` have been applied, the stream may consist more or less elements
than the original input.

After this setup, there are several ways to use the object `stream` (or `pipeline`).

    1. Since `stream` is an Iterable and an Iterator, we can use it as such.
       Most naturally, iterate over it and process each element however
       we like.

       We can of couse also provide `stream` as a parameter where an iterable
       or iterator is expected. For example, the `mpservice.mpserver.Server`
       class has a method `stream` that expects an iterable, hence
       we can do things like

            server = Server(...)
            with server:
                for y in server.stream(stream):
                    ...
       Note that `server.stream(...)` does not produce a `Streamer` object.
       If we want to put it in subsequent operations, simply turn it into a
       `Streamer` object:

                pipeline = Streamer(server.stream(stream))
                pipeline.transform(yet_another_io_op)
                ...

       If the stream is not too long (not "big data"), we can pass it to `list` to
       convert it to a list:

            result = list(stream)

    2. If we don't need the elements coming out of `stream`, but rather
       just need the original data to flow through all the operations
       of the pipeline (e.g. if the last "substantial" operation is inserting
       the data into a database), we can "drain" the stream:

            stream.drain()

    3. We can continue to add more operations to the pipeline, for example,

            stream.transform(another_op, concurrency=3)

Of all the methods on a Streamer object, two will start new threads, namely
`.buffer()` and `.transform()`. (The latter may also start new processes.)


======================
Handling of exceptions
======================

There are two modes of exception handling.
In the first mode, exception propagates and, as it should, halts the program with
a printout of traceback. Any not-yet-processed data is discarded.

In the second mode, exception object is passed on in the pipeline as if it is
a regular data item. Subsequent data items are processed as usual.
This mode is enabled by `return_exceptions=True` to the function `transform`.
However, to the next operation, the exception object that is coming along
with regular data elements (i.e. regular output of the previous operation)
is most likely a problem. One may want to call `drop_exceptions` to remove
exception objects from the data stream before they reach the next operation.
In order to be knowledgeable about exceptions before they are removed,
the function `log_exceptions` can be used. Therefore, this is a useful pattern:

    (
        data_stream
        .transform(func1,..., return_exceptions=True)
        .log_exceptions()
        .drop_exceptions()
        .transform(func2,..., return_exceptions=True)
    )

Bear in mind that the first mode, with `return_exceptions=False` (the default),
is a totally legitimate and useful mode.

=====
Hooks
=====

There are several "hooks" that allow user to pass in custom functions to
perform operations tailored to their need. Check out the following functions:

    `drop_if`
    `keep_if`
    `peek`
    `transform`

Both `drop_if` and `keep_if` accept a function that evaluates a data element
and return a boolean value. Dependending on the return value, the element
is dropped from or kept in the data stream.

`peek` accepts a function that takes a data element and usually does
informational printing or logging (including persisting info to files).
This function may check conditions on the data element to decide whether
to do the printing or do nothing. (Usually we don't want to print for
every element; that would be overwhelming.)
This function is called for the side-effect;
it does not affect the flow of the data stream. The user-provided operator
should not modify the data element.

`transform` accepts a function that takes a data element, does something
about it, and returns a value. For example, modify the element and return
a new value, or call an external service with the data element as part of
the payload. Each input element will produce a new elment, becoming the
resultant stream. This method can not "drop" a data element (i.e do not
produce a result corresponding to an input element), neither can it produce
multiple results for a single input element (if it produces a list, say,
that list would be the result for the single input.)
If the operation is mainly for the side effect, e.g.
saving data in files or a database, hence there isn't much useful result,
then the result could be `None`, which is perfectly valid. Regardless,
the returned `None`s will still become the resultant stream.

Reference (for an early version of the code): https://zpz.github.io/blog/stream-processing/
"""

# Iterable vs iterator
#
# if we need to use
#
#   for v in X:
#       ...
#
# then `X.__iter__()` is called to get an "iterator".
# In this case, `X` is an "iterable", and it must implement `__iter__`.
#
# If we do not use it that way, but rather only directly call
#
#   next(X)
#
# then `X` must implement `__next__` (but does not need to implement `__iter__`).
# This `X` is an "iterator".
#
# Often we let `__iter__` return `self`, and implement `__next__` in the same class.
# This way the class is both an iterable and an iterator.

# Async generator returns an async iterator.

from __future__ import annotations

# Enable using a class in type annotations in the code
# that defines that class itself.
# https://stackoverflow.com/a/49872353
# Will no longer be needed in Python 3.10.

import concurrent.futures
import queue
import random
import threading
import traceback
from multiprocessing.util import Finalize
from typing import (
    Callable,
    TypeVar,
    Union,
    Optional,
    Iterable,
    Iterator,
    Tuple,
    List,
)

from overrides import EnforceOverrides, overrides, final

from .util import is_remote_exception, get_remote_traceback
from .util import is_exception, Thread, MP_SPAWN_CTX
from ._queues import SingleLane


FINISHED = "8d906c4b-1161-40cc-b585-7cfb012bca26"
STOPPED = "ceccca5e-9bb2-46c3-a5ad-29b3ba00ad3e"
CRASHED = "57cf8a88-434e-4772-9bca-01086f6c45e9"


T = TypeVar("T")  # indicates input data element
TT = TypeVar("TT")  # indicates output after an op on `T`


def _default_peek_func(i, x):
    # print("")
    # print("#", i)
    # if is_exception(x):
    #     print(repr(x))
    # else:
    #     print(x)
    print(f"#{i}:  {x!r}")


class Streamer(EnforceOverrides):
    def __init__(self, instream: Union[Iterator, Iterable], /):
        self.streamlets: List[Stream] = [Stream(instream)]
        self._started = False

    def __enter__(self):
        self._started = True
        return self

    def __exit__(self, *args, **kwargs):
        for s in self.streamlets:
            s._stop()

    @final
    def __iter__(self):
        if not self._started:
            raise RuntimeError(
                "iteration must be started within context manager, i.e. within a 'with ...:' block"
            )
        return self

    @final
    def __next__(self):
        return self.streamlets[-1].__next__()

    def drain(self) -> Tuple[int, int]:
        """Drain off the stream.

        Return a tuple of the number of elements processed
        as well as the number of exceptions (hopefully 0!).

        The number of exceptions could be non-zero only if
        upstream transformers have set `return_exceptions` to True.
        Otherwise, any exception would have propagated and
        prevented this method from completing.
        """
        n = 0
        nexc = 0
        for v in self:
            n += 1
            if is_exception(v):
                nexc += 1
        return n, nexc

    def drop_if(self, func: Callable[[int, T], bool], /):
        """
        `func`: a function that takes the data element index
            along with the element value, and returns `True` if the element
            should be skipped, that is, not included in the output stream.
        """
        self.streamlets.append(Dropper(self.streamlets[-1], func))
        return self

    def drop_exceptions(self):
        """
        Used to skip exception objects that are produced in an upstream
        transform that has `return_exceptions=True`. This way,
        the previous op allows exceptions (i.e. do not crash), and
        this op removes the exception objects from the output stream.
        """
        return self.drop_if(lambda i, x: is_exception(x))

    def drop_first_n(self, n: int):
        assert n >= 0
        return self.drop_if(lambda i, x: i < n)

    def keep_if(self, func: Callable[[int, T], bool], /):
        """Keep an element in the stream only if a condition is met.

        This is the opposite of `drop_if`.
        """
        return self.drop_if(lambda i, x: not func(i, x))

    def head(self, n: int):
        """
        Take the first `n` elements and ignore the rest.
        """
        # This does not delegate to `keep_if`, because `keep_if`
        # would need to walk through the entire stream,
        # which is not needed for `head`.
        self.streamlets.append(Header(self.streamlets[-1], n))
        return self

    def peek(self, func: Callable[[int, T], None] = None, /):
        """Take a peek at the data element *before* it is sent
        on for processing.

        The function `func` takes the data element index and value.
        Typical actions include print out
        info or save the data for later inspection. Usually this
        function should not modify the data element in the stream.

        User has flexibilities in `func`, e.g. to not print anything
        under certain conditions.
        """
        if func is None:
            func = _default_peek_func
        self.streamlets.append(Peeker(self.streamlets[-1], func))
        return self

    def peek_every_nth(
        self,
        n: int,
        peek_func: Callable[[int, T], None] = None,
        /,
        *,
        base: int = 0,
        first: int = 0,
        last: int = None,
    ):
        """
        `base`: if 0, peek at indices 0, n, 2*n, 3*n, ... (0-based);
            if 1, peek at indices n, 2*n, 3*n, ... (1-based).
        """
        assert n > 0
        assert base in (0, 1)

        if peek_func is None:
            peek_func = _default_peek_func

        def foo(i, x):
            k = i + base
            if k < first:
                return
            if last and k > last:
                return
            if k % n == 0:
                peek_func(k, x)

        return self.peek(foo)

    def peek_random(self, frac: float, peek_func: Callable[[int, T], None] = None, /):
        assert 0 < frac <= 1
        rand = random.random

        if peek_func is None:
            peek_func = _default_peek_func

        def foo(i, x):
            if rand() < frac:
                peek_func(i, x)

        return self.peek(foo)

    def peek_exceptions(self, *, with_trace: bool = True, print_func: Callable = None):
        # User may want to pass in `logger.error` as `print_func`.
        if print_func is None:
            print_func = print

        def func(i, x):
            if is_exception(x):
                trace = ""
                if with_trace:
                    if is_remote_exception(x):
                        trace = get_remote_traceback(x)
                    else:
                        try:
                            trace = "".join(traceback.format_tb(x.__traceback__))
                        except AttributeError:
                            pass
                if trace:
                    print_func("#%d:  %r\n%s" % (i, x, trace))
                else:
                    print_func("#%d:  %r" % (i, x))

        return self.peek(func)

    def batch(self, batch_size: int):
        """Bundle elements into batches, i.e. lists.

        Take elements from an input stream,
        and bundle them up into batches up to a size limit,
        and produce the batches.

        The output batches are all of the specified size,
        except possibly the final batch.
        There is no 'timeout' logic to proceed eagerly with a partial batch .
        For efficiency, this requires the input stream to have a steady supply.
        If that is a concern, having a `buffer` on the input stream
        prior to `batch` may help.
        """
        self.streamlets.append(Batcher(self.streamlets[-1], batch_size))
        return self

    def unbatch(self):
        """Reverse of "batch".

        Turn a stream of lists into a stream of individual elements.

        This is usually used to correpond with a previous
        `.batch()`, but that is not required. The only requirement
        is that the input elements are lists.
        """
        self.streamlets.append(Unbatcher(self.streamlets[-1]))
        return self

    def buffer(self, maxsize: int):
        """Buffer is used to stabilize and improve the speed of data flow.

        A buffer is useful after any operation that can not guarantee
        (almost) instant availability of output. A buffer allows its
        output to "pile up" when the downstream consumer is slow,
        so that data *is* available when the downstream does come to request
        data. The buffer evens out irregularities in the speeds of upstream
        production and downstream consumption.
        """
        self.streamlets.append(Buffer(self.streamlets[-1], maxsize))
        return self

    def transform(
        self,
        func: Callable[[T], TT],
        *,
        executor: str = "thread",
        concurrency: Optional[int] = None,
        return_x: bool = False,
        return_exceptions: bool = False,
        **func_args,
    ):
        """Apply a transformation on each element of the data stream,
        producing a stream of corresponding results.

        `func`: a sync function that takes a single input item
        as the first positional argument and produces a result.
        Additional keyword args can be passed in via `func_args`.
        Async can be supported, but it's a little more involved than the sync case.
        Since the need seems to be low, it's not supported for now.

        The outputs are in the order of the input elements.

        The main point of `func` does not have to be the output.
        It could rather be some side effect. For example,
        saving data in a database. In that case, the output may be
        `None`. Regardless, the output is yielded to be consumed by the next
        operator in the pipeline. A stream of `None`s could be used
        in counting, for example. The output stream may also contain
        Exception objects (if `return_exceptions` is `True`), which may be
        counted, logged, or handled in other ways.

        `executor`: either 'thread' or 'process'.

        `concurrency`: max number of concurrent calls to `func`. By default
        there is no concurrency, but this is usually *not* what you want,
        because the point of this method is to run an I/O-bound operation
        with concurrency.

        `return_x`: if True, output stream will contain tuples `(x, y)`;
        if False, output stream will contain `y` only.

        `return_exceptions`: if True, exceptions raised by `func` will be
        in the output stream as if they were regular results; if False,
        they will propagate. Note that this does not absorbe exceptions
        raised by previous components in the pipeline; it is concered about
        exceptions raised by `func` only.

        User may want to add a `buffer` to the output of this method,
        esp if the `func` operations are slow.
        """
        self.streamlets.append(
            Transformer(
                self.streamlets[-1],
                func,
                executor=executor,
                concurrency=concurrency,
                return_x=return_x,
                return_exceptions=return_exceptions,
                **func_args,
            )
        )
        return self


class Stream(EnforceOverrides):
    def __init__(self, instream: Union[Stream, Iterator, Iterable], /):
        if hasattr(instream, "__next__"):
            self._instream = instream
        else:
            self._instream = iter(instream)
        self.index = 0
        # Index of the upcoming element; 0 based.
        # Here, "element" refers to the element to be produced by
        # `self.__next__`, which does not need to have the same index
        # as the element to be produced by `next(self._instream)`.
        self._stopped = threading.Event()

    def _stop(self):
        # Clean up and deal with early-stop situations.
        self._stopped.set()

    @final
    def __iter__(self):
        return self

    def _get_next(self):
        """Produce the next element in the stream.

        Subclasses refine this method rather than `__next__`.
        In a subclass, almost always it will not get the next element
        from `self._instream`, but rather from some object that holds
        results of transformations on `self._instream`.
        In other words, subclass typically needs to override this method.
        Subclass should take care to handle exceptions in this method.
        """
        # This method should not increment `self.index` for the element
        # that is returned by this method. That is done by `__next__`.
        if self._stopped.is_set():
            raise StopIteration
        z = next(self._instream)
        return z

    @final
    def __next__(self):
        z = self._get_next()
        self.index += 1
        return z


class Batcher(Stream):
    def __init__(self, instream: Stream, /, batch_size: int):
        """
        Aggregate the stream into batches (lists) of the specified size.
        The last batch may have less elements.
        """
        super().__init__(instream)
        assert 1 <= batch_size <= 10_000
        self.batch_size = batch_size

    @overrides
    def _get_next(self):
        if self._stopped.is_set():
            raise StopIteration
        batch = []
        for _ in range(self.batch_size):
            try:
                batch.append(next(self._instream))
            except StopIteration:
                self._stop()
                break
        if batch:
            return batch
        raise StopIteration


class Unbatcher(Stream):
    def __init__(self, instream: Stream, /):
        """
        The incoming stream consists of lists.
        This object "expands" or "flattens" the lists into a stream
        of individual elements. Usually, the output stream
        is "longer" than the input stream.

        This may correspond to a "Batcher" operator upstream,
        but that is by no means a requirement.
        """
        super().__init__(instream)
        self._batch = None

    @overrides
    def _get_next(self):
        if self._stopped.is_set():
            raise StopIteration
        if self._batch:
            return self._batch.pop(0)
        z = next(self._instream)
        if is_exception(z):
            return z
        self._batch = z
        if self._stopped.is_set():
            raise StopIteration
        return self._batch.pop(0)


class Dropper(Stream):
    def __init__(self, instream: Stream, func: Callable[[int, T], bool], /):
        """
        `func`: takes element index and the element value; if returns True,
            the element is dropped (i.e. not produced; proceed to check the next
            elements until one returns False and gets produced); if returns
            False, the element is not drop (i.e., it is produced, hence the
            Dropper object becomes a simple pass-through for that element).

        `self.index` of this object has different meaning from other Stream
        classes. It is the index of the next element of the instream,
        not the index of the element to be produced (not-dropped) by the current
        object. This is because that, when the user designed the predicate function
        `func`, if the logic depends on the first argument, this is the
        interpretation of the "index" that's more natural and useful.
        """
        super().__init__(instream)
        self.func = func

    @overrides
    def _get_next(self):
        if self._stopped.is_set():
            raise StopIteration
        while True:
            z = next(self._instream)
            if self.func(self.index, z):
                self.index += 1
                continue
            return z


class Header(Stream):
    def __init__(self, instream: Stream, /, n: int):
        """
        Keeps the first `n` elements and ignores all the rest.
        """
        super().__init__(instream)
        assert n > 0
        self.n = n

    @overrides
    def _get_next(self):
        if self._stopped.is_set():
            raise StopIteration
        if self.index >= self.n:
            raise StopIteration
        return next(self._instream)


class Peeker(Stream):
    def __init__(self, instream: Stream, func: Callable[[int, T], None], /):
        """
        This class provides a mechanism to log or print some info for elements
        that meet certain conditions.

        `func`: takes element index and value, does whatever as long as the action
        does not modify the element (if it is of a mutable type).
        Usually, the action is to log or print about the element
        if the element (its index and/or value) meets certain conditions;
        do nothing if the element does not meet the conditions.
        """
        super().__init__(instream)
        self.func = func

    @overrides
    def _get_next(self):
        if self._stopped.is_set():
            raise StopIteration
        z = next(self._instream)
        self.func(self.index, z)
        return z


class Buffer(Stream):
    def __init__(self, instream: Stream, /, maxsize: int):
        super().__init__(instream)
        assert 1 <= maxsize <= 10_000
        self.maxsize = maxsize
        self._tasks = SingleLane(maxsize)
        self._worker = Thread(target=self._start, loud_exception=False)
        self._worker.start()
        self._finalize_func = Finalize(
            self,
            type(self)._finalize,
            (self._stopped, self._tasks, self._worker),
            exitpriority=10,
        )

    def _start(self):
        threading.current_thread().name = "BufferThread"
        q = self._tasks
        while True:
            try:
                v = next(self._instream)
            except StopIteration:
                q.put(FINISHED)
                break
            except Exception:
                q.put(STOPPED)
                raise
            if self._stopped.is_set():
                break
            q.put(v)  # if `q` is full, will wait here

    @overrides
    def _stop(self):
        self._finalize_func()

    @staticmethod
    def _finalize(stopped, tasks, worker):
        stopped.set()
        while not tasks.empty():
            _ = tasks.get()
        # `tasks` is now empty. The thread needs to put at most one
        # more element into the queue, which is safe.
        worker.join()

    @overrides
    def _get_next(self):
        if self._stopped.is_set():
            raise StopIteration
        while True:
            try:
                z = self._tasks.get(timeout=1)
                break
            except queue.Empty as e:
                if self._worker.is_alive():
                    continue
                if self._stopped.is_set():
                    raise StopIteration
                if self._worker.exception():
                    raise self._worker.exception()
                raise RuntimeError("unknown situation occurred") from e
        if z == FINISHED:
            self._stop()
            raise StopIteration
        if z == STOPPED:
            self._stop()
            raise self._worker.exception()
        return z


class Transformer(Stream):
    def __init__(
        self,
        instream: Stream,
        func: Callable[[T], TT],
        *,
        executor: str = "thread",  # or 'process'
        concurrency: int = None,
        return_x: bool = False,
        return_exceptions: bool = False,
        **func_args,
    ):
        super().__init__(instream)

        if concurrency is None:
            concurrency = 1
        else:
            assert concurrency > 0
        self._return_x = return_x
        self._return_exceptions = return_exceptions
        if executor == "thread":
            self._executor = concurrent.futures.ThreadPoolExecutor(concurrency)
        else:
            assert executor == "process"
            self._stopped = MP_SPAWN_CTX.Event()
            self._executor = concurrent.futures.ProcessPoolExecutor(
                concurrency, mp_context=MP_SPAWN_CTX
            )
        self._tasks = SingleLane(concurrency)
        self._worker = Thread(
            target=self._start, args=(func,), kwargs=func_args, loud_exception=False
        )
        self._worker.start()
        self._finalize_func = Finalize(
            self,
            type(self)._finalize,
            (self._stopped, self._tasks, self._worker, self._executor),
            exitpriority=10,
        )

    def _start(self, func, **kwargs):
        tasks = self._tasks
        while True:
            try:
                x = next(self._instream)
            except StopIteration:
                tasks.put(FINISHED)
                break
            except Exception:
                tasks.put(STOPPED)
                raise
            if self._stopped.is_set():
                break
            t = self._executor.submit(func, x, **kwargs)
            # The size of the queue `tasks` regulates how many
            # concurrent calls to `func` there can be.
            tasks.put((x, t))

    @overrides
    def _stop(self):
        self._finalize_func()

    @staticmethod
    def _finalize(stopped, tasks, worker, executor):
        stopped.set()
        while not tasks.empty():
            _ = tasks.get()
        worker.join()
        executor.shutdown()

    @overrides
    def _get_next(self):
        if self._stopped.is_set():
            raise StopIteration
        z = self._tasks.get()
        if z == FINISHED:
            self._stop()
            raise StopIteration
        if z == STOPPED:
            self._stop()
            raise self._worker.exception()

        x, fut = z
        try:
            y = fut.result()
            if self._return_x:
                return x, y
            return y
        except Exception as e:
            if self._return_exceptions:
                # TODO: think about when `e` is a "remote exception".
                if self._return_x:
                    return x, e
                return e
            else:
                self._stop()
                raise
