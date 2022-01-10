'''Utilities for processing a continuous stream of data.

An input data stream goes through a series of operations.
The target use case is that one or more operations is I/O bound,
hence can benefit from multi-thread concurrency.
These operations are triggered via `transform`.

The other operations are typically light weight and supportive
of the main (concurrent) operation. These operations perform batching,
unbatching, buffering, filtering, logging, etc.

===========
Basic usage
===========

In a typical use case, one starts with a `Stream` object, and calls
its methods in a "chained" fashion:

    data = range(100)
    pipeline = (
        Stream(data)
        .batch(10)
        .transform(my_op_that_takes_a_batch, workers=4)
        .unbatch()
        )

After this setup, there are several ways to use the object `pipeline`.

    1. Since `pipeline` is an Iterable and an Iterator, we can use it as such.
       Most naturally, iterate over it and process each element however
       we like.

       We can of couse also provide `pipeline` as a parameter where an iterable
       or iterator is expected. For example, the `mpservice.mpserver.Server`
       class has a method `stream` that expects an iterable, hence
       we can do things like

            server = Server(...)
            with server:
                pipeline = ...
                pipeline = server.stream(pipeline)
                pipeline = pipeline.transform(yet_another_io_op)

    2. If the stream is not too long (not "big data"), we can convert it to
       a list by the method `collect`:

            result = pipeline.collect()

    3. If we don't need the elements coming out of `pipeline`, but rather
       just need the original data (`data`) to flow through all the operations
       of the pipeline (e.g. if the last "substantial" operation is inserting
       the data into a database), we can "drain" the pipeline:

            n = pipeline.drain()

       where the returned `n` is the number of elements coming out of the
       last operation in the pipeline.

    4. We can continue to add more operations to the pipeline, for example,

            pipeline = pipeline.transform(another_op, workers=3)

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
then the result could be `None`, which is not a problem. Regardless,
the returned `None`s will still become the resultant stream.
'''

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

import asyncio
import collections.abc
import concurrent.futures
import functools
import inspect
import logging
import multiprocessing
import queue
import random
import threading
from time import sleep
from typing import (
    Callable, TypeVar, Union, Optional,
    Iterable, Iterator,
    Tuple, Type,
)


MAX_THREADS = min(32, multiprocessing.cpu_count() + 4)
# This default is suitable for I/O bound operations.
# For others, user may want to specify a smaller value.


logger = logging.getLogger(__name__)

T = TypeVar('T')
TT = TypeVar('TT')


def is_exception(e):
    return isinstance(e, Exception) or (
        inspect.isclass(e) and issubclass(e, Exception)
    )


def _default_peek_func(i, x):
    print('')
    print('#', i)
    print(x)


class StreamMixin:
    def drop_exceptions(self):
        return self.drop_if(lambda i, x: is_exception(x))

    def drop_nones(self):
        return self.drop_if(lambda i, x: x is None)

    def drop_first_n(self, n: int):
        assert n >= 0
        return self.drop_if(lambda i, x: i < n)

    def keep_if(self, func: Callable[[int, T], bool]):
        return self.drop_if(lambda i, x: not func(i, x))

    def keep_every_nth(self, nth: int):
        assert nth > 0
        return self.keep_if(lambda i, x: i % nth == 0)

    def keep_random(self, frac: float):
        assert 0 < frac <= 1
        rand = random.random
        return self.keep_if(lambda i, x: rand() < frac)

    def peek_every_nth(self, nth: int, peek_func: Callable[[int, T], None] = None):
        assert nth > 0

        if peek_func is None:
            peek_func = _default_peek_func

        def foo(i, x):
            if i % nth == 0:
                peek_func(i, x)

        return self.peek(foo)

    def peek_random(self, frac: float, peek_func: Callable[[int, T], None] = None):
        assert 0 < frac <= 1
        rand = random.random

        if peek_func is None:
            peek_func = _default_peek_func

        def foo(i, x):
            if rand() < frac:
                peek_func(i, x)

        return self.peek(foo)

    def log_every_nth(self, nth: int, level: str = 'info'):
        assert nth > 0
        flog = getattr(logger, level)

        def foo(i, x):
            flog('#%d:  %r', i, x)

        return self.peek_every_nth(nth, foo)

    def log_exceptions(self, level: str = 'error') -> Peeker:
        flog = getattr(logger, level)

        def func(i, x):
            if is_exception(x):
                flog('#%d:  %r', i, x)

        return self.peek(func)


class Stream(collections.abc.Iterator, StreamMixin):
    @classmethod
    def register(cls, class_: Type[Stream], name: str):
        def f(self, *args, **kwargs):
            return class_(self, *args, **kwargs)

        setattr(cls, name, f)

    def __init__(self, instream: Union[Stream, Iterator, Iterable]):
        if isinstance(instream, Stream):
            self._to_shutdown = instream._to_shutdown
            self._instream = instream
        else:
            self._to_shutdown = threading.Event()
            if hasattr(instream, '__next__'):
                self._instream = instream
            else:
                self._instream = iter(instream)
        self.index = 0
        # Index of the upcoming element; 0 based.
        # This is also the count of finished elements.

    def __iter__(self):
        return self

    def _get_next(self):
        return next(self._instream)

    def __next__(self):
        try:
            z = self._get_next()
            self.index += 1
            return z
        except StopIteration:
            raise
        except:
            self._to_shutdown.set()
            raise

    def collect(self) -> list:
        return list(self)

    def drain(self) -> Union[int, Tuple[int, int]]:
        '''Drain off the stream.

        Return the number of elements processed.
        When there are exceptions, return the total number of elements
        as well as the number of exceptions.
        '''
        n = 0
        nexc = 0
        for v in self:
            n += 1
            if is_exception(v):
                nexc += 1
        if nexc:
            return n, nexc
        return n

    def batch(self, batch_size: int) -> Batcher:
        '''Take elements from an input stream,
        and bundle them up into batches up to a size limit,
        and produce the batches in an iterable.

        The output batches are all of the specified size, except possibly the final batch.
        There is no 'timeout' logic to produce a smaller batch.
        For efficiency, this requires the input stream to have a steady supply.
        If that is a concern, having a `buffer` on the input stream may help.
        '''
        return Batcher(self, batch_size)

    def unbatch(self) -> Unbatcher:
        '''Reverse of "batch", turning a stream of batches into
        a stream of individual elements.
        '''
        return Unbatcher(self)

    def drop_if(self, func: Callable[[int, T], bool]) -> Dropper:
        return Dropper(self, func)

    def head(self, n: int) -> Head:
        return Head(self, n)

    def peek(self, func: Callable[[int, T], None] = None) -> Peeker:
        '''Take a peek at the data element *before* it is sent
        on for processing.

        The function `func` takes the data index (0-based)
        and the data element. Typical actions include print out
        info or save the data for later inspection. Usually this
        function should not modify the data element in the stream.
        '''
        if func is None:
            func = _default_peek_func
        return Peeker(self, func)

    def buffer(self, maxsize: int = None) -> Buffer:
        '''Buffer is used to stabilize and improve the speed of data flow.

        A buffer is useful after any operation that can not guarantee
        (almost) instant availability of output. A buffer allows its
        output to "pile up" when the downstream consumer is slow in requests,
        so that data *is* available when the downstream does come to request
        data. The buffer evens out unstabilities in the speeds of upstream
        production and downstream consumption.
        '''
        if maxsize is None:
            maxsize = 256
        else:
            assert 1 <= maxsize <= 10_000
        return Buffer(self, maxsize)

    def transform(self,
                  func: Callable[[T], TT],
                  *,
                  workers: Optional[Union[int, str]] = None,
                  return_exceptions: bool = False,
                  **func_args) -> Union[Transformer, ConcurrentTransformer]:
        '''Apply a transformation on each element of the data stream,
        producing a stream of corresponding results.

        `func`: a sync function that takes a single input item
        as the first positional argument and produces a result.
        Additional keyword args can be passed in via `func_args`.

        The outputs are in the order of the input elements in `self._instream`.

        The main point of `func` does not have to be the output.
        It could rather be some side effect. For example,
        saving data in a database. In that case, the output may be
        `None`. Regardless, the output is yielded to be consumed by the next
        operator in the pipeline. A stream of `None`s could be used
        in counting, for example. The output stream may also contain
        Exception objects (if `return_exceptions` is `True`), which may be
        counted, logged, or handled in other ways.

        `workers`: max number of concurrent calls to `func`. By default
        this is 0, i.e. there is no concurrency.

        `workers=0` and `workers=1` are different. The latter runs the
        transformer in a separate thread whereas the former runs "inline".

        When `workers = N > 0`, the worker threads are named 'transformer-0',
        'transformer-1',..., 'transformer-<N-1>'.
        '''
        if func_args:
            func = functools.partial(func, **func_args)

        if workers is None or workers == 0:
            return Transformer(self, func, return_exceptions=return_exceptions)

        if workers == 'max':
            workers = MAX_THREADS
        else:
            1 <= workers <= 100
        return ConcurrentTransformer(
            self, func, workers=workers,
            return_exceptions=return_exceptions)


class Batcher(Stream):
    def __init__(self, instream: Stream, batch_size: int):
        super().__init__(instream)
        assert 1 < batch_size <= 10_000
        self.batch_size = batch_size
        self._done = False

    def _get_next(self):
        if self._done:
            raise StopIteration
        batch = []
        for _ in range(self.batch_size):
            try:
                batch.append(next(self._instream))
            except StopIteration:
                self._done = True
                break
        if batch:
            return batch
        raise StopIteration


class Unbatcher(Stream):
    def __init__(self, instream: Stream):
        super().__init__(instream)
        self._batch = None

    def _get_next(self):
        if self._batch:
            return self._batch.pop(0)
        self._batch = next(self._instream)
        return self._get_next()


class Dropper(Stream):
    def __init__(self, instream: Stream, func: Callable[[int, T], bool]):
        super().__init__(instream)
        self.func = func

    def _get_next(self):
        while True:
            z = next(self._instream)
            if self.func(self.index, z):
                self.index += 1
                continue
            return z


class Head(Stream):
    def __init__(self, instream: Stream, n: int):
        super().__init__(instream)
        assert n >= 0
        self.n = n

    def _get_next(self):
        if self.index >= self.n:
            raise StopIteration
        return self._instream.__next__()


class Peeker(Stream):
    def __init__(self, instream: Stream, func: Callable[[int, T], None]):
        super().__init__(instream)
        self.func = func

    def _get_next(self):
        z = next(self._instream)
        self.func(self.index, z)
        return z


class IterQueue(queue.Queue, collections.abc.Iterator):
    '''
    A queue that supports iteration over its elements.

    In order to support iteration, it adds a special value
    to indicate end of data, which is inserted by calling
    the method `put_end`.
    '''
    GET_SLEEP = 0.00056
    PUT_SLEEP = 0.00045
    NO_MORE_DATA = object()

    def __init__(self, maxsize: int, to_shutdown: threading.Event):
        '''
        `upstream`: an upstream `IterQueue` object, usually the data stream that
        feeds into the current queue. This parameter allows this object and
        the upstream share an `Event` object that indicates either queue
        has stopped working, either deliberately or by exception.
        '''
        super().__init__(maxsize + 1)
        self._to_shutdown = to_shutdown

    def put_end(self, block: bool = True):
        self.put(self.NO_MORE_DATA, block=block)

    def put(self, x, block: bool = True):
        while True:
            try:
                super().put(x, block=False)
                break
            except queue.Full:
                if self._to_shutdown.is_set():
                    return
                if block:
                    sleep(self.PUT_SLEEP)
                else:
                    raise

    def __next__(self):
        while True:
            try:
                z = self.get_nowait()
                if z is self.NO_MORE_DATA:
                    raise StopIteration
                return z
            except queue.Empty:
                sleep(self.GET_SLEEP)

    async def __anext__(self):
        # This is used by `async_streamer`.
        while True:
            try:
                z = self.get_nowait()
                if z is self.NO_MORE_DATA:
                    raise StopAsyncIteration
                return z
            except queue.Empty:
                await asyncio.sleep(self.GET_SLEEP)


class Buffer(Stream):
    def __init__(self, instream: Stream, maxsize: int):
        super().__init__(instream)
        assert 1 <= maxsize <= 10_000
        self.maxsize = maxsize
        self._q = IterQueue(maxsize, self._to_shutdown)
        self._err = None
        self._thread = None
        self._start()

    def _start(self):
        def foo(instream, q):
            try:
                for v in instream:
                    q.put(v)
                    if self._to_shutdown.is_set():
                        break
                q.put_end()
            except Exception as e:
                # This should be exception while
                # getting data from `instream`,
                # not exception in the current object.
                self._err = e
                self._to_shutdown.set()

        self._thread = threading.Thread(
            target=foo, args=(self._instream, self._q))
        self._thread.start()

    def _stop(self):
        if self._thread is not None:
            self._thread.join()
            self._thread = None

    def __del__(self):
        self._stop()

    def _get_next(self):
        if self._err is not None:
            self._stop()
            raise self._err
        z = next(self._q)
        return z


class Transformer(Stream):
    def __init__(self,
                 instream: Stream,
                 func: Callable[[T], TT],
                 *,
                 return_exceptions: bool = False,
                 ):
        super().__init__(instream)
        self.func = func
        self.return_exceptions = return_exceptions

    def _get_next(self):
        z = next(self._instream)
        try:
            return self.func(z)
        except Exception as e:
            if self.return_exceptions:
                return e
            raise


def transform(in_stream: Iterator, out_stream: IterQueue,
              func, workers, return_exceptions, err):
    def _process(in_stream, out_stream, func,
                 lock, finished, return_exceptions):
        Future = concurrent.futures.Future
        while not finished.is_set():
            with lock:
                # This locked block ensures that
                # input is read in order and their corresponding
                # result placeholders (Future objects) are
                # put in the output stream in order.
                if finished.is_set():
                    return
                if err:
                    return
                try:
                    x = next(in_stream)
                    fut = Future()
                    out_stream.put(fut)
                except StopIteration:
                    finished.set()
                    out_stream.put_end()
                    return
                except Exception as e:
                    finished.set()
                    err.append(e)
                    return

            try:
                y = func(x)
                fut.set_result(y)
            except Exception as e:
                if return_exceptions:
                    fut.set_result(e)
                else:
                    fut.set_exception(e)
                    finished.set()
                    return

    lock = threading.Lock()
    finished = threading.Event()

    tasks = [
        threading.Thread(
            target=_process,
            name=f'transformer-{i}',
            args=(in_stream, out_stream, func, lock,
                  finished, return_exceptions),
        )
        for i in range(workers)
    ]
    for t in tasks:
        t.start()
    return tasks


class ConcurrentTransformer(Stream):
    def __init__(self,
                 instream: Stream,
                 func: Callable[[T], TT],
                 *,
                 workers: int,
                 return_exceptions: bool = False,
                 ):
        assert workers >= 1
        super().__init__(instream)
        self.func = func
        self.workers = workers
        self.return_exceptions = return_exceptions
        self._outstream = IterQueue(workers * 8, self._to_shutdown)
        self._err = []
        self._tasks = []
        self._start()

    def _start(self):
        self._tasks = transform(
            self._instream, self._outstream, self.func,
            self.workers, self.return_exceptions, self._err)

    def _stop(self):
        for t in self._tasks:
            t.join()

    def __del__(self):
        self._stop()

    def _get_next(self):
        try:
            if self._err:
                raise self._err[0]
            fut = next(self._outstream)
            return fut.result()
        except:
            self._stop()
            raise
