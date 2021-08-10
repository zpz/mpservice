'''Utilities for processing a continuous stream of data.

An input data stream goes through a series of operations.
The target use case is that one or more operations is I/O bound,
hence can benefit from multi-thread concurrency.
These operations are triggered via `transform`.

The other operations are light weight and supportive of the main (concurrent)
operation. These operations perform batching, unbatching, buffering,
filtering, logging, etc.

===========
Basic usage
===========

In a typical use case, one starts with a `Stream` object, and calls
its methods in a "chained" fashion:

    data = range(100)
    pipeline = (
        Stream(data)
        .batch(10)
        .transform(my_op_that_takes_stream_of_batches, workers=4)
        .unbatch()
        )

After this setup, there are four ways to use the object `pipeline`.

    1. Since `pipeline` is an Iterable and an Iterator, we can use it as such.
       Most naturally, iterate over it and process each element however
       we like.

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
    Tuple,
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


class IterQueue(queue.Queue):
    DEFAULT_MAXSIZE = 256
    GET_SLEEP = 0.0013
    PUT_SLEEP = 0.0014
    NO_MORE_DATA = object()

    def __init__(self, maxsize: int = None, upstream: Optional[IterQueue] = None):
        super().__init__(maxsize or self.DEFAULT_MAXSIZE)
        self.exception = None
        self._closed = False
        self._exhausted = False
        if upstream is None:
            self._to_shutdown = threading.Event()
        else:
            self._to_shutdown = upstream._to_shutdown

    def put_end(self):
        assert not self._closed
        self.put(self.NO_MORE_DATA)
        self._closed = True

    def put_exception(self, e):
        assert not self._closed
        self.exception = e
        self._to_shutdown.set()

    def put(self, x, block=True):
        while True:
            if self._to_shutdown.is_set():
                return
            assert not self._closed
            try:
                super().put(x, block=False)
                break
            except queue.Full:
                if block:
                    sleep(self.PUT_SLEEP)
                else:
                    raise

    def put_nowait(self, x):
        self.put(x, block=False)

    def get(self, block=True):
        while True:
            try:
                if self.exception is not None:
                    raise self.exception
                if self._exhausted:
                    return self.NO_MORE_DATA
                z = super().get(block=False)
                if z is self.NO_MORE_DATA:
                    self._exhausted = True
                return z
            except queue.Empty:
                if block:
                    sleep(self.GET_SLEEP)
                else:
                    raise

    def get_nowait(self):
        return self.get(block=False)

    def __next__(self):
        while True:
            try:
                z = self.get_nowait()
                if z is self.NO_MORE_DATA:
                    raise StopIteration
                return z
            except queue.Empty:
                sleep(self.GET_SLEEP)

    def __iter__(self):
        return self


def streamer_thread(q_in: Iterable, q_out: IterQueue,
                    func: Callable[..., None], *args, **kwargs):
    def foo(q_in, q_out, func, *args, **kwargs):
        try:
            func(q_in, q_out, *args, **kwargs)
            q_out.put_end()
        except Exception as e:
            q_out.put_exception(e)

    t = threading.Thread(
        target=foo, args=(q_in, q_out, func, *args), kwargs=kwargs
    )
    t.start()
    return t


def stream(x: Union[Iterable, Iterator], maxsize: int = None) -> IterQueue:
    if isinstance(x, IterQueue):
        return x

    if not hasattr(x, '__iter__'):
        if hasattr(x, '__next__'):
            def f(x):
                while True:
                    try:
                        yield x.__next__()
                    except StopIteration:
                        break
            x = f(x)
        else:
            raise TypeError('`x` is not iterable')

    def enqueue(q_in, q_out):
        for v in q_in:
            q_out.put(v)

    q = IterQueue(maxsize)
    _ = streamer_thread(x, q, enqueue)

    return q


def batch(q_in: IterQueue, q_out: IterQueue, batch_size: int) -> None:
    '''Take elements from an input stream,
    and bundle them up into batches up to a size limit,
    and produce the batches in an iterable.

    The output batches are all of the specified size, except possibly the final batch.
    There is no 'timeout' logic to produce a smaller batch.
    For efficiency, this requires the input stream to have a steady supply.
    If that is a concern, having a `buffer` on the input stream may help.
    '''
    assert 0 < batch_size <= 10000
    batch_ = []
    n = 0
    for x in q_in:
        batch_.append(x)
        n += 1
        if n >= batch_size:
            q_out.put(batch_)
            batch_ = []
            n = 0
    if n:
        q_out.put(batch_)


def unbatch(q_in: IterQueue, q_out: IterQueue) -> None:
    '''Reverse of "batch", turning a stream of batches into
    a stream of individual elements.
    '''
    for batch in q_in:
        for x in batch:
            q_out.put(x)


def buffer(q_in: IterQueue, q_out: IterQueue) -> None:
    '''Buffer is used to stabilize and improve the speed of data flow.

    A buffer is useful after any operation that can not guarantee
    (almost) instant availability of output. A buffer allows its
    output to "pile up" when the downstream consumer is slow in requests,
    so that data *is* available when the downstream does come to request
    data. The buffer evens out unstabilities in the speeds of upstream
    production and downstream consumption.
    '''
    for x in q_in:
        q_out.put(x)


def drop_if(q_in: IterQueue, q_out: IterQueue,
            func: Callable[[int, T], bool]) -> None:
    n = 0
    for x in q_in:
        if func(n, x):
            n += 1
            continue
        q_out.put(x)
        n += 1


def keep_if(q_in: IterQueue, q_out: IterQueue,
            func: Callable[[int, T], bool]) -> None:
    n = 0
    for x in q_in:
        if func(n, x):
            q_out.put(x)
        n += 1


def keep_first_n(q_in: IterQueue, q_out: IterQueue, n: int) -> None:
    assert n > 0
    k = 0
    for x in q_in:
        k += 1
        if k > n:
            break
        q_out.put(x)


def _default_peek_func(i, x):
    print('')
    print('#', i)
    print(x)


def peek(q_in: IterQueue, q_out: IterQueue,
         peek_func: Callable[[int, T], None] = None) -> None:
    '''Take a peek at the data element *before* it is sent
    on for processing.

    The function `peek_func` takes the data index (0-based)
    and the data element. Typical actions include print out
    info or save the data for later inspection. Usually this
    function should not modify the data element in the stream.
    '''
    if peek_func is None:
        peek_func = _default_peek_func

    n = 0
    for x in q_in:
        peek_func(n, x)
        q_out.put(x)
        n += 1


def log_exceptions(q_in: IterQueue, q_out: IterQueue,
                   level: str = 'error', drop: bool = False) -> None:
    flog = getattr(logger, level)

    for x in q_in:
        if is_exception(x):
            flog(x)
            if drop:
                continue
        q_out.put(x)


def _transform_thread(q_in, q_out, func, *,
                      workers, return_exceptions,
                      **func_args):
    if workers == 1:
        for x in q_in:
            try:
                z = func(x, **func_args)
                q_out.put(z)
            except Exception as e:
                if return_exceptions:
                    q_out.put(e)
                else:
                    raise e
        return

    def _process(in_stream, out_stream, func, lock, finished):
        Future = concurrent.futures.Future
        while not finished.is_set():
            with lock:
                # This locked block ensures that
                # input is read in order and their corresponding
                # result placeholders (Future objects) are
                # put in the output stream in order.
                if finished.is_set():
                    return
                try:
                    x = in_stream.__next__()
                    fut = Future()
                    out_stream.put(fut)
                except StopIteration:
                    finished.set()
                    out_stream.put_end()
                    return
                except Exception as e:
                    # `in_stream.exception` is not None.
                    # Propagate.
                    finished.set()
                    out_stream.put_exception(e)
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

    out_stream = IterQueue(max(q_in.maxsize, workers * 8))
    lock = threading.Lock()
    func = functools.wraps(func)(functools.partial(func, **func_args))
    finished = threading.Event()

    t_workers = [
        threading.Thread(target=_process,
                         args=(
                             q_in,
                             out_stream,
                             func,
                             lock,
                             finished,
                         ),
                         )
        for _ in range(workers)
    ]
    for t in t_workers:
        t.start()

    try:
        for fut in out_stream:
            z = fut.result()
            q_out.put(z)
    finally:
        finished.set()
        for t in t_workers:
            t.join()


def transform(q_in: IterQueue,
              q_out: IterQueue,
              func: Callable[[T], TT],
              *,
              workers: Optional[Union[int, str]] = None,
              return_exceptions: bool = False,
              **func_args,
              ) -> None:
    '''Apply a transformation on each element of the data stream,
    producing a stream of corresponding results.

    `func`: a sync function that takes a single input item
    as the first positional argument and produces a result.
    Additional keyword args can be passed in via `func_args`.

    The outputs are in the order of the input elements in `in_stream`.

    The main point of `func` does not have to be the output.
    It could rather be some side effect. For example,
    saving data in a database. In that case, the output may be
    `None`. Regardless, the output is yielded to be consumed by the next
    operator in the pipeline. A stream of `None`s could be used
    in counting, for example. The output stream may also contain
    Exception objects (if `return_exceptions` is `True`), which may be
    counted, logged, or handled in other ways.

    `workers`: max number of concurrent calls to `func`. By default
    this is 1, i.e. there is no concurrency.
    '''
    if workers is None:
        workers = 1
    elif isinstance(workers, str):
        assert workers == 'max'
        workers = MAX_THREADS
    else:
        workers > 0

    return _transform_thread(
        q_in, q_out, func,
        workers=workers, return_exceptions=return_exceptions,
        **func_args,
    )


def drain(q_in: IterQueue) -> Union[int, Tuple[int, int]]:
    '''Drain off the stream.

    Return the number of elements processed.
    When there are exceptions, return the total number of elements
    as well as the number of exceptions.
    '''
    n = 0
    nexc = 0
    for v in q_in:
        n += 1
        if is_exception(v):
            nexc += 1
    if nexc:
        return n, nexc
    return n


class StreamMixin:
    def drop_exceptions(self):
        return self.drop_if(lambda i, x: is_exception(x))

    def drop_first_n(self, n: int):
        assert n >= 0
        return self.drop_if(lambda i, x: i < n)

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
            flog('data item #%d:  %s', i, x)

        return self.peek_every_nth(nth, foo)


class Stream(StreamMixin):
    @ classmethod
    def registerapi(cls,
                    func: Callable[..., None],
                    *,
                    name: str = None,
                    maxsize: bool = False,
                    maxsize_first: bool = False,
                    ) -> None:
        '''
        `func` expects the input and output streams (both of type IterQueue)
        as the first two positional arguments. It may take additional
        positional and keyword arguments. See the functions `batch`, `drop_if`,
        `transform`, etc for examples.

        The created method accepts the extra positional and keyword
        args after the first two positional args. The input and output
        streams are not args of the method, because the first is
        provided by the host object, whereas the second is constructed
        during this registration.

        If `maxsize` is `True`, the created method also takes keyword
        arg `maxsize`, which is passed to the constructor of the
        output stream object, which is of type IterQueue.

        User can use this method to register other functions so that they
        can be used as methods of a `Stream` object, just like `batch`,
        `drop_if`, etc.
        '''
        if not name:
            name = func.__name__

        def _internal(maxsize, in_stream, *args, **kwargs):
            q_out = IterQueue(maxsize, in_stream)
            _ = streamer_thread(in_stream, q_out, func, *args, **kwargs)
            return cls(q_out)

        if maxsize:
            if maxsize_first:
                @ functools.wraps(func)
                def wrapped(self, maxsize: int = None, **kwargs):
                    if maxsize is None:
                        maxsize = self.in_stream.maxsize
                    return _internal(maxsize, self.in_stream, **kwargs)
            else:
                @ functools.wraps(func)
                def wrapped(self, *args, maxsize: int = None, **kwargs):
                    if maxsize is None:
                        maxsize = self.in_stream.maxsize
                    return _internal(maxsize, self.in_stream, *args, **kwargs)
        else:
            @ functools.wraps(func)
            def wrapped(self, *args, **kwargs):
                return _internal(self.in_stream.maxsize,
                                 self.in_stream, *args, **kwargs)

        setattr(cls, name, wrapped)

    def __init__(self,
                 in_stream: Union[Iterable, Iterator, IterQueue],
                 *,
                 maxsize: int = None):
        self.in_stream = stream(in_stream, maxsize=maxsize)

    def __next__(self):
        return self.in_stream.__next__()

    def __iter__(self):
        return self.in_stream.__iter__()

    def collect(self) -> list:
        return list(self.in_stream)

    def drain(self):
        return drain(self.in_stream)


Stream.registerapi(batch, maxsize=True)
Stream.registerapi(unbatch, maxsize=True)
Stream.registerapi(buffer, maxsize=True, maxsize_first=True)
Stream.registerapi(drop_if)
Stream.registerapi(keep_if)
Stream.registerapi(keep_first_n)
Stream.registerapi(peek)
Stream.registerapi(log_exceptions)
Stream.registerapi(transform)
