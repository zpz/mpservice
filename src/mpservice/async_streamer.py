'''Utilities for processing a continuous stream of data in an async context.

There is an input stream, which is an AsyncIterator.
This stream goes through a series of async operations, each of which
takes an AsyncIterator, and returns another AsyncIterator.
Thanks to this consistency of input/output, the operations can be "chained".

If the input is not an AsyncIterator, but rather some other (sync or async)
iterable, then the function `stream` will turn it into an AsyncIterator.

The target use case is that one or more operations is I/O bound,
hence can benefit from async or multi-thread concurrency.
These operations (which are sync or async functions) are triggered
via `transform`.

The other operations are light weight and supportive of the main (concurrent)
operation. These operations perform batching, unbatching, buffering,
filtering, logging, etc.

In a typical use, one starts with a `Stream` object and calls its methods
in a "chained" fashion:

    data = range(100)
    pipeline = (
        Stream(data)
        .batch(10)
        .transform(my_op_that_takes_stream_of_batches, workers=4)
        .unbatch()
        )
    result = [_ async for _ in pipeline]
or
    result = await result.collect()
or
    await result.drain()

(Of course, you don't have to call all the methods in one statement.)

Although the primary or initial target use is concurrent I/O-bound
operations, CPU-bound operations could be performed concurrently
in a `mpservice.Server` and registered by `transform`.

Reference for an early version: https://zpz.github.io/blog/stream-processing/

Please refer to the sync counterpart in the module `mpservice.streamer`
for additional info and doc.
'''

import asyncio
import functools
import inspect
import logging
import multiprocessing
import queue
import threading
import time
from typing import (
    Callable, Awaitable, TypeVar, Optional, Union,
    AsyncIterable, AsyncIterator, Iterable,
    Tuple)

from . import streamer as _sync_streamer
from .streamer import is_exception

MAX_THREADS = min(32, multiprocessing.cpu_count() + 4)
# This default is suitable for I/O bound operations.
# For others, user may want to specify a smaller value.


logger = logging.getLogger(__name__)


NO_MORE_DATA = object()

T = TypeVar('T')
TT = TypeVar('TT')

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


class IterQueue(asyncio.Queue):
    DEFAULT_MAXSIZE = 256

    def __init__(self, maxsize: int = None, to_shutdown: threading.Event = None):
        super().__init__(maxsize or self.DEFAULT_MAXSIZE)
        self.exception = None
        self._closed = False
        self._exhausted = False
        if to_shutdown is None:
            to_shutdown = threading.Event()
        self._to_shutdown = to_shutdown

    async def put_end(self):
        assert not self._closed
        await self.put(NO_MORE_DATA)
        self._closed = True

    async def put_exception(self, e):
        assert not self._closed
        self.exception = e
        self._to_shutdown.set()

    async def put(self, x):
        while True:
            try:
                self.put_nowait(x)
                break
            except asyncio.QueueFull:
                await asyncio.sleep(0.0015)

    def put_nowait(self, x):
        if self._to_shutdown.is_set():
            return
        assert not self._closed
        super().put_nowait(x)

    async def get(self):
        while True:
            try:
                return self.get_nowait()
            except asyncio.QueueEmpty:
                await asyncio.sleep(0.0012)

    def get_nowait(self):
        if self.exception is not None:
            raise self.exception
        if self._exhausted:
            return NO_MORE_DATA
        z = super().get_nowait()
        if z is NO_MORE_DATA:
            self._exhausted = True
        return z

    async def __anext__(self):
        while True:
            z = await self.get()
            if z is NO_MORE_DATA:
                raise StopAsyncIteration
            return z

    def __aiter__(self):
        return self


def stream(x: Union[Iterable, AsyncIterable, IterQueue], maxsize: int = None) -> IterQueue:
    if isinstance(x, IterQueue):
        return x

    async def f1(data):
        async for v in data:
            yield v

    async def f2(data):
        while True:
            try:
                yield await data.__anext__()
            except StopAsyncIteration:
                break

    async def f3(data):
        for v in data:
            yield v

    async def f4(data):
        while True:
            try:
                yield data.__next__()
            except StopIteration:
                break

    if hasattr(x, '__aiter__'):
        if not hasattr(x, '__anext__'):
            x = f1(x)
    elif hasattr(x, '__anext__'):
        x = f2(x)
    elif hasattr(x, '__iter__'):
        x = f3(x)
    elif hasattr(x, '__next__'):
        x = f4(x)
    else:
        raise TypeError("`x` is neither iterable nor async iterable")

    async def enqueue(q_in, q_out):
        try:
            async for v in q_in:
                await q_out.put(v)
            await q_out.put_end()
        except Exception as e:
            await q_out.put_exception(e)

    q_out = IterQueue(maxsize=maxsize)
    _ = asyncio.create_task(enqueue(x, q_out))
    return q_out


async def batch(q_in: IterQueue, q_out: IterQueue, batch_size: int) -> None:
    assert 0 < batch_size <= 10000
    batch_ = []
    n = 0
    try:
        async for x in q_in:
            batch_.append(x)
            n += 1
            if n >= batch_size:
                await q_out.put(batch_)
                batch_ = []
                n = 0
        if n:
            await q_out.put(batch_)
        await q_out.put_end()
    except Exception as e:
        await q_out.put_exception(e)


async def unbatch(q_in: IterQueue, q_out: IterQueue) -> None:
    try:
        async for batch in q_in:
            for x in batch:
                await q_out.put(x)
        await q_out.put_end()
    except Exception as e:
        await q_out.put_exception(e)


async def buffer(q_in: IterQueue, q_out: IterQueue) -> None:
    try:
        async for x in q_in:
            await q_out.put(x)
        await q_out.put_end()
    except Exception as e:
        await q_out.put_exception(e)


async def drop_if(q_in: IterQueue,
                  q_out: IterQueue,
                  func: Callable[[int, T], bool]) -> None:
    n = 0
    try:
        async for x in q_in:
            if func(n, x):
                n += 1
                continue
            await q_out.put(x)
            n += 1
        await q_out.put_end()
    except Exception as e:
        await q_out.put_exception(e)


async def keep_if(q_in: IterQueue,
                  q_out: IterQueue,
                  func: Callable[[int, T], bool]) -> None:
    n = 0
    try:
        async for x in q_in:
            if func(n, x):
                await q_out.put(x)
            n += 1
        await q_out.put_end()
    except Exception as e:
        await q_out.put_exception(e)


async def keep_first_n(q_in: IterQueue, q_out: IterQueue, n: int) -> None:
    assert n > 0
    k = 0
    try:
        async for x in q_in:
            await q_out.put(x)
            k += 1
            if k >= n:
                break
        await q_out.put_end()
    except Exception as e:
        await q_out.put_exception(e)


async def peek(q_in: IterQueue,
               q_out: IterQueue,
               peek_func: Callable[[int, T], None] = None,
               ) -> None:
    if peek_func is None:
        peek_func = _sync_streamer._default_peek_func

    n = 0
    try:
        async for x in q_in:
            peek_func(n, x)
            await q_out.put(x)
            n += 1
        await q_out.put_end()
    except Exception as e:
        await q_out.put_exception(e)


async def log_exceptions(q_in: IterQueue,
                         q_out: IterQueue,
                         level: str = 'error',
                         drop: bool = False):
    flog = getattr(logger, level)
    try:
        async for x in q_in:
            if is_exception(x):
                flog(x)
                if drop:
                    continue
            await q_out.put(x)
        await q_out.put_end()
    except Exception as e:
        await q_out.put_exception(e)


async def _transform_async(q_in: IterQueue,
                           q_out: IterQueue,
                           func: Callable,
                           workers: int,
                           return_exceptions: bool):
    if workers == 1:
        try:
            async for x in q_in:
                try:
                    z = await func(x)
                    await q_out.put(z)
                except Exception as e:
                    if return_exceptions:
                        await q_out.put(e)
                    else:
                        raise
            await q_out.put_end()
        except Exception as e:
            await q_out.put_exception(e)

        return

    async def _do_compute(in_stream, out_stream, lock, finished):
        while not finished.is_set():
            async with lock:
                if finished.is_set():
                    return
                try:
                    x = await in_stream.__anext__()
                except StopAsyncIteration:
                    finished.set()
                    await out_stream.put_end()
                    return
                except Exception as e:
                    await out_stream.put_exception(e)
                    finished.set()
                    return

                fut = asyncio.Future()
                await out_stream.put(fut)

            try:
                y = await func(x)
                fut.set_result(y)
            except Exception as e:
                if return_exceptions:
                    fut.set_result(e)
                else:
                    fut.cancel()
                    finished.set()
                    await out_stream.put_exception(e)
                    return

    out_buffer_size = workers * 8
    q_fut = IterQueue(out_buffer_size, q_in._to_shutdown)
    lock = asyncio.Lock()
    finished = asyncio.Event()

    _ = [
        asyncio.create_task(_do_compute(
            q_in,
            q_fut,
            lock,
            finished,
        ))
        for _ in range(workers)
    ]

    try:
        async for fut in q_fut:
            z = await fut
            await q_out.put(z)
        await q_out.put_end()
    except Exception as e:
        await q_out.put_exception(e)


async def _transform_sync(q_in: IterQueue,
                          q_out: IterQueue,
                          func: Callable,
                          workers: int,
                          return_exceptions: bool):
    def _put_input_in_queue(q_in, q_out):
        try:
            while True:
                try:
                    x = q_in.get_nowait()
                    if x is NO_MORE_DATA:
                        break
                    q_out.put(x)
                except asyncio.QueueEmpty:
                    time.sleep(0.0013)
            q_out.put_end()
        except Exception as e:
            q_out.put_exception(e)

    def _put_output_in_queue(q_in, q_out):
        _sync_streamer.transform(q_in, q_out, func,
                                 workers=workers,
                                 return_exceptions=return_exceptions)

    q_sync = _sync_streamer.IterQueue(to_shutdown=q_in._to_shutdown)
    q_sync2 = _sync_streamer.IterQueue(to_shutdown=q_in._to_shutdown)
    loop = asyncio.get_running_loop()
    loop.run_in_executor(None, _put_input_in_queue, q_in, q_sync)
    loop.run_in_executor(None, _put_output_in_queue, q_sync, q_sync2)

    try:
        while True:
            try:
                z = q_sync2.get_nowait()
                if z is _sync_streamer.NO_MORE_DATA:
                    break
                await q_out.put(z)
            except queue.Empty:
                await asyncio.sleep(0.0013)
        await q_out.put_end()
    except Exception as e:
        await q_out.put_exception(e)


async def transform(
    q_in: IterQueue,
    q_out: IterQueue,
    func: Callable[[T], Union[TT, Awaitable[TT]]],
    *,
    workers: Optional[Union[int, str]] = None,
    return_exceptions: bool = False,
    **func_args,
) -> None:
    '''Apply a transformation on each element of the data stream,
    producing a stream of corresponding results.

    `func`: a sync or async function that takes a single input item
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

    fun = functools.wraps(func)(functools.partial(func, **func_args))
    if inspect.iscoroutinefunction(func) or (
        not inspect.isfunction(func)
        and hasattr(func, '__call__')
        and inspect.iscoroutinefunction(func.__call__)
    ):
        await _transform_async(q_in, q_out, fun, workers, return_exceptions)
    else:
        await _transform_sync(q_in, q_out, fun, workers, return_exceptions)


async def drain(q_in: IterQueue) -> Union[int, Tuple[int, int]]:
    n = 0
    nexc = 0
    async for v in q_in:
        n += 1
        if is_exception(v):
            nexc += 1
    if nexc:
        return n, nexc
    return n


class Stream(_sync_streamer.StreamMixin):
    @ classmethod
    def registerapi(cls,
                    func: Callable[..., Awaitable[None]],
                    *,
                    name: str = None,
                    maxsize: bool = False,
                    maxsize_first: bool = False,
                    ) -> None:
        '''
        `func`: see the functions `batch`, `drop_if`,
        `transform`, etc for examples.

        User can use this method to register other functions so that they
        can be used as methods of a `Stream` object, just like `batch`,
        `drop_if`, etc.
        '''
        def _internal(maxsize, in_stream, *args, **kwargs):
            q_out = IterQueue(maxsize, in_stream._to_shutdown)
            _ = asyncio.create_task(
                func(in_stream, q_out, *args, **kwargs))
            return cls(q_out)

        if maxsize:
            if maxsize_first:
                @functools.wraps(func)
                def wrapped(self, maxsize: int = None, **kwargs):
                    if maxsize is None:
                        maxsize = self.in_stream.maxsize
                    return _internal(maxsize, self.in_stream, **kwargs)
            else:
                @functools.wraps(func)
                def wrapped(self, *args, maxsize: int = None, **kwargs):
                    if maxsize is None:
                        maxsize = self.in_stream.maxsize
                    return _internal(maxsize, self.in_stream, *args, **kwargs)
        else:
            @functools.wraps(func)
            def wrapped(self, *args, **kwargs):
                return _internal(self.in_stream.maxsize, self.in_stream,
                                 *args, **kwargs)

        setattr(cls, name or func.__name__, wrapped)

    def __init__(self, in_stream: Union[Iterable, AsyncIterable]):
        self.in_stream = stream(in_stream)

    def __anext__(self):
        return self.in_stream.__anext__()

    def __aiter__(self):
        return self.in_stream.__aiter__()

    async def collect(self):
        return [v async for v in self.in_stream]

    async def drain(self):
        return await drain(self.in_stream)


Stream.registerapi(batch, maxsize=True)
Stream.registerapi(unbatch, maxsize=True)
Stream.registerapi(buffer, maxsize=True, maxsize_first=True)
Stream.registerapi(drop_if)
Stream.registerapi(keep_if)
Stream.registerapi(keep_first_n)
Stream.registerapi(peek)
Stream.registerapi(log_exceptions)
Stream.registerapi(transform)
