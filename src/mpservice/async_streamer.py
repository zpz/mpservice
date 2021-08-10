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

Then use `pipeline` in onf the the following ways:

    async for elem in pipeline:
        ...

    result = await pipeline.collect()

    await pipeline.drain()

Although the primary or initial target use is concurrent I/O-bound
operations, CPU-bound operations could be performed concurrently
in a `mpservice.Server` and registered by `transform`.

Reference for an early version: https://zpz.github.io/blog/stream-processing/

Please refer to the sync counterpart in the module `mpservice.streamer`
for additional info and doc.
'''

from __future__ import annotations
# Enable using a class in type annotations in the code
# that defines that class itself.
# https://stackoverflow.com/a/49872353
# Will no longer be needed in Python 3.10.

import asyncio
import functools
import inspect
import logging
import queue
import threading
import time
from typing import (
    Callable, Awaitable, TypeVar, Optional, Union,
    AsyncIterable, AsyncIterator, Iterable, Iterator,
    Tuple)

from . import streamer as _sync_streamer
from .streamer import is_exception


logger = logging.getLogger(__name__)


T = TypeVar('T')
TT = TypeVar('TT')


class IterQueue(asyncio.Queue):
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

    async def put_end(self):
        assert not self._closed
        await self.put(self.NO_MORE_DATA)
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
                await asyncio.sleep(self.PUT_SLEEP)

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
                await asyncio.sleep(self.GET_SLEEP)

    def get_nowait(self):
        if self.exception is not None:
            raise self.exception
        if self._exhausted:
            return self.NO_MORE_DATA
        z = super().get_nowait()
        if z is self.NO_MORE_DATA:
            self._exhausted = True
        return z

    async def __anext__(self):
        while True:
            z = await self.get()
            if z is self.NO_MORE_DATA:
                raise StopAsyncIteration
            return z

    def __aiter__(self):
        return self


def streamer_task(q_in: AsyncIterable, q_out: IterQueue,
                  func: Callable[..., Awaitable[None]],
                  *args, **kwargs) -> asyncio.Task:
    async def foo(q_in, q_out, func, *args, **kwargs):
        try:
            await func(q_in, q_out, *args, **kwargs)
            await q_out.put_end()
        except Exception as e:
            await q_out.put_exception(e)

    return asyncio.create_task(
        foo(q_in, q_out, func, *args, **kwargs))


def stream(x: Union[Iterable, AsyncIterable, Iterator, AsyncIterator],
           maxsize: int = None) -> IterQueue:
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

    async def _enqueue(q_in, q_out):
        async for v in q_in:
            await q_out.put(v)

    q_out = IterQueue(maxsize=maxsize)
    _ = streamer_task(x, q_out, _enqueue)
    return q_out


async def batch(q_in: IterQueue, q_out: IterQueue, batch_size: int) -> None:
    assert 0 < batch_size <= 10000
    batch_ = []
    n = 0
    async for x in q_in:
        batch_.append(x)
        n += 1
        if n >= batch_size:
            await q_out.put(batch_)
            batch_ = []
            n = 0
    if n:
        await q_out.put(batch_)


async def unbatch(q_in: IterQueue, q_out: IterQueue) -> None:
    async for batch in q_in:
        for x in batch:
            await q_out.put(x)


async def buffer(q_in: IterQueue, q_out: IterQueue) -> None:
    async for x in q_in:
        await q_out.put(x)


async def drop_if(q_in: IterQueue, q_out: IterQueue,
                  func: Callable[[int, T], bool]) -> None:
    n = 0
    async for x in q_in:
        if func(n, x):
            n += 1
            continue
        await q_out.put(x)
        n += 1


async def keep_if(q_in: IterQueue, q_out: IterQueue,
                  func: Callable[[int, T], bool]) -> None:
    n = 0
    async for x in q_in:
        if func(n, x):
            await q_out.put(x)
        n += 1


async def keep_first_n(q_in: IterQueue, q_out: IterQueue, n: int) -> None:
    assert n > 0
    k = 0
    async for x in q_in:
        await q_out.put(x)
        k += 1
        if k >= n:
            break


async def peek(q_in: IterQueue, q_out: IterQueue,
               peek_func: Callable[[int, T], None] = None) -> None:
    if peek_func is None:
        peek_func = _sync_streamer._default_peek_func

    n = 0
    async for x in q_in:
        peek_func(n, x)
        await q_out.put(x)
        n += 1


async def log_exceptions(q_in: IterQueue, q_out: IterQueue,
                         level: str = 'error', drop: bool = False) -> None:
    flog = getattr(logger, level)
    async for x in q_in:
        if is_exception(x):
            flog(x)
            if drop:
                continue
        await q_out.put(x)


async def _transform_async(q_in: IterQueue,
                           q_out: IterQueue,
                           func: Callable,
                           workers: int,
                           return_exceptions: bool):
    if workers == 1:
        async for x in q_in:
            try:
                z = await func(x)
                await q_out.put(z)
            except Exception as e:
                if return_exceptions:
                    await q_out.put(e)
                else:
                    raise
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

    q_fut = IterQueue(workers * 8, q_in)
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

    async for fut in q_fut:
        z = await fut
        await q_out.put(z)


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
                    if x is q_in.NO_MORE_DATA:
                        break
                    q_out.put(x)
                except asyncio.QueueEmpty:
                    time.sleep(q_in.GET_SLEEP)
            q_out.put_end()
        except Exception as e:
            q_out.put_exception(e)

    def _put_output_in_queue(q_in, q_out):
        try:
            _sync_streamer.transform(q_in, q_out, func,
                                     workers=workers,
                                     return_exceptions=return_exceptions)
            q_out.put_end()
        except Exception as e:
            q_out.put_exception(e)

    q_in_out = _sync_streamer.IterQueue(upstream=q_in)
    q_out_in = _sync_streamer.IterQueue(upstream=q_in_out)
    loop = asyncio.get_running_loop()
    loop.run_in_executor(None, _put_input_in_queue, q_in, q_in_out)
    loop.run_in_executor(None, _put_output_in_queue, q_in_out, q_out_in)

    while True:
        try:
            z = q_out_in.get_nowait()
            if z is q_out_in.NO_MORE_DATA:
                break
            await q_out.put(z)
        except queue.Empty:
            await asyncio.sleep(q_out.GET_SLEEP)


async def transform(
        q_in: IterQueue,
        q_out: IterQueue,
        func: Callable[[T], Union[TT, Awaitable[TT]]],
        *,
        workers: Optional[Union[int, str]] = None,
        return_exceptions: bool = False,
        **func_args) -> None:
    '''Apply a transformation on each element of the data stream,
    producing a stream of corresponding results.

    `func`: a sync or async function that takes a single input item
    as the first positional argument and produces a result.
    Additional keyword args can be passed in via `func_args`.

    The outputs are in the order of the input elements in `in_stream`.
    '''
    if workers is None:
        workers = 1
    elif isinstance(workers, str):
        assert workers == 'max'
        workers = _sync_streamer.MAX_THREADS
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
            q_out = IterQueue(maxsize, in_stream)
            _ = streamer_task(in_stream, q_out, func, *args, **kwargs)
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

    def __init__(self, in_stream, *, maxsize: int = None):
        self.in_stream = stream(in_stream, maxsize=maxsize)

    def __anext__(self):
        return self.in_stream.__anext__()

    def __aiter__(self):
        return self.in_stream.__aiter__()

    async def collect(self) -> list:
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
