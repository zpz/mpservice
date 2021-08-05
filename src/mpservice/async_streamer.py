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

In the recommended usage,
one starts with a `Stream` object and calls its methods in a "chained" fashion:

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
from typing import (
    Callable, Awaitable, TypeVar, Optional, Union,
    AsyncIterable, AsyncIterator, Iterable,
    Tuple, List)

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

    def __init__(self, maxsize: int = None):
        super().__init__(maxsize or self.DEFAULT_MAXSIZE)
        self.exception = None
        self._closed = False
        self._exhausted = False

    async def put_end(self):
        assert not self._closed
        await self.put(NO_MORE_DATA)
        self._closed = True

    async def put_exception(self, e):
        assert not self._closed
        self.exception = e

    async def put(self, x, **kwargs):
        assert not self._closed
        await super().put(x, **kwargs)

    def put_nowait(self, x):
        assert not self._closed
        super().put_nowait(x)

    async def get(self, **kwargs):
        if self.exception is not None:
            raise self.exception
        if self._exhausted:
            return NO_MORE_DATA
        # if self._closed and self.empty():
        #     self._exhausted = True
        #     return NO_MORE_DATA
        z = await super().get(**kwargs)
        if z is NO_MORE_DATA:
            self._exhausted = True
        return z

    def get_nowait(self):
        if self.exception is not None:
            raise self.exception
        if self._exhausted:
            return NO_MORE_DATA
        # if self._closed and self.empty():
        #     self._exhausted = True
        #     return NO_MORE_DATA
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


def stream(x: Union[Iterable[T], AsyncIterable[T]]) -> AsyncIterator[T]:
    '''Turn a sync iterable into an async iterator.
    However, user should try to provide a natively async iterable
    if possible.

    The returned object has both `__anext__` and `__aiter__`
    methods.
    '''
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
        if hasattr(x, '__anext__'):
            return x
        else:
            return f1(x)
    if hasattr(x, '__anext__'):
        return f2(x)
    if hasattr(x, '__iter__'):
        return f3(x)
    if hasattr(x, '__next__'):
        return f4(x)

    raise TypeError("`x` is neither iterable or async iterable")


async def collect(in_stream: AsyncIterable[T]) -> List[T]:
    return [_ async for _ in in_stream]


async def batch(in_stream: AsyncIterable[T],
                batch_size: int) -> AsyncIterator[List[T]]:

    assert 0 < batch_size <= 10000
    batch_ = []
    n = 0
    async for x in in_stream:
        batch_.append(x)
        n += 1
        if n >= batch_size:
            yield batch_
            batch_ = []
            n = 0
    if n:
        yield batch_


async def unbatch(in_stream: AsyncIterable[Iterable[T]]) -> AsyncIterator[T]:

    async for batch in in_stream:
        for x in batch:
            yield x


async def buffer(in_stream: AsyncIterable[T],
                 buffer_size: int = None) -> AsyncIterator[T]:

    out_stream = IterQueue(buffer_size)

    async def buff(in_stream, out_stream):
        try:
            async for x in in_stream:
                await out_stream.put(x)
        except Exception as e:
            await out_stream.put_exception(e)
        await out_stream.put_end()

    t = asyncio.create_task(buff(in_stream, out_stream))

    async for x in out_stream:
        yield x

    await t


async def drop_if(in_stream: AsyncIterable[T],
                  func: Callable[[int, T], bool]) -> AsyncIterator[T]:
    n = 0
    async for x in in_stream:
        if func(n, x):
            n += 1
            continue
        yield x
        n += 1


async def keep_if(in_stream: AsyncIterable[T],
                  func: Callable[[int, T], bool]) -> AsyncIterator[T]:
    n = 0
    async for x in in_stream:
        if func(n, x):
            yield x
        n += 1


async def keep_first_n(in_stream, n: int):
    assert n > 0
    k = 0
    async for x in in_stream:
        yield x
        k += 1
        if k >= n:
            break


async def peek(in_stream: AsyncIterable[T],
               peek_func: Callable[[int, T], None] = None,
               ) -> AsyncIterator[T]:
    if peek_func is None:
        peek_func = _sync_streamer._default_peek_func

    n = 0
    async for x in in_stream:
        peek_func(n, x)
        yield x
        n += 1


async def log_exceptions(in_stream: AsyncIterable,
                         level: str = 'error',
                         drop: bool = False):
    flog = getattr(logger, level)
    async for x in in_stream:
        if is_exception(x):
            flog(x)
            if drop:
                continue
        yield x


async def _transform_async(in_stream, func, workers, return_exceptions):
    if workers == 1:
        async for x in in_stream:
            try:
                z = await func(x)
                yield z
            except Exception as e:
                if return_exceptions:
                    yield e
                else:
                    raise e
        return

    finished = False

    async def _process(in_stream, out_stream, lock):
        nonlocal finished
        while not finished:
            async with lock:
                if finished:
                    return
                try:
                    x = await in_stream.__anext__()
                    fut = asyncio.Future()
                    await out_stream.put(fut)
                except StopAsyncIteration:
                    finished = True
                    await out_stream.put_end()
                    return
                except Exception as e:
                    if return_exceptions:
                        await out_stream.put(e)
                        continue
                    else:
                        finished = True
                        await out_stream.put_exception(e)
                        # No need to process subsequent data.
                        await out_stream.put_end()
                        return

            try:
                y = await func(x)
                fut.set_result(y)
            except Exception as e:
                fut.set_exception(e)
                if not return_exceptions:
                    finished = True
                    await out_stream.put_end()
                    return

    out_buffer_size = workers * 8
    out_stream = IterQueue(out_buffer_size)
    lock = asyncio.Lock()

    t_workers = [
        asyncio.create_task(_process(
            in_stream,
            out_stream,
            lock,
        ))
        for _ in range(workers)
    ]

    async for fut in out_stream:
        if is_exception(fut):
            if return_exceptions:
                yield fut
            else:
                raise fut
        else:
            try:
                z = await fut
                yield z
            except Exception as e:
                if return_exceptions:
                    yield e
                else:
                    raise e

    for t in t_workers:
        await t


async def _transform_sync(in_stream, func, workers, return_exceptions):
    q_in = _sync_streamer.IterQueue()
    q_out = _sync_streamer.IterQueue()

    async def _put_input_in_queue():
        try:
            async for x in in_stream:
                while True:
                    try:
                        q_in.put_nowait(x)
                        break
                    except queue.Full:
                        await asyncio.sleep(0.002)
            q_in.put_end()
        except Exception as e:
            q_in.put_exception(e)

    def _put_output_in_queue(q_in, q_out, func):
        _sync_streamer.transform(q_in, q_out, func,
                                 workers=workers,
                                 return_exceptions=return_exceptions)

    t_in = asyncio.create_task(_put_input_in_queue())
    t_out = asyncio.get_running_loop().run_in_executor(
        None, _put_output_in_queue, q_in, q_out, func)

    while True:
        try:
            z = q_out.get_nowait()
            if z is _sync_streamer.NO_MORE_DATA:
                break
            yield z
        except queue.Empty:
            await asyncio.sleep(0.0023)

    await t_in
    await t_out


def transform(
    in_stream: AsyncIterator[T],
    func: Callable[[T], Union[TT, Awaitable[TT]]],
    *,
    workers: Optional[Union[int, str]] = None,
    return_exceptions: bool = False,
    **func_args,
) -> AsyncIterator[TT]:
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

    fun = functools.partial(func, **func_args)
    if inspect.iscoroutinefunction(func) or (
        not inspect.isfunction(func)
        and hasattr(func, '__call__')
        and inspect.iscoroutinefunction(func.__call__)
    ):
        return _transform_async(in_stream, fun, workers, return_exceptions)
    else:
        return _transform_sync(in_stream, fun, workers, return_exceptions)


async def drain(in_stream: AsyncIterable) -> Union[int, Tuple[int, int]]:
    n = 0
    nexc = 0
    async for v in in_stream:
        n += 1
        if is_exception(v):
            nexc += 1
    if nexc:
        return n, nexc
    return n


class Stream(_sync_streamer.StreamMixin):
    @ classmethod
    def registerapi(cls,
                    func: Callable[..., AsyncIterator],
                    *,
                    name: str = None) -> None:
        '''
        `func` expects the data stream, an AsyncIterable or AsyncIterator,
        as the first positional argument. It may take additional positional
        and keyword arguments. See the functions `batch`, `drop_if`,
        `transform`, etc for examples.

        User can use this method to register other functions so that they
        can be used as methods of a `Stream` object, just like `batch`,
        `drop_if`, etc.
        '''
        if not name:
            name = func.__name__

        @ functools.wraps(func)
        def wrapped(self, *args, **kwargs):
            return cls(func(self.in_stream, *args, **kwargs))

        setattr(cls, name, wrapped)

    def __init__(self, in_stream: Union[Iterable, AsyncIterable]):
        self.in_stream = stream(in_stream)

    def __anext__(self):
        return self.in_stream.__anext__()

    def __aiter__(self):
        return self.in_stream.__aiter__()

    def collect(self):
        return collect(self.in_stream)

    def drain(self):
        return drain(self.in_stream)


Stream.registerapi(batch)
Stream.registerapi(unbatch)
Stream.registerapi(buffer)
Stream.registerapi(drop_if)
Stream.registerapi(keep_if)
Stream.registerapi(keep_first_n)
Stream.registerapi(peek)
Stream.registerapi(log_exceptions)
Stream.registerapi(transform)
