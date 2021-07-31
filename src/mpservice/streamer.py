'''Utilities for processing a continuous stream of data in an async context.

There is an input stream, which is an AsyncIterator.
This stream goes through a series of async operations, each of which
takes an AsyncIterator, and returns another AsyncIterator.
Thanks to this consistency of input/output, the operations can be "chained".

If the input is not an AsyncIterator, but rather some other (sync or async)
iterable, then the function `stream` will turn it into an AsyncIterator.

The target use case is that one or more operations is I/O bound,
hence can benefit from async or multi-thread concurrency.
Concurrency is enabled by `transform`, `unordered_transform`, and `drain`.

There are two ways to use these utilities. In the first way, one calls the
operation functions in sequence, e.g.

    data = range(100)
    data_stream = stream(data)
    result = transform(
        batch(data_stream, 10),
        my_op_that_takes_stream_of_batches)
    result = [_ async for _ in unbatch(result))]

In the second way, one starts with a `Stream` object, and calls
its methods in a "chained" fashion:

    data = range(100)
    result = (
        Stream(data)
        .batch(10)
        .transform(my_op_that_takes_stream_of_batches)
        .unbatch()
        .collect()
        )
    result = await result

(Of course, you don't have to call all the methods in one statement.)

Reference for an early version: https://zpz.github.io/blog/stream-processing/
'''

import asyncio
import functools
import inspect
import logging
import multiprocessing
import random
from typing import (
    Callable, Awaitable, Any, TypeVar, Optional, Union,
    AsyncIterable, AsyncIterator, Iterable)


MAX_THREADS = min(32, multiprocessing.cpu_count() + 4)
# This default is suitable for I/O bound operations.
# For others, user may want to specify a smaller value.


logger = logging.getLogger(__name__)


NO_MORE_DATA = object()

T = TypeVar('T')

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


def _is_exc(e):
    assert isinstance(e, Exception) or (
        inspect.isclass(e) and issubclass(e, Exception))


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


async def batch(in_stream: AsyncIterable[T],
                batch_size: int) -> AsyncIterator[T]:
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
    '''Reverse of "batch", turning a stream of batches into
    a stream of individual elements.
    '''
    async for batch in in_stream:
        for x in batch:
            yield x


async def buffer(in_stream: AsyncIterable[T],
                 buffer_size: int = None) -> AsyncIterator[T]:
    '''Buffer is used to stabilize and improve the speed of data flow.

    A buffer is useful after any operation that can not guarantee
    (almost) instant availability of output. A buffer allows its
    output to "pile up" when the downstream consumer is slow in requests,
    so that data *is* available when the downstream does come to request
    data. The buffer evens out unstabilities in the speeds of upstream
    production and downstream consumption.
    '''
    out_stream = asyncio.Queue(maxsize=buffer_size or 256)

    async def buff(in_stream, out_stream):
        async for x in in_stream:
            await out_stream.put(x)
        await out_stream.put(NO_MORE_DATA)

    t = asyncio.create_task(buff(in_stream, out_stream))

    while True:
        if t.done() and t.exception() is not None:
            raise t.exception()
        x = await out_stream.get()
        if x is NO_MORE_DATA:
            break
        yield x

    await t


async def drop_if(in_stream: AsyncIterable[T],
                  func: Callable[[T], bool]) -> AsyncIterator[T]:
    async for x in in_stream:
        if func(x):
            continue
        yield x


async def drop_exceptions(in_stream: AsyncIterable[T]) -> AsyncIterator[T]:
    async for x in in_stream:
        if _is_exc(x):
            continue
        yield x


async def keep_if(in_stream: AsyncIterable[T],
                  func: Callable[[T], bool]) -> AsyncIterator[T]:
    async for x in in_stream:
        if func(x):
            yield x


# TODO: support sync function.
async def transform(
    in_stream: AsyncIterator[T],
    func: Callable[[T], Awaitable[None]],
    *,
    workers: Optional[Union[int, str]] = None,
    return_exceptions: bool = False,
    **func_args,
) -> AsyncIterator[T]:
    '''Apply a transformation on each element of the data stream,
    producing a stream of corresponding results.

    `func`: an async function that takes a single input item
    as the first positional argument and produces a result.
    Additional keywargs can be passed in via the keyward arguments
    `func_args`.

    The outputs are in the order of the input elements in `in_stream`.

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

    if workers == 1:
        async for x in in_stream:
            try:
                z = await func(x, **func_args
                               )
                yield z
            except Exception as e:
                if return_exceptions:
                    yield e
                else:
                    raise e
        return

    finished = False

    async def _process(in_stream, lock, out_stream, func, **kwargs):
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
                    await out_stream.put(NO_MORE_DATA)
                    return
                except Exception as e:
                    fut = asyncio.Future()
                    await out_stream.put(fut)
                    if inspect.isclass(e):
                        e = e()
                    fut.set_exception(e)
                    continue

            try:
                y = await func(x, **kwargs)
                fut.set_result(y)
            except Exception as e:
                if inspect.isclass(e):
                    e = e()
                fut.set_exception(e)

    out_buffer_size = workers * 8
    out_stream = asyncio.Queue(out_buffer_size)
    lock = asyncio.Lock()

    t_workers = [
        asyncio.create_task(_process(
            in_stream,
            lock,
            out_stream,
            func,
            **func_args,
        ))
        for _ in range(workers)
    ]

    while True:
        fut = await out_stream.get()
        if fut is NO_MORE_DATA:
            break
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


async def unordered_transform(
    in_stream: AsyncIterator[T],
    func: Callable[[T], Awaitable[Any]],
    *,
    workers: Optional[Union[int, str]] = None,
    return_exceptions: bool = False,
    **func_args,
) -> AsyncIterator[T]:
    '''Similar to `transform`, except that elements
    in the output stream are not guaranteed to be
    in the same order as the elements in the input
    stream.

    This function is useful when the time for processing a
    single data element varies a lot. The effect of this
    "eager" mode is such that a slow processing does not block
    the production of results by other faster workers.
    '''
    if workers is None:
        workers = MAX_THREADS
    elif isinstance(workers, str):
        assert workers == 'max'
        workers = MAX_THREADS
    else:
        assert workers > 1
        # For single worker, there is no point in emphasizing
        # 'unordered', because order will be preserved.

    out_buffer_size = workers * 8
    out_stream = asyncio.Queue(out_buffer_size)
    finished = False
    lock = asyncio.Lock()
    n_active_workers = workers

    async def _process(in_stream, lock, out_stream, func, **kwargs):
        nonlocal finished
        while not finished:
            error = None
            async with lock:
                if finished:
                    break
                try:
                    x = await in_stream.__anext__()
                except StopAsyncIteration:
                    finished = True
                    break
                except Exception as e:
                    if inspect.isclass(e):
                        e = e()
                    error = e

            if error is not None:
                y = error
            else:
                try:
                    y = await func(x, **kwargs)
                except Exception as e:
                    if inspect.isclass(e):
                        e = e()
                    y = e
            await out_stream.put(y)

        nonlocal n_active_workers
        n_active_workers -= 1
        if n_active_workers == 0:
            await out_stream.put(NO_MORE_DATA)

    t_workers = [
        asyncio.create_task(_process(
            in_stream, lock, out_stream,
            func, **func_args,
        ))
        for _ in range(workers)
    ]

    while True:
        y = await out_stream.get()
        if y is NO_MORE_DATA:
            break
        if _is_exc(y):
            if return_exceptions:
                yield y
            else:
                raise y
        else:
            yield y

    for t in t_workers:
        await t


async def drain(
        in_stream: AsyncIterable[T],
        func: Callable[[T], Awaitable[None]] = None,
        *,
        workers: Optional[Union[int, str]] = None,
        log_every: int = 1000,
        ignore_exceptions: bool = False,
        **func_args,
) -> int:
    '''
    `func`: an async function that takes a single input item
    but does not produce (useful) return.
    Example operation of `func`: insert into DB.
    Additional arguments can be passed in via `func_args`.

    Return number of elements processed.

    While input is processed in the order they come in `in_stream`,
    there is no way to guarantee they *finish* in the same order,
    unless `workers` is 1, in which case the data are processed
    sequentially.
    '''
    if func is None:
        n = 0
        nn = 0
        async for z in in_stream:
            nn += 1
            if log_every:
                n += 1
                if n == log_every:
                    logger.info('drained %d', nn)
                    n = 0
        return nn

    if workers is None:
        workers = 1
    elif isinstance(workers, str):
        assert workers == 'max'
        workers = MAX_THREADS
    else:
        workers > 0

    return await drain(
        transform(
            in_stream,
            func,
            workers=workers,
            return_exceptions=ignore_exceptions,
            **func_args,
        ),
        log_every=log_every,
        ignore_exceptions=ignore_exceptions,
    )


async def peek_randomly(in_stream: AsyncIterable[T],
                        frac: float,
                        func: Callable[[int, T], None] = None,
                        ) -> AsyncIterator[T]:
    assert 0 < frac <= 1
    rand = random.random

    if func is None:
        def func(n, x):
            print('#', n)
            print(x)

    n = 0
    async for x in in_stream:
        n += 1
        if rand() < frac:
            func(n, x)
        yield x


async def peek_regularly(in_stream: AsyncIterable[T],
                         kth: int,
                         func: Callable[[int, T], None] = None,
                         ) -> AsyncIterator[T]:
    '''Take a peek at the data at regular intervals.

    `func` usually prints out info of the data element,
    but can save it to a file or does other things.
    The function should not modify the data element.
    '''
    assert kth > 0

    if func is None:
        def func(n, x):
            print('#', n)
            print(x)

    n = 0
    k = 0
    async for x in in_stream:
        n += 1
        k += 1
        if k == kth:
            func(n, x)
            k = 0
        yield x


async def sample_randomly(in_stream: AsyncIterable[T], frac: float) -> AsyncIterator[T]:
    assert 0 < frac <= 1
    rand = random.random
    async for x in in_stream:
        if rand() < frac:
            yield x


async def sample_regularly(in_stream: AsyncIterable[T], kth: int) -> AsyncIterator[T]:
    assert kth > 0
    k = 0
    async for x in in_stream:
        k += 1
        if k == kth:
            yield x
            k = 0


class Stream:
    def __init__(self, in_stream: Union[Iterable[T], AsyncIterable[T]]):
        self.in_stream = stream(in_stream)

    def __anext__(self):
        return self.in_stream.__anext__()

    def __aiter__(self):
        return self.in_stream.__aiter__()

    @classmethod
    def registerapi(cls, func: Callable, name=None):
        if not name:
            name = func.__name__

        @functools.wraps(func)
        def wrapped(self, *args, **kwargs):
            return cls(func(self.in_stream, *args, **kwargs))

        setattr(cls, name, wrapped)

    def drain(self, func=None, *,
              workers=None, log_every=1000,
              ignore_exceptions=False, **func_args):
        return drain(
            self.in_stream, func,
            workers=workers, log_every=log_every,
            ignore_exceptions=ignore_exceptions, **func_args)

    async def collect(self) -> list:
        return [_ async for _ in self.in_stream]


Stream.registerapi(batch)
Stream.registerapi(unbatch)
Stream.registerapi(buffer)
Stream.registerapi(drop_if)
Stream.registerapi(drop_exceptions)
Stream.registerapi(keep_if)
Stream.registerapi(transform)
Stream.registerapi(unordered_transform)
Stream.registerapi(peek_randomly)
Stream.registerapi(peek_regularly)
Stream.registerapi(sample_randomly)
Stream.registerapi(sample_regularly)
