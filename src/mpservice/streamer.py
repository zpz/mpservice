'''This module provides utilities for processing a continuous stream of data
by one or more I/O-bound operations. The processing happens in an async context.

Reference: https://zpz.github.io/blog/stream-processing/
'''

import asyncio
import inspect
import logging
import multiprocessing
import typing
from collections.abc import AsyncIterable, Iterable, AsyncIterator
from typing import Callable, Awaitable, Any, TypeVar


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


async def stream(x: Iterable) -> AsyncIterator:
    '''Turn a sync iterable into an async iterator.
    '''
    for xx in x:
        yield xx


async def buffer(in_stream: AsyncIterable, buffer_size: int = None) -> AsyncIterator:
    '''Buffer is used to stabilize the speed of data flow in situations
    where each the upstream production or downstream consumption
    have unstable speed.
    '''
    out_stream = asyncio.Queue(maxsize=buffer_size or 1024)

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


async def batch(in_stream: AsyncIterable, batch_size: int) -> AsyncIterator:
    '''Take elements from an input stream,
    and bundle them up into batches up to a size limit,
    and produce the batches in an iterable.

    The output batches are all of the specified size, except
    possibly the final batch.
    There is no 'timeout' logic to produce a smaller batch.
    For efficiency, this requires the input stream to have a steady
    supply.
    If that is a concern, having a `buffer` on the input stream
    may help.
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


async def unbatch(in_stream: AsyncIterable) -> AsyncIterator:
    '''Reverse of "batch", turning a stream of batches into
    a stream of individual elements.
    '''
    async for batch in in_stream:
        for x in batch:
            yield x


# TODO: support sync function.
async def transform(
    in_stream: typing.AsyncIterator[T],
    func: Callable,
    *,
    workers: int = None,
    out_buffer_size: int = None,
    return_exceptions: bool = False,
    **func_args,
) -> AsyncIterator:
    '''Apply a transformation on each element of the data stream,
    producing a stream of corresponding results.

    `func`: an async function that takes a single input item
    as the first positional argument and produces a result.
    Additional keywargs can be passed in via the keyward arguments
    `func_args`.

    The outputs are in the order of the input elements in `in_stream`.

    `workers`: max number of concurrent calls to `func`.
    If <= 1, no concurrency. By default there are multiple.
    Pass in 0 or 1 to enforce single worker. However, since
    the primary use of this function is to achieve concurrency
    with multiple workers, the single-worker case is not optimized
    as much as it can be.
    '''
    if workers is None:
        workers = MAX_THREADS
    if workers < 2:
        workers = 1

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

    if out_buffer_size is None:
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
    in_stream: typing.AsyncIterator[T],
    func: Callable[[T], Awaitable[Any]],
    *,
    workers: int = None,
    out_buffer_size: int = None,
    return_exceptions: bool = False,
    **func_args,
) -> AsyncIterator:
    '''Similar to `transform`, except that elements
    in the output stream are not guaranteed to be
    in the same order as the elements in the input
    stream.
    '''
    if workers is None:
        workers = MAX_THREADS
    assert workers > 1
    # For single worker, there is no point in emphasizing
    # 'unordered', because order will be preserved.

    if out_buffer_size is None:
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
        if isinstance(y, Exception):
            if return_exceptions:
                yield y
            else:
                raise y
        else:
            yield y

    for t in t_workers:
        await t


async def drain(
        in_stream: typing.AsyncIterable[T],
        func: Callable[[T], Awaitable[None]],
        *,
        workers: int = None,
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

    When `workers > 1` (or is `None`),
    order of processing of elements in `in_stream`
    is NOT guaranteed to be the same as the elements' order
    in `in_stream`.
    However, the shuffling of order is local.

    If ordering is desired, then use `transform`.
    '''
    if workers is not None and workers < 2:
        trans = transform(
            in_stream,
            func,
            workers=workers,
            return_exceptions=ignore_exceptions,
            **func_args,
        )
    else:
        trans = unordered_transform(
            in_stream,
            func,
            workers=workers,
            return_exceptions=ignore_exceptions,
            **func_args,
        )

    n = 0
    nn = 0
    async for z in trans:
        if isinstance(z, Exception):
            if ignore_exceptions:
                logger.error(z)
            else:
                raise z
        if log_every:
            n += 1
            nn += 1
            if n == log_every:
                logger.info('drained %d', nn)
                n = 0
    return nn
