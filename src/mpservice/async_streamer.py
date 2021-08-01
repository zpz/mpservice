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

In most cases, the second way is recommended.

Reference for an early version: https://zpz.github.io/blog/stream-processing/
'''

import asyncio
import functools
import inspect
import logging
import multiprocessing
import random
from typing import (
    Callable, Awaitable, TypeVar, Optional, Union,
    AsyncIterable, AsyncIterator, Iterable,
    Tuple, List)


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


def _is_exc(e):
    return isinstance(e, Exception) or (
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


async def collect(in_stream: AsyncIterable[T]) -> List[T]:
    return [_ async for _ in in_stream]


async def batch(in_stream: AsyncIterable[T],
                batch_size: int) -> AsyncIterator[List[T]]:
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
                  func: Callable[[int, T], bool]) -> AsyncIterator[T]:
    n = 0
    async for x in in_stream:
        if func(n, x):
            n += 1
            continue
        yield x
        n += 1


def drop_exceptions(in_stream: AsyncIterable[T]) -> AsyncIterator[T]:
    return drop_if(in_stream, lambda i, x: _is_exc(x))


def drop_first_n(in_stream, n: int):
    assert n >= 0
    if n == 0:
        return in_stream
    return drop_if(in_stream, lambda i, x: i < n)


async def keep_if(in_stream: AsyncIterable[T],
                  func: Callable[[int, T], bool]) -> AsyncIterator[T]:
    n = 0
    async for x in in_stream:
        if func(n, x):
            yield x
        n += 1


def keep_every_nth(in_stream, nth: int):
    assert nth > 0
    return keep_if(in_stream, lambda i, x: i % nth == 0)


def keep_random(in_stream, frac: float):
    assert 0 < frac <= 1
    rand = random.random
    return keep_if(in_stream, lambda i, x: rand() < frac)


async def keep_first_n(in_stream, n: int):
    assert n > 0
    k = 0
    async for x in in_stream:
        yield x
        k += 1
        if k >= n:
            break


def _default_peek_func(i, x):
    print('')
    print('#', i)
    print(x)


async def peek_if(in_stream: AsyncIterable[T],
                  condition_func: Callable[[int, T], bool],
                  peek_func: Callable[[int, T], None] = _default_peek_func,
                  ) -> AsyncIterator[T]:
    '''Take a peek at the data elements that statisfy the specified condition.

    `peek_func` usually prints out info of the data element,
    but can save it to a file or does other things. This happens *before*
    the element is sent downstream.

    The peek function usually should not modify the data element.
    '''
    n = 0
    async for x in in_stream:
        if condition_func(n, x):
            peek_func(n, x)
        yield x
        n += 1


def peek_every_nth(in_stream, nth: int, peek_func=_default_peek_func):
    return peek_if(in_stream, lambda i, x: i % nth == 0, peek_func)


def peek_random(in_stream, frac: float, peek_func=_default_peek_func):
    assert 0 < frac <= 1
    rand = random.random
    return peek_if(in_stream, lambda i, x: rand() < frac, peek_func)


def log_every_nth(in_stream, nth: int):
    def peek_func(i, x):
        logger.info('data item #%d:  %s', i, x)

    return peek_every_nth(in_stream, nth, peek_func)


# TODO: support sync function.
async def transform(
    in_stream: AsyncIterator[T],
    func: Callable[[T], Awaitable[TT]],
    *,
    workers: Optional[Union[int, str]] = None,
    return_exceptions: bool = False,
    **func_args,
) -> AsyncIterator[TT]:
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


async def drain(in_stream: AsyncIterable,
                log_nth: int = 0) -> Union[int, Tuple[int, int]]:
    '''Drain off the stream and the number of elements processed.
    '''
    if log_nth:
        in_stream = log_every_nth(in_stream, log_nth)
    n = 0
    nexc = 0
    async for v in in_stream:
        n += 1
        if _is_exc(v):
            nexc += 1
    if nexc:
        return n, nexc
    return n


class Stream:
    @classmethod
    def registerapi(cls,
                    func: Callable[..., AsyncIterator],
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

        @functools.wraps(func)
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

    def drain(self, log_nth=0):
        return drain(self.in_stream, log_nth)


Stream.registerapi(batch)
Stream.registerapi(unbatch)
Stream.registerapi(buffer)
Stream.registerapi(drop_if)
Stream.registerapi(drop_exceptions)
Stream.registerapi(drop_first_n)
Stream.registerapi(keep_if)
Stream.registerapi(keep_every_nth)
Stream.registerapi(keep_random)
Stream.registerapi(keep_first_n)
Stream.registerapi(peek_if)
Stream.registerapi(peek_every_nth)
Stream.registerapi(peek_random)
Stream.registerapi(log_every_nth)
Stream.registerapi(transform)
