'''Utilities for processing a continuous stream of data in an async context.

The target use case is that one or more operations is I/O bound,
hence can benefit from async or multi-thread concurrency.
These operations (which are sync or async functions) are triggered
via `transform`.

In a typical use, one starts with a `Stream` object and calls its methods
in a "chained" fashion:

    data = range(100)
    pipeline = (
        Stream(data)
        .batch(10)
        .transform(my_op_that_takes_a_batch, workers=4)
        .unbatch()
        )

Then use `pipeline` in on of the following ways:

    async for elem in pipeline:
        ...

    result = await pipeline.collect()

    await pipeline.drain()

Please refer to the sync counterpart in the module `mpservice.streamer`
for additional info and doc.

Reference (for an early version of the code): https://zpz.github.io/blog/stream-processing/
'''

from __future__ import annotations
# Enable using a class in type annotations in the code
# that defines that class itself.
# https://stackoverflow.com/a/49872353
# Will no longer be needed in Python 3.10.

import asyncio
import collections.abc
import functools
import inspect
import logging
import queue
import threading
from typing import (
    Awaitable, Callable, TypeVar, Optional, Union,
    Iterable, Iterator,
    Tuple, Type)

from . import _streamer as _sync_streamer
from ._streamer import is_exception, _default_peek_func, MAX_THREADS


logger = logging.getLogger(__name__)


T = TypeVar('T')
TT = TypeVar('TT')


class Stream(collections.abc.AsyncIterator, _sync_streamer.StreamMixin):
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
            if hasattr(instream, '__anext__'):
                self._instream = instream
            else:
                if hasattr(instream, '__aiter__'):

                    async def foo(instream):
                        async for x in instream:
                            yield x

                    self._instream = foo(instream)
                    # TODO: if `.head()` is called on the current object,
                    # which stops the iteration before the stream is exhausted,
                    # it might cause the "Task was destroyed but it is pending"
                    # warning. How can we get a `__anext__` method given
                    # that the input is an oject that has `__aiter__` but
                    # not `__anext__`?
                else:
                    if not hasattr(instream, '__next__'):
                        assert hasattr(instream, '__iter__')
                        instream = iter(instream)

                    class MyStream:
                        async def __anext__(self):
                            try:
                                return next(instream)
                            except StopIteration:
                                raise StopAsyncIteration

                    self._instream = MyStream()

        self.index = 0
        # Index of the upcoming element; 0 based.
        # This is also the count of finished elements.

    def __aiter__(self):
        return self

    async def _get_next(self):
        return await self._instream.__anext__()

    async def __anext__(self):
        try:
            z = await self._get_next()
            self.index += 1
            return z
        except StopAsyncIteration:
            raise
        except:
            self._to_shutdown.set()
            raise

    async def collect(self) -> list:
        return [v async for v in self]

    async def drain(self) -> Union[int, Tuple[int, int]]:
        n = 0
        nexc = 0
        async for v in self:
            n += 1
            if is_exception(v):
                nexc += 1
        if nexc:
            return n, nexc
        return n

    def batch(self, batch_size: int) -> Batcher:
        return Batcher(self, batch_size)

    def unbatch(self) -> Unbatcher:
        return Unbatcher(self)

    def drop_if(self, func: Callable[[int, T], bool]) -> Dropper:
        return Dropper(self, func)

    def head(self, n: int) -> Head:
        return Head(self, n)

    def peek(self, func: Callable[[int, T], None] = None) -> Peeker:
        if func is None:
            func = _default_peek_func
        return Peeker(self, func)

    def buffer(self, maxsize: int = None) -> Buffer:
        if maxsize is None:
            maxsize = 256
        else:
            assert 1 <= maxsize <= 10_000
        return Buffer(self, maxsize)

    def transform(self,
                  func: Callable[[T], Union[TT, Awaitable[TT]]],
                  *,
                  workers: Optional[Union[int, str]] = None,
                  return_exceptions: bool = False,
                  **func_args) -> Union[Transformer, ConcurrentTransformer]:
        '''
        When `workers = N > 0`, the worker threads (if `func` is sync)
        or tasks (if `func` is async) are named 'transformer-0',
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

    async def _get_next(self):
        if self._done:
            raise StopAsyncIteration
        batch = []
        for _ in range(self.batch_size):
            try:
                batch.append(await self._instream.__anext__())
            except StopAsyncIteration:
                self._done = True
                break
        if batch:
            return batch
        raise StopAsyncIteration


class Unbatcher(Stream):
    def __init__(self, instream: Stream):
        super().__init__(instream)
        self._batch = None

    async def _get_next(self):
        if self._batch:
            return self._batch.pop(0)
        self._batch = await self._instream.__anext__()
        return await self._get_next()


class Dropper(Stream):
    def __init__(self, instream: Stream, func: Callable[[int, T], bool]):
        super().__init__(instream)
        self.func = func

    async def _get_next(self):
        while True:
            z = await self._instream.__anext__()
            if self.func(self.index, z):
                self.index += 1
                continue
            return z


class Head(Stream):
    def __init__(self, instream: Stream, n: int):
        super().__init__(instream)
        assert n >= 0
        self.n = n

    async def _get_next(self):
        if self.index >= self.n:
            raise StopAsyncIteration
        return await self._instream.__anext__()


class Peeker(Stream):
    def __init__(self, instream: Stream, func: Callable[[int, T], None]):
        super().__init__(instream)
        self.func = func

    async def _get_next(self):
        z = await self._instream.__anext__()
        self.func(self.index, z)
        return z


class IterQueue(asyncio.Queue, collections.abc.AsyncIterator):
    GET_SLEEP = 0.00056
    PUT_SLEEP = 0.00045
    NO_MORE_DATA = object()

    def __init__(self, maxsize: int, to_shutdown: threading.Event):
        super().__init__(maxsize)
        self._to_shutdown = to_shutdown

    async def put_end(self):
        await self.put(self.NO_MORE_DATA)

    async def put(self, x):
        while True:
            try:
                super().put_nowait(x)
                break
            except asyncio.QueueFull:
                if self._to_shutdown.is_set():
                    return
                await asyncio.sleep(self.PUT_SLEEP)

    async def __anext__(self):
        while True:
            try:
                z = self.get_nowait()
                if z is self.NO_MORE_DATA:
                    raise StopAsyncIteration
                return z
            except asyncio.QueueEmpty:
                await asyncio.sleep(self.GET_SLEEP)


class Buffer(Stream):
    def __init__(self, instream: Stream, maxsize: int):
        super().__init__(instream)
        assert 1 <= maxsize <= 10_000
        self.maxsize = maxsize
        self._q = IterQueue(maxsize, self._to_shutdown)
        self._err = None
        self._task = None
        self._start()

    def _start(self):
        async def foo(instream, q):
            try:
                async for v in instream:
                    await q.put(v)
                    if self._to_shutdown.is_set():
                        break
                await q.put_end()
            except Exception as e:
                # This should be exception while
                # getting data from `instream`,
                # not exception in the current object.
                self._err = e
                self._to_shutdown.set()

        self._task = asyncio.create_task(foo(self._instream, self._q))

    def _stop(self):
        if self._task is not None:
            _ = self._task.result()
            self._task = None

    def __del__(self):
        self._stop()

    async def _get_next(self):
        if self._err is not None:
            self._stop()
            raise self._err
        return await self._q.__anext__()


def is_async(func):
    while isinstance(func, functools.partial):
        func = func.func
    return inspect.iscoroutinefunction(func) or (
        not inspect.isfunction(func)
        and hasattr(func, '__call__')
        and inspect.iscoroutinefunction(func.__call__)
    )


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
        self._async = is_async(func)

    async def _get_next(self):
        z = await self._instream.__anext__()
        try:
            if self._async:
                return await self.func(z)
            else:
                return self.func(z)
        except Exception as e:
            if self.return_exceptions:
                return e
            raise


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
        self._err = []
        self._tasks = []

        if is_async(func):
            self._async = True
            self._outstream = IterQueue(workers * 8, self._to_shutdown)
            self._start_async()
        else:
            self._async = False
            self._outstream = _sync_streamer.IterQueue(
                workers * 8, self._to_shutdown)
            self._start_sync()

    def _start_async(self):
        async def _process(in_stream, out_stream, func,
                           lock, finished, return_exceptions):
            while not finished.is_set():
                async with lock:
                    # This locked block ensures that
                    # input is read in order and their corresponding
                    # result placeholders (Future objects) are
                    # put in the output stream in order.
                    if finished.is_set():
                        return
                    if self._err:
                        return
                    try:
                        x = await in_stream.__anext__()
                        fut = asyncio.Future()
                        await out_stream.put(fut)
                    except StopAsyncIteration:
                        finished.set()
                        await out_stream.put_end()
                        return
                    except Exception as e:
                        finished.set()
                        self._err.append(e)
                        return

                try:
                    y = await func(x)
                    fut.set_result(y)
                except Exception as e:
                    if return_exceptions:
                        fut.set_result(e)
                    else:
                        fut.set_exception(e)
                        finished.set()
                        return

        lock = asyncio.Lock()
        finished = asyncio.Event()

        self._tasks = [
            asyncio.create_task(
                _process(
                    self._instream,
                    self._outstream,
                    self.func,
                    lock,
                    finished,
                    self.return_exceptions),
                name=f'transformer-{i}',
            )
            for i in range(self.workers)
        ]

    def _start_sync(self):
        async def _put_input_in_queue(q_in, q_out):
            try:
                async for x in q_in:
                    while True:
                        try:
                            q_out.put(x, block=False)
                            break
                        except queue.Full:
                            await asyncio.sleep(q_out.PUT_SLEEP)
                while True:
                    try:
                        q_out.put_end(block=False)
                        break
                    except queue.Full:
                        await asyncio.sleep(q_out.PUT_SLEEP)
            except Exception as e:
                self._err.append(e)

        q_in = _sync_streamer.IterQueue(self.workers * 8, self._to_shutdown)
        t1 = asyncio.create_task(_put_input_in_queue(self._instream, q_in))
        t2 = _sync_streamer.transform(
            q_in, self._outstream, self.func,
            self.workers, self.return_exceptions, self._err)
        self._tasks = [t1] + t2

    async def _astop(self):
        if not self._tasks:
            return
        if self._async:
            for t in self._tasks:
                await t
        else:
            await self._tasks[0]
            for t in self._tasks[1:]:
                t.join()
        self._tasks = []

    async def _get_next(self):
        try:
            if self._err:
                raise self._err[0]
            fut = await self._outstream.__anext__()
            if self._async:
                return await fut
            else:
                while not fut.done():
                    await asyncio.sleep(IterQueue.GET_SLEEP)
                return fut.result()
        except:
            await self._astop()
            raise
