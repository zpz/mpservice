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
        .transform(my_op_that_takes_stream_of_batches, workers=4)
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
import time
from typing import (
    Callable, TypeVar, Optional, Union,
    Iterable, Iterator,
    Tuple, Type)

from . import streamer as _sync_streamer
from .streamer import is_exception, _default_peek_func, MAX_THREADS


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
            elif hasattr(instream, '__aiter__'):
                async def foo():
                    async for x in instream:
                        yield x
                self._instream = foo()
            else:
                async def foo():
                    for x in instream:
                        yield x
                self._instream = foo()
        self.index = 0
        # Index of the upcoming element; 0 based.
        # This is also the count of finished elements.

    def __aiter__(self):
        return self

    async def _get_next(self):
        return self._instream.__anext__()

    async def __anext__(self):
        try:
            z = await self._get_next()
            self.index += 1
            return z
        except StopIteration:
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

    def keep_first_n(self, n: int):
        async def foo(n, instream):
            i = 0
            async for x in instream:
                yield x
                i += 1
                if i == n:
                    break

        return Stream(foo(n, self._instream))

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
                  func: Callable[[T], TT],
                  *,
                  workers: Optional[Union[int, str]] = None,
                  return_exceptions: bool = False,
                  **func_args) -> Union[Transformer, ConcurrentTransformer]:
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

    async def _get_next(self):
        batch = []
        for _ in range(self.batch_size):
            try:
                batch.append(await self._instream.__anext__())
            except StopAsyncIteration:
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
            z = self._instream.__anext__()
            if self.func(self.index, z):
                self.index += 1
                continue
            return z


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

    async def put(self, x, block=True):
        while True:
            try:
                super().put_nowait(x)
                break
            except asyncio.QueueFull:
                if self._to_shutdown.is_set():
                    return
                if block:
                    await asyncio.sleep(self.PUT_SLEEP)
                else:
                    raise

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
        assert 1 <= 10_000
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
                await q.put_end()
            except Exception as e:
                # This should be exception while
                # getting data from `instream`,
                # not exception in the current object.
                self._err = e

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

    async def __next__(self):
        z = await self._instream.__anext__()
        try:
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

        if inspect.iscoroutinefunction(func) or (
            not inspect.isfunction(func)
            and hasattr(func, '__call__')
            and inspect.iscoroutinefunction(func.__call__)
        ):
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
            asyncio.create_task(_process(
                self._instream,
                self._outstream,
                self.func,
                lock,
                finished,
                self.return_exceptions,
            ))
            for _ in range(self.workers)
        ]

    def _start_sync(self):
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
                self._err.append(e)

        def _put_output_in_queue(q_in, q_out):
            return _sync_streamer.transform(
                q_in, q_out, self.func,
                self.workers, self.return_exceptions, self._err)

        loop = asyncio.get_running_loop()
        q_in_out = _sync_streamer.IterQueue(
            self.workers * 8, self._to_shutdown)
        t1 = loop.run_in_executor(None, _put_input_in_queue,
                                  self._instream, q_in_out)
        t2 = loop.run_in_executor(None, _put_output_in_queue,
                                  q_in_out, self._outstream)
        self._tasks = [t1] + t2

    def _stop(self):
        for t in self._tasks:
            _ = t.result()

    def __del__(self):
        self._stop()

    async def _get_next(self):
        try:
            if self._err:
                raise self._err[0]
            if self._async:
                fut = self._outstream.__anext__()
                return await fut
            else:
                while True:
                    try:
                        z = self._outstream.get_nowait()
                        if z is _sync_streamer.IterQueue.NO_MORE_DATA:
                            raise StopAsyncIteration
                        return z
                    except queue.Empty:
                        await asyncio.sleep(_sync_streamer.IterQueue.GET_SLEEP)
        except:
            self._stop()
            raise
