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
import logging
from typing import (
    Awaitable, Callable, TypeVar, Optional, Union,
    Iterable, Iterator,
    Tuple, Type)

from overrides import overrides
from . import _streamer as _sync_streamer
from ._streamer import _default_peek_func
from .util import make_aiter, EventUpstreamer, IterQueue, MAX_THREADS, is_exception

logger = logging.getLogger(__name__)


T = TypeVar('T')
TT = TypeVar('TT')


class Stream(collections.abc.AsyncIterator, _sync_streamer.StreamMixin):
    @classmethod
    def register(cls, class_: Type[Stream], name: str):
        def f(self, *args, **kwargs):
            return class_(self, *args, **kwargs)

        setattr(cls, name, f)

    def __init__(self, instream: Union[Stream, Iterator, Iterable], /):
        if isinstance(instream, Stream):
            self._crashed = instream._crashed
            self._instream = instream
        else:
            self._crashed = EventUpstreamer()
            self._instream = make_aiter(instream)

        self.index = 0
        # Index of the upcoming element; 0 based.
        # This is also the count of finished elements.

    def __aiter__(self):
        return self

    async def _get_next(self):
        return await self._instream.__anext__()

    async def __anext__(self):
        z = await self._get_next()
        self.index += 1
        return z

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

    def head(self, n: int) -> Header:
        return Header(self, n)

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
                  func: Callable[[T], Awaitable[TT]],
                  *,
                  workers: Optional[Union[int, str]] = None,
                  return_exceptions: bool = False,
                  keep_order: bool = None,
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
            return_exceptions=return_exceptions,
            keep_order=keep_order,
        )


class Batcher(Stream):
    def __init__(self, instream: Stream, batch_size: int):
        super().__init__(instream)
        assert 1 < batch_size <= 10_000
        self.batch_size = batch_size
        self._done = False

    @overrides
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

    @overrides
    async def _get_next(self):
        if self._batch:
            return self._batch.pop(0)
        z = await self._instream.__anext__()
        if isinstance(z, Exception):
            return z
        self._batch = z
        return await self._get_next()


class Dropper(Stream):
    def __init__(self, instream: Stream, func: Callable[[int, T], bool]):
        super().__init__(instream)
        self.func = func

    @overrides
    async def _get_next(self):
        while True:
            z = await self._instream.__anext__()
            if self.func(self.index, z):
                self.index += 1
                continue
            return z


class Header(Stream):
    def __init__(self, instream: Stream, n: int):
        super().__init__(instream)
        assert n >= 0
        self.n = n

    @overrides
    async def _get_next(self):
        if self.index >= self.n:
            raise StopAsyncIteration
        return await self._instream.__anext__()


class Peeker(Stream):
    def __init__(self, instream: Stream, func: Callable[[int, T], None]):
        super().__init__(instream)
        self.func = func

    @overrides
    async def _get_next(self):
        z = await self._instream.__anext__()
        self.func(self.index, z)
        return z


class Buffer(Stream):
    def __init__(self, instream: Stream, maxsize: int):
        super().__init__(instream)
        assert 1 <= maxsize <= 10_000
        self.maxsize = maxsize
        self._q = IterQueue(maxsize, downstream_crashed=self._crashed)
        self._upstream_err = None
        self._task = None
        self._start()

    def _start(self):
        async def foo(instream, q, crashed):
            try:
                async for v in instream:
                    if crashed.is_set():
                        break
                    await q.aput(v)
                q.close()
            except Exception as e:
                # This should be exception while
                # getting data from `instream`,
                # not exception in the current object.
                self._upstream_err = e
                q.close()

        self._task = asyncio.create_task(foo(self._instream, self._q, self._crashed))

    @overrides
    async def _get_next(self):
        try:
            return await self._q.__anext__()
        except StopAsyncIteration:
            if self._upstream_err is not None:
                raise self._upstream_err
            raise


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

    @overrides
    async def _get_next(self):
        z = await self._instream.__anext__()
        try:
            return await self.func(z)
        except Exception as e:
            if self.return_exceptions:
                return e
            self._crashed.set()
            raise


class ConcurrentTransformer(Stream):
    def __init__(self,
                 instream: Stream,
                 func: Callable[[T], TT],
                 *,
                 workers: int,
                 return_exceptions: bool = False,
                 keep_order: bool = None,
                 ):
        assert workers >= 1
        super().__init__(instream)
        self.func = func
        self.workers = workers
        self.return_exceptions = return_exceptions
        self.keep_order = workers > 1 and (keep_order is None or keep_order)
        self._upstream_err = []
        self._tasks = []

        self._outstream = IterQueue(workers * 1024, downstream_crashed=self._crashed)
        self._start()

    def _start(self):
        async def _process(in_stream, out_stream, func, lock, finished, crashed, keep_order):

            async def set_finish():
                active_tasks.pop()
                if not active_tasks:
                    # This thread is the last active one
                    # for the current transformer.
                    out_stream.close()
                finished.set()

            Future = asyncio.Future
            while True:
                async with lock:
                    if finished.is_set():
                        await set_finish()
                        return
                    if crashed.is_set():
                        await set_finish()
                        return

                    try:
                        x = await in_stream.__anext__()
                    except StopAsyncIteration:
                        await set_finish()
                        return
                    except Exception as e:
                        self._upstream_err.append(e)
                        await set_finish()
                        return
                    else:
                        if keep_order:
                            fut = Future()
                            await out_stream.aput(fut)

                if keep_order:
                    try:
                        y = await func(x)
                    except Exception as e:
                        if self.return_exceptions:
                            y = e
                        else:
                            fut.set_exception(e)
                            self._crashed.set()
                            await set_finish()
                            return
                    fut.set_result(y)
                else:
                    try:
                        y = await func(x)
                    except Exception as e:
                        await out_stream.aput(e)
                        if not self.return_exceptions:
                            self._crashed.set()
                            await set_finish()
                            return
                    else:
                        await out_stream.aput(y)

        lock = asyncio.Lock()
        finished = asyncio.Event()

        active_tasks = list(range(self.workers))
        self._tasks = [
            asyncio.create_task(
                _process(
                    self._instream,
                    self._outstream,
                    self.func,
                    lock,
                    finished,
                    self._crashed,
                    self.keep_order),
                name=f'transformer-{i}',
            )
            for i in range(self.workers)
        ]

    @overrides
    async def _get_next(self):
        if self._upstream_err:
            raise self._upstream_err[0]
        try:
            y = await self._outstream.__anext__()
        except StopAsyncIteration:
            if self._upstream_err:
                raise self._upstream_err[0]
            raise
        if self.keep_order:
            try:
                return await y
            except Exception:
                self._crashed.set()
                raise
        if isinstance(y, Exception) and not self.return_exceptions:
            self._crashed.set()
            raise y
        return y
