# Async generator returns an async iterator.


import asyncio
import functools
import inspect
import logging
import queue
import random
import threading
from collections import deque
from collections.abc import (
    AsyncIterable,
    AsyncIterator,
    Callable,
    Iterable,
)
from inspect import iscoroutinefunction
from typing import (
    Any,
    Awaitable,
    Literal,
    TypeVar,
)

import asyncstdlib.itertools
from typing_extensions import Self  # In 3.11, import this from `typing`

from . import multiprocessing
from ._queues import SingleLane
from ._streamer import _NUM_PROCESSES, _NUM_THREADS, Stream, async_fifo_stream
from .concurrent.futures import (
    ProcessPoolExecutor,
    ThreadPoolExecutor,
)
from .threading import Thread

logger = logging.getLogger(__name__)

FINISHED = '8d906c4b-1161-40cc-b585-7cfb012bca26'
STOPPED = 'ceccca5e-9bb2-46c3-a5ad-29b3ba00ad3e'


T = TypeVar('T')  # indicates input data element
TT = TypeVar('TT')  # indicates output after an op on `T`
Elem = TypeVar('Elem')


def isiterable(x):
    try:
        iter(x)
        return True
    except (TypeError, AttributeError):
        return False


def isasynciterable(x):
    try:
        aiter(x)
        return True
    except (TypeError, AttributeError):
        return False


class AsyncStream(AsyncIterable[Elem]):
    def __init__(self, instream: AsyncIterable, /):
        self.streamlets: list[AsyncIterable] = [instream]

    def __aiter__(self) -> AsyncIterator[Elem]:
        return self.streamlets[-1].__aiter__()

    async def drain(self) -> int:
        n = 0
        async for _ in self:
            n += 1
        return n

    async def collect(self) -> list[Elem]:
        return [x async for x in self]

    def _choose_by_mode(self, sync_choice, async_choice):
        if isiterable(self):
            return sync_choice
        return async_choice

    def map(self, func: Callable[[T], Any], /, **kwargs) -> Self:
        self.streamlets.append(AsyncMapper(self.streamlets[-1], func, **kwargs))
        return self

    def filter(self, func: Callable[[T], bool], /, **kwargs) -> Self:
        self.streamlets.append(AsyncFilter(self.streamlets[-1], func, **kwargs))
        return self

    filter_exceptions = Stream.filter_exceptions

    peek = Stream.peek

    def shuffle(self, buffer_size: int = 1000) -> Self:
        self.streamlets.append(
            AsyncShuffler(self.streamlets[-1], buffer_size=buffer_size)
        )
        return self

    def head(self, n: int) -> Self:
        self.streamlets.append(AsyncHeader(self.streamlets[-1], n))
        return self

    def tail(self, n: int) -> Self:
        self.streamlets.append(AsyncTailor(self.streamlets[-1], n))
        return self

    def groupby(self, key: Callable[[T], Any], /, **kwargs) -> Self:
        self.streamlets.append(AsyncGrouper(self.streamlets[-1], key, **kwargs))
        return self

    def batch(self, batch_size: int) -> Self:
        self.streamlets.append(AsyncBatcher(self.streamlets[-1], batch_size))
        return self

    def unbatch(self) -> Self:
        self.streamlets.append(AsyncUnbatcher(self.streamlets[-1]))
        return self

    accumulate = Stream.accumulate

    def buffer(self, maxsize: int) -> Self:
        self.streamlets.append(AsyncBuffer(self.streamlets[-1], maxsize))
        return self

    def parmap(
        self,
        func: Callable[[T], TT],
        /,
        *,
        concurrency: int = None,
        return_x: bool = False,
        return_exceptions: bool = False,
        **kwargs,
    ) -> Self:
        if inspect.iscoroutinefunction(func):
            # The operator runs in the current thread on the current asyncio event loop.
            cls = AsyncParmapperAsync
        else:
            cls = AsyncParmapper

        self.streamlets.append(
            cls(
                self.streamlets[-1],
                func,
                concurrency=concurrency,
                return_x=return_x,
                return_exceptions=return_exceptions,
                **kwargs,
            )
        )
        return self


class SyncIter(Iterable):
    def __init__(self, instream: AsyncIterable):
        self._instream = instream

    def _worker(self):
        async def main():
            q = self._q
            to_stop = self._stopped
            try:
                async for x in self._instream:
                    if to_stop.is_set():
                        while True:
                            try:
                                q.get_nowait()
                            except queue.Empty:
                                break
                        return
                    q.put(x)
                q.put(FINISHED)
            except Exception as e:
                q.put(STOPPED)
                q.put(e)

        asyncio.run(main())
        # Since `self._instream` is async iterable,
        # this program is running in an async environment,
        # hence an event loop is running.
        # `asyncio.run_coroutine_threadsafe` might be useful,
        # but I couldn't make it work.

    def _start(self):
        self._q = queue.Queue(2)
        self._stopped = threading.Event()
        self._worker_thread = Thread(target=self._worker)
        self._worker_thread.start()

    def _finalize(self):
        if self._stopped is None:
            return
        self._stopped.set()
        self._worker_thread.join()
        self._stopped = None

    def __iter__(self):
        if isiterable(self._instream):
            yield from self._instream
        else:
            self._start()
            q = self._q
            finished = FINISHED
            stopped = STOPPED
            try:
                while True:
                    x = q.get()
                    if x == finished:
                        break
                    if x == stopped:
                        raise q.get()
                    yield x
            finally:
                self._finalize()


class AsyncIter(AsyncIterable):
    def __init__(self, instream: Iterable):
        self._instream = instream

    async def __aiter__(self):
        if isasynciterable(self._instream):
            async for x in self._instream:
                yield x
        else:
            loop = asyncio.get_running_loop()
            instream = iter(self._instream)
            finished = FINISHED
            while True:
                # ``next(instream)`` could involve some waiting and sleeping,
                # hence doing it in another thread.
                x = await loop.run_in_executor(None, next, instream, finished)
                # `FINISHED` is returned if there's no more elements.
                # See https://stackoverflow.com/a/61774972
                if x == finished:  # `instream_` exhausted
                    break
                yield x


class AsyncMapper(AsyncIterable):
    def __init__(
        self,
        instream: AsyncIterable,
        func: Callable[[T], Any] | Callable[[T], Awaitable[T]],
        **kwargs,
    ):
        self._instream = instream
        if kwargs:
            func = functools.partial(func, **kwargs)
        self.func = func

    async def __aiter__(self):
        func = self.func
        if iscoroutinefunction(func):
            async for v in self._instream:
                yield await func(v)
        else:
            async for v in self._instream:
                yield func(v)


class AsyncFilter(AsyncIterable):
    def __init__(
        self,
        instream: AsyncIterable,
        func: Callable[[T], bool] | Callable[[T], Awaitable[bool]],
        **kwargs,
    ):
        self._instream = instream
        self.func = functools.partial(func, **kwargs) if kwargs else func

    async def __aiter__(self):
        func = self.func
        if iscoroutinefunction(func):
            async for v in self._instream:
                if await func(v):
                    yield v
        else:
            async for v in self._instream:
                if func(v):
                    yield v


class AsyncHeader(AsyncIterable):
    def __init__(self, instream: AsyncIterable, /, n: int):
        assert n > 0
        self._instream = instream
        self.n = n

    async def __aiter__(self):
        n = 0
        async for v in self._instream:
            yield v
            n += 1
            if n >= self.n:
                break


class AsyncTailor(AsyncIterable):
    def __init__(self, instream: AsyncIterable, /, n: int):
        self._instream = instream
        assert n > 0
        self.n = n

    async def __aiter__(self):
        data = deque(maxlen=self.n)
        async for v in self._instream:
            data.append(v)
        for x in data:
            yield x


class AsyncGrouper(AsyncIterable):
    def __init__(
        self,
        instream: AsyncIterable,
        /,
        key: Callable[[T], Any] | Callable[[T], Awaitable[Any]],
        **kwargs,
    ):
        self._instream = instream
        if kwargs:
            key = functools.partial(key, **kwargs)
        self.key = key

    async def __aiter__(self):
        async for v in asyncstdlib.itertools.groupby(self._instream, self.key):
            yield v

        # _z = object()
        # group = None
        # func = self.func
        # async for x in self._instream:
        #     z = func(x)
        #     if z == _z:
        #         group.append(x)
        #     else:
        #         if group is not None:
        #             yield _z, group
        #         group = [x]
        #         _z = z
        # if group:
        #     yield _z, group


class AsyncBatcher(AsyncIterable):
    def __init__(self, instream: AsyncIterable, /, batch_size: int):
        self._instream = instream
        assert batch_size > 0
        self._batch_size = batch_size

    async def __aiter__(self):
        batch_size = self._batch_size
        batch = []
        async for x in self._instream:
            batch.append(x)
            if len(batch) == batch_size:
                yield batch
                batch = []
        if batch:
            yield batch


class AsyncUnbatcher(AsyncIterable):
    def __init__(self, instream: AsyncIterable, /):
        self._instream = instream

    async def __aiter__(self):
        async for x in self._instream:
            # `x` must be iterable or async iterable.
            if isiterable(x):
                for y in x:
                    yield y
            else:
                async for y in x:
                    yield y


class AsyncShuffler(AsyncIterable):
    def __init__(self, instream: AsyncIterable, /, buffer_size: int = 1000):
        assert buffer_size > 0
        self._instream = instream
        self._buffersize = buffer_size

    async def __aiter__(self):
        buffer = []
        buffersize = self._buffersize
        randrange = random.randrange
        async for x in self._instream:
            if len(buffer) < buffersize:
                buffer.append(x)
            else:
                idx = randrange(buffersize)
                y = buffer[idx]
                buffer[idx] = x
                yield y
        if buffer:
            random.shuffle(buffer)
            for x in buffer:
                yield x


class AsyncBuffer(AsyncIterable):
    def __init__(
        self,
        instream: AsyncIterable,
        /,
        maxsize: int,
        to_stop: threading.Event | multiprocessing.Event = None,
    ):
        self._instream = instream
        assert 1 <= maxsize <= 10_000
        self.maxsize = maxsize
        self._externally_stopped = to_stop

    def _start(self):
        self._stopped = threading.Event()
        self._tasks = SingleLane(self.maxsize)
        self._worker = Thread(target=self._run_worker, name='AsyncBuffer-worker-thread')
        self._worker.start()

    def _run_worker(self):
        async def main():
            q = self._tasks
            stopped = self._stopped
            extern_stopped = self._externally_stopped
            try:
                async for x in self._instream:
                    if stopped.is_set():
                        break
                    if extern_stopped is not None and extern_stopped.is_set():
                        break
                    q.put(x)  # if `q` is full, will wait here
                q.put(FINISHED)
            except Exception as e:
                q.put(STOPPED)
                q.put(e)
                # raise
                # Do not raise here. Otherwise it would print traceback,
                # while the same would be printed again in ``__iter__``.

        asyncio.run(main())

    def _finalize(self):
        if self._stopped is None:
            return
        self._stopped.set()
        tasks = self._tasks
        while not tasks.empty():
            _ = tasks.get()
        # `tasks` is now empty. The thread needs to put at most one
        # more element into the queue, which is safe.
        self._worker.join()
        self._stopped = None

    async def __aiter__(self):
        self._start()
        tasks = self._tasks
        finished = FINISHED
        stopped = STOPPED
        try:
            while True:
                try:
                    z = tasks.get_nowait()
                except queue.Empty:
                    await asyncio.sleep(0.002)
                    # TODO: how to avoid this sleep?
                    continue
                if z == finished:
                    break
                if z == stopped:
                    raise tasks.get()
                yield z
        finally:
            self._finalize()


class AsyncParmapper(AsyncIterable):
    # Environ is async; worker func is sync.
    def __init__(
        self,
        instream: AsyncIterable,
        func: Callable[[T], TT],
        *,
        executor: Literal['thread', 'process'],
        concurrency: int = None,
        return_x: bool = False,
        return_exceptions: bool = False,
        executor_initializer=None,
        executor_init_args=(),
        parmapper_name='parmapper-async-sync',
        **kwargs,
    ):
        assert executor in ('thread', 'process')
        if executor_initializer is None:
            assert not executor_init_args
        self._instream = instream
        self._func = func
        self._func_kwargs = kwargs
        self._return_x = return_x
        self._return_exceptions = return_exceptions
        self._executor_type = executor
        if concurrency is None:
            concurrency = _NUM_THREADS if executor == 'thread' else _NUM_PROCESSES
        self._concurrency = concurrency
        self._executor_initializer = executor_initializer
        self._executor_init_args = executor_init_args
        self._name = parmapper_name

    async def __aiter__(self):
        if self._executor_type == 'thread':
            executor = ThreadPoolExecutor(
                self._concurrency,
                initializer=self._executor_initializer,
                initargs=self._executor_init_args,
                thread_name_prefix=self._name + '-thread',
            )
        else:
            executor = ProcessPoolExecutor(
                self._concurrency,
                initializer=self._executor_initializer,
                initargs=self._executor_init_args,
            )
            # TODO: what about the process names?

        with executor:

            async def func(x, *, executor, loop, **kwargs):
                fut = executor.submit(self._func, x, **kwargs)
                return loop.run_in_executor(None, fut.result)

            loop = asyncio.get_running_loop()

            async for z in async_fifo_stream(
                self._instream,
                func,
                capacity=self._concurrency * 2,
                return_x=self._return_x,
                return_exceptions=self._return_exceptions,
                executor=executor,
                loop=loop,
                **self._func_kwargs,
            ):
                yield z


class AsyncParmapperAsync(AsyncIterable):
    # Environ is async; worker func is async.
    def __init__(
        self,
        instream: AsyncIterable,
        func: Callable[[T], Awaitable[TT]],
        *,
        concurrency: int | None = None,
        return_x: bool = False,
        return_exceptions: bool = False,
        parmapper_name='parmapper-async-async',
        **kwargs,
    ):
        self._instream = instream
        self._func = func
        self._func_kwargs = kwargs
        self._return_x = return_x
        self._return_exceptions = return_exceptions
        self._concurrency = concurrency or 128
        self._name = parmapper_name

    def __aiter__(self):
        async def func(x, loop, **kwargs):
            return loop.create_task(self._func(x, **kwargs))

        return async_fifo_stream(
            self._instream,
            func,
            name=self._name,
            capacity=self._concurrency * 2,
            return_x=self._return_x,
            return_exceptions=self._return_exceptions,
            loop=asyncio.get_running_loop(),
            **self._func_kwargs,
        )
