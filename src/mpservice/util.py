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
import logging.handlers
import multiprocessing
import queue
import subprocess
import threading
import time
import warnings
from typing import Optional, Callable

from overrides import EnforceOverrides, overrides

logger = logging.getLogger(__name__)

MAX_THREADS = min(32, multiprocessing.cpu_count() + 4)
# This default is suitable for I/O bound operations.
# This value is what is used by `concurrent.futures.ThreadPoolExecutor`.
# For others, user may want to specify a smaller value.


def is_exception(e):
    return isinstance(e, BaseException) or (
        (type(e) is type) and issubclass(e, BaseException)
    )


def is_async(func):
    while isinstance(func, functools.partial):
        func = func.func
    return inspect.iscoroutinefunction(func) or (
        not inspect.isfunction(func)
        and hasattr(func, '__call__')
        and inspect.iscoroutinefunction(func.__call__)
    )


def logger_thread(q: multiprocessing.Queue):
    '''
    In main thread, start another thread with this function as `target`.
    '''
    while True:
        record = q.get()
        if record is None:
            # User should put a `None` in `q` to indicate stop.
            break
        logger = logging.getLogger(record.name)
        if record.levelno >= logger.getEffectiveLevel():
            logger.handle(record)


def forward_logs(q: multiprocessing.Queue):
    '''
    In a Process (created using the "spawn" method),
    run this function at the beginning to set up putting all log messages
    ever produced in that process into the queue that will be consumed by
    `logger_thread`.

    During the execution of the process, logging should not be configured.
    Logging config should happen in the main process/thread.
    '''
    root = logging.getLogger()
    if root.handlers:
        warnings.warn('root logger has handlers: {}; deleted'.format(root.handlers))
        root.handlers = []
    root.setLevel(logging.DEBUG)
    qh = logging.handlers.QueueHandler(q)
    root.addHandler(qh)


def get_docker_host_ip():
    # INTERNAL_HOST_IP=$(ip route show default | awk '/default/ {print $3})')
    # another idea:
    # ip -4 route list match 0/0 | cut -d' ' -f3
    #
    # Usually the result is '172.17.0.1'

    z = subprocess.check_output(['ip', '-4', 'route', 'list', 'match', '0/0'])
    z = z.decode()[len('default via '):]
    return z[: z.find(' ')]


def make_aiter(data):
    # `data` is iterable or async iterable.
    # The returned value is guaranteed to be async iterable and iterator.

    if hasattr(data, '__anext__') and hasattr(data, '__aiter__'):
        return data

    if hasattr(data, '__aiter__'):

        async def foo(instream):
            async for x in instream:
                yield x

        return foo(data)

    if hasattr(data, '__anext__'):

        class MyStream:
            def __init__(self, data):
                self._data = data

            async def __anext__(self):
                return await self._data.__anext__()

            def __aiter__(self):
                return self

        return MyStream(data)

    if not hasattr(data, '__next__'):
        assert hasattr(data, '__iter__')
        data = iter(data)

    class YourStream:
        def __init__(self, data):
            self._data = data

        async def __anext__(self):
            try:
                return next(self._data)
            except StopIteration:
                raise StopAsyncIteration

        def __aiter__(self):
            return self

    return YourStream(data)


class DownstreamError(Exception):
    pass


class EventUpstreamer:
    '''Propagate a signal (Event) upstream the caller chain.'''
    def __init__(self, upstream: Optional[EventUpstreamer] = None, /):
        self.upstream = upstream
        self._event = threading.Event()

    def set(self, include_self: bool = False):
        if include_self:
            self._event.set()
        if self.upstream is not None:
            self.upstream.set(True)

    def is_set(self) -> bool:
        return self._event.is_set()

    def __bool__(self):
        return self._event.is_set()


class IterQueueClosedError(Exception):
    def __init__(self, q):
        super().__init__(f"{q} is already closed")


class ErrorBox:
    def __init__(self):
        self._error = None

    def set_exception(self, e: BaseException):
        self._error = e

    def __bool__(self):
        return self._error is not None

    def exception(self):
        return self._error


class IterQueue(collections.abc.Iterator, EnforceOverrides):
    '''
    A queue that supports iteration over its elements.

    In order to support iteration, it adds a special value
    to indicate end of data, which is inserted by calling
    the method `put_end`.
    '''
    GET_SLEEP = 0.00026
    PUT_SLEEP = 0.00015
    NO_MORE_DATA = object()

    def __init__(self, maxsize: int, *,
                 downstream_crashed: EventUpstreamer = None,
                 upstream_error: ErrorBox = None,
                 ):
        self.maxsize = maxsize
        self._q = queue.Queue(maxsize)
        self._downstream_crashed = downstream_crashed
        self._upstream_error = upstream_error
        self._closed = False
        self._done_callbacks = []

    def __repr__(self):
        return f"{self.__class__.__name__}({self.maxsize})"

    def __str__(self):
        return self.__repr__()

    def qsize(self):
        return self._q.qsize()

    def empty(self):
        return self._q.empty()

    def full(self):
        return self._q.full()

    def close(self):
        # Close the 'write' side: no more items can be put in it.
        # If the queue is not empty, use can still get them out.
        if self._closed:
            self._run_done_callbacks()
            raise IterQueueClosedError(self)
        self._closed = True

    def put(self, item, block=True, timeout=None):
        t0 = time.perf_counter()
        while True:
            try:
                self._q.put_nowait(item)
                break
            except queue.Full:
                if self._downstream_crashed:
                    self._run_done_callbacks()
                    raise DownstreamError
                if self._closed:
                    self._run_done_callbacks()
                    raise IterQueueClosedError(self)
                if not block:
                    self._run_done_callbacks()
                    raise
                if timeout is None or (time.perf_counter() - t0 < timeout):
                    time.sleep(self.PUT_SLEEP)
                else:
                    self._run_done_callbacks()
                    raise

    def put_nowait(self, item):
        self.put(item, False)

    async def aput(self, item, block=True, timeout=None):
        assert not self._closed
        t0 = time.perf_counter()
        while True:
            try:
                self._q.put_nowait(item)
                break
            except queue.Full:
                if self._downstream_crashed:
                    self._run_done_callbacks()
                    raise DownstreamError
                if self._closed:
                    self._run_done_callbacks()
                    raise IterQueueClosedError(self)
                if not block:
                    self._run_done_callbacks()
                    raise
                if timeout is None or (time.perf_counter() - t0 < timeout):
                    await asyncio.sleep(self.PUT_SLEEP)
                else:
                    self._run_done_callbacks()
                    raise

    def get(self, block=True, timeout=None):
        t0 = time.perf_counter()
        while True:
            try:
                return self._q.get_nowait()
            except queue.Empty:
                if self._downstream_crashed:
                    self._run_done_callbacks()
                    raise DownstreamError
                if self._upstream_error:
                    self._run_done_callbacks()
                    raise self._upstream_error.exception()
                if self._closed:
                    self._run_done_callbacks()
                    raise EOFError
                if not block:
                    self._run_done_callbacks()
                    raise
                if timeout is None or (time.perf_counter() - t0 < timeout):
                    time.sleep(self.GET_SLEEP)
                else:
                    self._run_done_callbacks()
                    raise

    def get_nowait(self):
        return self.get(False)

    async def aget(self, block=True, timeout=None):
        t0 = time.perf_counter()
        while True:
            try:
                return self._q.get_nowait()
            except queue.Empty:
                if self._downstream_crashed:
                    self._run_done_callbacks()
                    raise DownstreamError
                if self._upstream_error:
                    self._run_done_callbacks()
                    raise self._upstream_error.exception()
                if self._closed:
                    self._run_done_callbacks()
                    raise EOFError
                if not block:
                    self._run_done_callbacks()
                    raise
                if timeout is None or (time.perf_counter() - t0 < timeout):
                    await asyncio.sleep(self.GET_SLEEP)
                else:
                    self._run_done_callbacks()
                    raise

    def __next__(self):
        while True:
            try:
                return self.get_nowait()
            except EOFError:
                raise StopIteration
            except queue.Empty:
                if self._downstream_crashed:
                    raise StopIteration
                time.sleep(self.GET_SLEEP)

    async def __anext__(self):
        while True:
            try:
                return self.get_nowait()
            except EOFError:
                raise StopAsyncIteration
            except queue.Empty:
                if self._downstream_crashed:
                    raise StopAsyncIteration
                await asyncio.sleep(self.GET_SLEEP)

    def __iter__(self):
        return self

    def __aiter__(self):
        return self

    def add_done_callback(self, cb: Callable):
        self._done_callbacks.append(cb)

    def _run_done_callbacks(self):
        for f in self._done_callbacks:
            try:
                f()
            except Exception as e:
                logger.error(e)


class FutureIterQueue(IterQueue):
    # Elements put in this object are `concurrent.futures.Future`
    # or `asyncio.Future` objects. In either case, one can use
    # either `__next__` or `__anext__` to iterate the stream.
    # The user of the Future object takes some data `x` and produces a result `y`,
    # then calls `fut.set_result((x, y))`. If any error happens, use the exception
    # object as `y`.
    def __init__(self, maxsize: int, *, return_x: bool = False, return_exceptions: bool = False, **kwargs):
        super().__init__(maxsize=maxsize, **kwargs)
        self._return_x = return_x
        self._return_exceptions = return_exceptions

    @overrides
    def get(self, block=True, timeout=None):
        fut = super().get(block=block, timeout=timeout)
        while not fut.done():
            time.sleep(0.001)
        x, y = fut.result()
        if is_exception(y) and not self._return_exceptions:
            self._run_done_callbacks()
            raise y
        if self._return_x:
            return x, y
        return y

    @overrides
    async def aget(self, block=True, timeout=None):
        fut = await super().aget(block=block, timeout=timeout)
        while not fut.done():
            await asyncio.sleep(0.001)
        x, y = fut.result()
        if is_exception(y) and not self._return_exceptions:
            self._run_done_callbacks()
            raise y
        if self._return_x:
            return x, y
        return y
