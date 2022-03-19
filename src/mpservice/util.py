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
from typing import Optional


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
    # The returned value is guaranteed to be async iterable.

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


class EventUpstreamer:
    '''Propagate a signal (Event) upstream, i.e. to input_stream.'''
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


class IterQueue(collections.abc.Iterator):
    '''
    A queue that supports iteration over its elements.

    In order to support iteration, it adds a special value
    to indicate end of data, which is inserted by calling
    the method `put_end`.
    '''
    GET_SLEEP = 0.00026
    PUT_SLEEP = 0.00015
    NO_MORE_DATA = object()

    def __init__(self, maxsize: int, *, downstream_crashed: EventUpstreamer = None):
        '''
        `upstream`: an upstream `IterQueue` object, usually the data stream that
        feeds into the current queue. This parameter allows this object and
        the upstream share an `Event` object that indicates either queue
        has stopped working, either deliberately or by exception.
        '''
        self.maxsize = maxsize
        self._q = queue.Queue(maxsize)
        self._downstream_crashed = downstream_crashed
        self._closed = False

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
            raise EOFError
        self._closed = True

    def put(self, item, block=True, timeout=None):
        t0 = time.perf_counter()
        while True:
            try:
                self._q.put_nowait(item)
                break
            except queue.Full:
                if self._downstream_crashed is not None and self._downstream_crashed.is_set():
                    return
                if self._closed:
                    raise EOFError
                if not block:
                    raise
                if timeout is None or (time.perf_counter() - t0 < timeout):
                    time.sleep(self.PUT_SLEEP)
                else:
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
                if self._downstream_crashed is not None and self._downstream_crashed.is_set():
                    return
                if self._closed:
                    raise EOFError
                if not block:
                    raise
                if timeout is None or (time.perf_counter() - t0 < timeout):
                    await asyncio.sleep(self.PUT_SLEEP)
                else:
                    raise

    def get(self, block=True, timeout=None):
        t0 = time.perf_counter()
        while True:
            try:
                return self._q.get_nowait()
            except queue.Empty:
                if self._downstream_crashed is not None and self._downstream_crashed.is_set():
                    return  # TODO: this is not ideal, as it still returns a value--`None`
                if self._closed:
                    raise EOFError
                if not block:
                    raise
                if timeout is None or (time.perf_counter() - t0 < timeout):
                    time.sleep(self.GET_SLEEP)
                else:
                    raise

    def get_nowait(self):
        return self.get(False)

    async def aget(self, block=True, timeout=None):
        t0 = time.perf_counter()
        while True:
            try:
                return self._q.get_nowait()
            except queue.Empty:
                if self._downstream_crashed is not None and self._downstream_crashed.is_set():
                    return
                if self._closed:
                    raise EOFError
                if not block:
                    raise
                if timeout is None or (time.perf_counter() - t0 < timeout):
                    await asyncio.sleep(self.GET_SLEEP)
                else:
                    raise

    def __next__(self):
        while True:
            try:
                return self.get_nowait()
            except EOFError:
                raise StopIteration
            except queue.Empty:
                if self._downstream_crashed is not None and self._downstream_crashed.is_set():
                    raise StopIteration
                time.sleep(self.GET_SLEEP)

    async def __anext__(self):
        while True:
            try:
                return self.get_nowait()
            except EOFError:
                raise StopAsyncIteration
            except queue.Empty:
                if self._downstream_crashed is not None and self._downstream_crashed.is_set():
                    raise StopAsyncIteration
                await asyncio.sleep(self.GET_SLEEP)

    def __iter__(self):
        return self

    def __aiter__(self):
        return self


class FutureIterQueue(IterQueue):
    # Elements put in this object are `concurrent.futures.Future`
    # or `asyncio.Future` objects. In either case, one can use
    # either `__next__` or `__anext__` to iterate the stream.
    # The user of the Future object should call its `set_result`
    # with a tuple of the input data element and the result of a computation.
    def __init__(self, maxsize: int, *, return_x: bool, return_exceptions: bool, **kwargs):
        super().__init__(maxsize=maxsize, **kwargs)
        self._return_x = return_x
        self._return_exceptions = return_exceptions

    def __next__(self):
        fut = super().__next__()
        while not fut.done():
            time.sleep(0.001)
        x, y = fut.result()
        if is_exception(y) and not self._return_exceptions:
            raise y
        if self._return_x:
            return x, y
        return y

    async def __anext__(self):
        fut = await super().__anext__()
        while not fut.done():
            await asyncio.sleep(0.001)
        x, y = fut.result()
        if is_exception(y) and not self._return_exceptions:
            raise y
        if self._return_x:
            return x, y
        return y
