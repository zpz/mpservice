from __future__ import annotations
# Enable using a class in type annotations in the code
# that defines that class itself.
# https://stackoverflow.com/a/49872353
# Will no longer be needed in Python 3.10.

import asyncio
import collections.abc
import concurrent.futures
import functools
import inspect
import logging
import logging.handlers
import multiprocessing
import queue
import subprocess
import time
import warnings

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


def full_class_name(cls):
    if not isinstance(cls, type):
        cls = cls.__class__
    mod = cls.__module__
    if mod is None or mod == 'builtins':
        return cls.__name__
    return mod + '.' + cls.__name__


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


class IterQueue(collections.abc.Iterator, EnforceOverrides):
    '''
    A queue that supports iteration over its elements.

    In order to support iteration, it has an attribute that
    indicates whether there are more elements in the queue.
    The attribute is marked `True` by the method `close`.
    '''
    GET_SLEEP = 0.00026
    PUT_SLEEP = 0.00015

    def __init__(self, maxsize: int):
        self.maxsize = maxsize
        self._q: queue.Queue = queue.Queue(maxsize)
        self._closed: bool = False

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
            raise RuntimeError('%s is already closed', self)
        self._closed = True

    def put(self, item, block=True, timeout=None):
        t0 = time.perf_counter()
        while True:
            try:
                self._q.put_nowait(item)
                break
            except queue.Full:
                if self._closed:
                    raise RuntimeError('%s is already closed', self)
                if not block:
                    raise
                if timeout is None or (time.perf_counter() - t0 < timeout):
                    time.sleep(self.PUT_SLEEP)
                else:
                    raise
            except BaseException:
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
                if self._closed:
                    raise RuntimeError('%s is already closed', self)
                if not block:
                    raise
                if timeout is None or (time.perf_counter() - t0 < timeout):
                    await asyncio.sleep(self.PUT_SLEEP)
                else:
                    raise
            except BaseException:
                raise

    def get(self, block=True, timeout=None):
        t0 = time.perf_counter()
        while True:
            try:
                return self._q.get_nowait()
            except queue.Empty:
                if self._closed:
                    raise EOFError
                if not block:
                    raise
                if timeout is None or (time.perf_counter() - t0 < timeout):
                    time.sleep(self.GET_SLEEP)
                else:
                    raise
            except BaseException:
                raise

    def get_nowait(self):
        return self.get(False)

    async def aget(self, block=True, timeout=None):
        t0 = time.perf_counter()
        while True:
            try:
                return self._q.get_nowait()
            except queue.Empty:
                if self._closed:
                    raise EOFError
                if not block:
                    raise
                if timeout is None or (time.perf_counter() - t0 < timeout):
                    await asyncio.sleep(self.GET_SLEEP)
                else:
                    raise
            except BaseException:
                raise

    def __next__(self):
        while True:
            try:
                return self.get_nowait()
            except EOFError:
                raise StopIteration
            except queue.Empty:
                time.sleep(self.GET_SLEEP)

    async def __anext__(self):
        while True:
            try:
                return self.get_nowait()
            except EOFError:
                raise StopAsyncIteration
            except queue.Empty:
                await asyncio.sleep(self.GET_SLEEP)

    def __iter__(self):
        return self

    def __aiter__(self):
        return self


class FutureIterQueue(IterQueue):
    '''
    Elements put in this object are length-two tuples.
    The first element is a data element `x`; the second element
    is a `concurrent.futures.Future` or `asyncio.Future` object.
    The Future object will hold the result of some I/O-bound operation
    that takes `x` as input.
    '''

    def __init__(self, maxsize: int, *,
                 return_x: bool = False,
                 return_exceptions: bool = False,
                 **kwargs):
        super().__init__(maxsize=maxsize, **kwargs)
        self._return_x = return_x
        self._return_exceptions = return_exceptions

    @overrides
    def get(self, block=True, timeout=None):
        t0 = time.perf_counter()
        x, fut = super().get(block=block, timeout=timeout)
        while not fut.done():
            if timeout is not None and time.perf_counter() - t0 > timeout:
                raise concurrent.futures.TimeoutError
            time.sleep(0.001)
        try:
            y = fut.result()
        except Exception as e:
            if self._return_exceptions:
                if self._return_x:
                    return x, e
                else:
                    return e
            raise
        except BaseException:
            raise
        else:
            if self._return_x:
                return x, y
            return y

    @overrides
    async def aget(self, block=True, timeout=None):
        t0 = time.perf_counter()
        x, fut = await super().aget(block=block, timeout=timeout)
        while not fut.done():
            if timeout is not None and time.perf_counter() - t0 > timeout:
                raise concurrent.futures.TimeoutError
            await asyncio.sleep(0.001)
        try:
            y = fut.result()
        except Exception as e:
            if self._return_exceptions:
                if self._return_x:
                    return x, e
                else:
                    return e
            raise
        else:
            if self._return_x:
                return x, y
            return y
