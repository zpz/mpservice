'''
This module defines a few alternatives to the standard `multiprocessing.queues.Queue`.
The goal is to improve performance over the standard Queue.
Once a clear winner emerges, the others may be removed.
'''

# Queue performance benchmarking:
#   https://stackoverflow.com/questions/8463008/multiprocessing-pipe-vs-queue
# quick-quque: https://github.com/Invarato/quick_queue_project
# queue/pipe/zmq benchmarking: https://gist.github.com/kylemcdonald/a2f0dcb86f01d4c57b68ac6a6c7a3068
# https://stackoverflow.com/questions/47085458/why-is-multiprocessing-queue-get-so-slow
# https://stackoverflow.com/questions/43439194/python-multiprocessing-queue-vs-multiprocessing-manager-queue/45236748#45236748
# https://stackoverflow.com/questions/23961669/how-can-i-speed-up-simultaneous-read-and-write-of-multiprocessing-queues
# https://stackoverflow.com/questions/60197392/high-performance-replacement-for-multiprocessing-queue

import logging
import multiprocessing
import os
import sys
import threading
from collections import deque
from multiprocessing import queues as mp_queues, context as mp_context
from queue import Empty, Full
from time import monotonic
from types import SimpleNamespace
from uuid import uuid4

import faster_fifo
import faster_fifo_reduction  # noqa: F401
import zmq
from zmq import ZMQError, devices as zmq_devices

logger = logging.getLogger(__name__)

_ForkingPickler = mp_context.reduction.ForkingPickler


# About changing pickle protocol for multiprocessing:
#  https://stackoverflow.com/questions/45119053/how-to-change-the-serialization-method-used-by-the-multiprocessing-module


class BasicQueue:
    def __init__(self, *, ctx=None):
        if ctx is None:
            ctx = multiprocessing.get_context()
        self._q = ctx.Queue()
        self._disable_semaphore()

    def _disable_semaphore(self):
        self._q._sem = SimpleNamespace()
        self._q._sem.acquire = lambda *args: True
        self._q._sem.release = lambda: True

    def __getstate__(self):
        self._q._sem = None
        z = self._q.__getstate__()
        self._disable_semaphore()
        return z

    def __setstate__(self, state):
        self._q = object.__new__(mp_queues.Queue)
        self._q.__setstate__(state)
        self._disable_semaphore()

    def empty(self):
        return self._q.empty()

    def close(self):
        # sleep(0.1)  # to prevent 'BrokenPipe' error. TODO: how to get rid of this?
        self._q.close()
        # self._q.join_thread()

    def put(self, obj, block=True, timeout=None):
        if timeout is None:
            timeout = 3600
        self._q.put(obj, block, timeout)

    def get(self, block=True, timeout=None):
        if timeout is None:
            timeout = 3600
        return self._q.get(block, timeout)

    def put_many(self, objs):
        if self._q._closed:
            raise ValueError(f"{self!r} is closed")

        with self._q._notempty:
            if self._q._thread is None:
                self._q._start_thread()
            for obj in objs:
                self._q._buffer.append(obj)
                self._q._notempty.notify()

    def get_many(self, max_n: int, *, first_timeout=None, extra_timeout=None):
        # Adapted from code of cpython.
        if self._q._closed:
            raise ValueError(f"{self!r} is closed")
        out = []
        n = 0
        if first_timeout is None:
            first_timeout = 3600
        deadline = monotonic() + first_timeout
        if not self._q._rlock.acquire(block=True, timeout=first_timeout):
            raise Empty
        # Now with the read lock
        try:
            if not self._q._poll(max(0, deadline - monotonic())):
                raise Empty
            out.append(self._q._recv_bytes())
            n += 1
            if extra_timeout is None:
                extra_timeout = 3600
            deadline = monotonic() + extra_timeout
            while n < max_n:
                # If there is remaining time, wait up to
                # that long.
                # Otherwise, get the next item if it is there
                # right now (i.e. no waiting) even if we
                # are already over time. That is, if supply
                # has piled up, then will get up to the
                # batch capacity.
                if not self._q._poll(max(0, deadline - monotonic())):
                    break
                out.append(self._q._recv_bytes())
                n += 1
        finally:
            self._q._rlock.release()
        return [_ForkingPickler.loads(v) for v in out]

    def put_nowait(self, obj):
        self.put(obj, False)

    def get_nowait(self):
        return self.get(False)


class ZeroQueue:
    def __init__(self, *, hwm: int = 1000):
        '''
        This class is meant for a many-many relationship, that is,
        multiple parties (processes) will put items in the queue,
        and multiple parties (processes) will get items from the queue.
        To support this relationship, an intermediate "collector" socket
        is necessary. If this class is used in a one-many or many-one
        relationship, this collector socket many be unnecessary, hence
        the use of it may incur a small overhead.

        WARNING: when used in multiprocessing,
        this class requires processes to be created by the "spawn" method.
        '''
        self._hwm = hwm
        self._writer = None
        self._reader = None
        self._closed = False

        self._copy_on_write = False
        self._copy_on_read = False

        self.writer_path, self.reader_path, self._collector = self._start_device()

    def _start_device(self):
        path = '/tmp/unixsockets/'
        os.makedirs(path, exist_ok=True)
        writer_path = path + str(uuid4())
        try:
            os.unlink(writer_path)
        except FileNotFoundError:
            pass
        reader_path = path + str(uuid4())
        try:
            os.unlink(reader_path)
        except FileNotFoundError:
            pass
        writer_path = 'ipc://' + writer_path
        reader_path = 'ipc://' + reader_path

        # dev = zmq_devices.ThreadDevice(zmq.STREAMER, zmq.PULL, zmq.PUSH)  # pylint: disable=no-member
        # # dev.setsockopt_in(zmq.IMMEDIATE, 1)  # pylint: disable=no-member
        # # dev.setsockopt_in(zmq.RCVHWM, 1) # writer_hwm)  # pylint: disable=no-member
        # # dev.setsockopt_out(zmq.IMMEDIATE, 1)  # pylint: disable=no-member
        # dev.setsockopt_out(zmq.SNDHWM, self._hwm)  # pylint: disable=no-member
        # dev.bind_in(writer_path)
        # dev.bind_out(reader_path)
        # dev.start()

        def foo():
            ctx = zmq.Context.instance()
            s_in = ctx.socket(zmq.PULL)
            s_in.bind(writer_path)
            s_out = ctx.socket(zmq.PUSH)
            s_out.bind(reader_path)
            s_out.set(zmq.SNDHWM, self._hwm)
            zmq.proxy(s_in, s_out)

        t = threading.Thread(target=foo, daemon=True)
        t.start()

        return writer_path, reader_path, t

    def __getstate__(self):
        multiprocessing.context.assert_spawning(self)
        return (self.writer_path, self.reader_path, self._hwm)

    def __setstate__(self, state):
        (self.writer_path, self.reader_path, self._hwm) = state
        self._writer, self._reader = None, None
        self._closed = False
        self._collector = None
        self._copy_on_write = False
        self._copy_on_read = False

    def _get_writer(self):
        writer = self._writer
        if writer is None:
            context = zmq.Context()
            writer = context.socket(zmq.PUSH)  # pylint: disable=no-member
            # writer.set(zmq.IMMEDIATE, 1)  # pylint: disable=no-member
            writer.connect(self.writer_path)  # f'{self.host}:{self.writer_port}')
            self._writer = writer
        return writer

    def _get_reader(self):
        reader = self._reader
        if reader is None:
            context = zmq.Context()
            reader = context.socket(zmq.PULL)  # pylint: disable=no-member
            # reader.set(zmq.IMMEDIATE, 1)  # pylint: disable=no-member
            reader.set(zmq.RCVHWM, self._hwm)
            reader.connect(self.reader_path)  # f'{self.host}:{self.reader_port}')
            self._reader = reader
        return reader

    def empty(self):
        reader = self._get_reader()
        return reader.poll(0, zmq.POLLIN) == 0

    def close(self, linger=60):
        '''
        Indicate that no more data will be put on this queue by the current process.
        Other processes may continue to put elements on the queue,
        and elements already on the queue can continue to be taken out.
        '''
        if self._closed:
            return
        if self._writer is not None:
            self._writer.close(linger)
        if self._reader is not None:
            self._reader.close(linger)
        self._closed = True

    def put(self, obj, block=True, timeout=None):
        '''
        `timeout`: seconds
        '''
        if self._closed:
            raise ValueError(f"{self!r} is closed")
        writer = self._get_writer()
        data = _ForkingPickler.dumps(obj)

        if not block:
            while True:
                try:
                    # return writer.send(data, zmq.NOBLOCK)  # pylint: disable=no-member
                    return writer.send(data, copy=self._copy_on_write)
                    # https://stackoverflow.com/a/22027139/6178706
                except ZMQError as e:
                    if isinstance(e, zmq.error.Again):
                        logger.error(repr(e))
                    # TODO: there can be other error conditions than Full.
                    raise Full from e

        if timeout is None:
            timeout = 3600
        if writer.poll(timeout * 1000, zmq.POLLOUT):
            return writer.send(data, copy=self._copy_on_write)
        raise Full

    def get(self, block=True, timeout=None):
        '''
        `timeout`: seconds.
        '''
        reader = self._get_reader()

        if not block:
            try:
                z = reader.recv(zmq.NOBLOCK, copy=self._copy_on_read)  # pylint: disable=no-member
                return _ForkingPickler.loads(z)
            except ZMQError as e:
                raise Empty from e
        if timeout is None:
            timeout = 3600
        if reader.poll(timeout * 1000, zmq.POLLIN):
            z = reader.recv(copy=self._copy_on_read)
            return _ForkingPickler.loads(z)
        raise Empty

    def put_many(self, objs):
        if self._closed:
            raise ValueError(f"{self!r} is closed")
        writer = self._get_writer()
        for v in objs:
            writer.send(_ForkingPickler.dumps(v), copy=self._copy_on_write)

    def get_many(self, max_n: int, *, first_timeout=None, extra_timeout=None):
        reader = self._get_reader()
        out = []
        n = 0

        if first_timeout is None:
            first_timeout = 3600
        if not reader.poll(first_timeout * 1000, zmq.POLLIN):
            raise Empty
        out.append(reader.recv(copy=self._copy_on_read))
        n += 1
        if extra_timeout is None:
            extra_timeout = 3600
        deadline = monotonic() + extra_timeout
        while n < max_n:
            t = max(0, deadline - monotonic())
            if reader.poll(t * 1000, zmq.POLLIN):
                out.append(reader.recv(copy=self._copy_on_read))
                n += 1
            else:
                break
        return [_ForkingPickler.loads(v) for v in out]

    def put_nowait(self, obj):
        return self.put(obj, False)

    def get_nowait(self):
        return self.get(False)


class FastQueue:
    def __init__(self, maxsize_bytes=10_000_000):
        self._q = faster_fifo.Queue(max_size_bytes=maxsize_bytes)

    def empty(self):
        return self._q.empty()

    def close(self):
        return self._q.close()

    def put(self, obj, block=True, timeout=None):
        if self._q.is_closed():
            raise ValueError(f"{self!r} is closed")
        if timeout is None:
            timeout = float(3600)
        self._q.put(obj, block, timeout)

    def get(self, block=True, timeout=None):
        if timeout is None:
            timeout = float(3600)
        if self._q.message_buffer is None:
            self._q.reallocate_msg_buffer(1_000_000)
        return self._q.get(block, timeout)

    def put_many(self, objs):
        if self._q.is_closed():
            raise ValueError(f"{self!r} is closed")

        objs = [list(objs)]
        while objs:
            try:
                self._q.put_many(objs[0], timeout=0.01)
            except Full:
                if len(objs[0]) > 1:
                    k = int(len(objs[0]) / 2)
                    objs = [objs[0][:k], objs[0][k:]] + objs[1:]
                    # Split the data batch into smaller batches.
                    # This is in case the buffer is too small for the whole batch,
                    # hence would never succeed unless we split the batch into
                    # smaller chunks.
            else:
                objs = objs[1:]

    def get_many(self, max_n: int, *, first_timeout=None, extra_timeout=None):
        if first_timeout is None:
            first_timeout = float(3600)
        if self._q.message_buffer is None:
            self._q.reallocate_msg_buffer(1_000_000)
        out = self._q.get_many(block=True, timeout=first_timeout, max_messages_to_get=1)
        if extra_timeout is None:
            extra_timeout = float(3600)
        try:
            more = self._q.get_many(block=True, timeout=extra_timeout, max_messages_to_get=max_n - 1)
        except Empty:
            more = []
        out.extend(more)
        return out

    def put_nowait(self, obj):
        self.put(obj, False)

    def get_nowait(self):
        return self.get(False)


class NaiveQueue:
    '''
    Adapted from the standard `queue.SimpleQueue`.

    Used by a single thread, but supports `maxsize`.
    '''
    def __init__(self, maxsize=0):
        self.maxsize = maxsize
        self._queue = deque()
        self._count = threading.Semaphore(0)
        if maxsize > 0:
            self._space = threading.Semaphore(maxsize)
        else:
            self._space = None

    def put(self, item, block=True, timeout=None):
        if self._space is not None:
            if not self._space.acquire(block, timeout):
                raise Full
        self._queue.append(item)
        self._count.release()

    def put_nowait(self, item):
        self.put(item, False)

    def get(self, block=True, timeout=None):
        if timeout is not None and timeout < 0:
            raise ValueError("'timeout' must be a non-negative number")
        if not self._count.acquire(block, timeout):
            raise Empty
        z = self._queue.popleft()
        if self._space is not None:
            self._space.release()
        return z

    def get_nowait(self):
        return self.get(False)

    def empty(self):
        return len(self._queue) == 0

    def full(self):
        return 0 < self.maxsize <= len(self._queue)

    def qsize(self):
        return len(self._queue)


class UniWriter:
    def __init__(self, pipe_writer, pipe_lock):
        self._writer = pipe_writer
        self._lock = pipe_lock  # this is None on win32
        self._buffer = deque()
        self._notempty = threading.Condition()
        self._nomore = object()
        self._closed = False
        self._thread = threading.Thread(target=self._feed, daemon=True)
        self._thread.start()

    def _feed(self):
        buffer = self._buffer
        notempty = self._notempty
        nomore = self._nomore
        lock = self._lock
        writer = self._writer

        while True:
            if not buffer:
                with notempty:
                    notempty.wait()
            x = buffer.popleft()
            if x is nomore:
                return
            x = _ForkingPickler.dumps(x)
            if lock is None:
                writer.send_bytes(x)
            else:
                with lock:
                    writer.send_bytes(x)

    def close(self):
        self.put(self._nomore)
        self._writer.close()
        self._closed = True

    def full(self):
        return False

    def put(self, obj, block=True, timeout=None):
        '''
        `block` and `timeout` are ignored.
        Accepted for API consistency with standard lib.
        '''
        if self._closed:
            raise ValueError(f"{self!r} is closed")
        with self._notempty:
            self._buffer.append(obj)
            self._notempty.notify()

    def put_many(self, objs):
        if self._closed:
            raise ValueError(f"{self!r} is closed")
        for obj in objs:
            with self._notempty:
                self._buffer.append(obj)
                self._notempty.notify()

    def put_nowait(self, item):
        self.put(item, False)


class UniReader:
    def __init__(self, pipe_reader, pipe_lock, maxsize=0):
        self._reader = pipe_reader
        self._lock = pipe_lock
        self.maxsize = maxsize
        self._buffer = NaiveQueue(maxsize)
        self._notfull = threading.Condition()
        self._get_called = threading.Event()
        self._closed = False
        self._thread = threading.Thread(target=self._read, daemon=True)
        self._thread.start()

    def _read(self):
        buffer = self._buffer
        lock = self._lock
        notfull = self._notfull
        get_called = self._get_called
        closed = self._closed
        while True:
            if buffer.full():
                with notfull:
                    notfull.wait()
            with lock:
                # In order to facilitate batching,
                # we hold the lock and keep getting
                # data off the pipe for this reader's
                # buffer, while there may be other readers
                # waiting for the lock.
                # Once `get` or `get_many` has been called,
                # we release the lock so that other readers
                # get a chance for the lock.
                while not buffer.full():
                    x = self._reader._recv_bytes()
                    buffer.put_nowait(x.getbuffer())
                    # This is the only code that `put` data
                    # into `buffer`, hence `put_nowait`
                    # is safe b/c we know `buffer` is not full.
                    if closed:
                        return
                    if get_called.is_set():
                        get_called.clear()
                        break

    def close(self):
        self._reader.close()
        self._closed = True

    def empty(self):
        return self._buffer.empty()

    def qsize(self):
        return self._buffer.qsize()

    def get(self, block=True, timeout=None):
        if self._closed:
            raise ValueError(f"{self!r} is closed")
        with self._notfull:
            z = self._buffer.get(block, timeout)
            self._notfull.notify()
        self._get_called.set()
        return _ForkingPickler.loads(z)

    def get_many(self, max_n: int, *, first_timeout=None, extra_timeout=None):
        if first_timeout is None:
            first_timeout = 3600
        deadline = monotonic() + first_timeout
        z = self.get(timeout=first_timeout)
        out = [z]
        n = 1
        if extra_timeout is None:
            extra_timeout = 3600
        deadline = monotonic() + extra_timeout
        while n < max_n:
            # If there is remaining time, wait up to
            # that long.
            # Otherwise, get the next item if it is there
            # right now (i.e. no waiting) even if we
            # are already overtime. That is, if supply
            # has piled up, then will get up to the
            # batch capacity.
            try:
                t = max(0., deadline - monotonic())
                out.append(self.get(timeout=t))
            except Empty:
                break
            n += 1
        return out

    def get_nowait(self):
        return self.get(False)


class UniQueue:
    def __init__(self, *, ctx=None):
        if ctx is None:
            ctx = multiprocessing.get_context()
        self._reader, self._writer = multiprocessing.connection.Pipe(duplex=False)
        self._rlock = ctx.Lock()
        if sys.platform == 'win32':
            self._wlock = None
        else:
            self._wlock = ctx.Lock()

    def writer(self):
        return UniWriter(self._writer, self._wlock)

    def reader(self, buffer_size: int = 1024):
        return UniReader(self._reader, self._rlock, buffer_size)


def _BasicQueue(self):
    return BasicQueue(ctx=self.get_context())


mp_context.BaseContext.BasicQueue = _BasicQueue


def _ZeroQueue(self, **kwargs):
    return ZeroQueue(**kwargs)


mp_context.BaseContext.ZeroQueue = _ZeroQueue


def _FastQueue(self, **kwargs):
    return FastQueue(**kwargs)


mp_context.BaseContext.FastQueue = _FastQueue


def _UniQueue(self, **kwargs):
    return UniQueue(**kwargs)


mp_context.BaseContext.UniQueue = _UniQueue

