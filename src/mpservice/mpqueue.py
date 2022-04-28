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
from types import SimpleNamespace
from multiprocessing import queues as mp_queues, context as mp_context
from queue import Empty, Full
from time import monotonic
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

    def get_many(self, max_n: int, first_timeout=None, extra_timeout=None):
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

        dev = zmq_devices.ThreadDevice(zmq.STREAMER, zmq.PULL, zmq.PUSH)  # pylint: disable=no-member
        # dev.setsockopt_in(zmq.IMMEDIATE, 1)  # pylint: disable=no-member
        # dev.setsockopt_in(zmq.RCVHWM, 1) # writer_hwm)  # pylint: disable=no-member
        # dev.setsockopt_out(zmq.IMMEDIATE, 1)  # pylint: disable=no-member
        dev.setsockopt_out(zmq.SNDHWM, self._hwm)  # pylint: disable=no-member
        # self.writer_port = dev.bind_in_to_random_port('ipc://*')  # self.host)
        # self.reader_port = dev.bind_out_to_random_port('ipc://*')  # self.host)
        dev.bind_in(writer_path)
        dev.bind_out(reader_path)
        dev.start()

        return writer_path, reader_path, dev

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
            self._writer = None
        if self._reader is not None:
            self._reader.close(linger)
            self._reader = None
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

    def get_many(self, max_n: int, first_timeout=None, extra_timeout=None):
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

    def get_many(self, max_n: int, first_timeout=None, extra_timeout=None):
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


def _BasicQueue(self):
    return BasicQueue(ctx=self.get_context())


mp_context.BaseContext.BasicQueue = _BasicQueue


def _ZeroQueue(self, **kwargs):
    return ZeroQueue(**kwargs)


mp_context.BaseContext.ZeroQueue = _ZeroQueue


def _FastQueue(self, **kwargs):
    return FastQueue(**kwargs)


mp_context.BaseContext.FastQueue = _FastQueue
