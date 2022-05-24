'''
A multiprocessing queue using ZeroMQ.
I hoped to create a queue that is faster than the standard queue,
but the result was not convincing.
'''

import logging
import multiprocessing
import os
import threading
from multiprocessing import context as mp_context
from queue import Empty
from time import monotonic
from uuid import uuid4

import zmq
from zmq import ZMQError

logger = logging.getLogger(__name__)

_ForkingPickler = mp_context.reduction.ForkingPickler



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

    def put(self, obj):
        if self._closed:
            raise ValueError(f"{self!r} is closed")
        writer = self._get_writer()
        data = _ForkingPickler.dumps(obj)
        writer.send(data, copy=self._copy_on_write)

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

    def get_nowait(self):
        return self.get(False)
