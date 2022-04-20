import multiprocessing
from multiprocessing import queues
from pickle import dumps as _pickle_dumps, loads as pickle_loads, HIGHEST_PROTOCOL
from queue import Empty, Full
from time import monotonic
from typing import Sequence

import faster_fifo
import faster_fifo_reduction  # noqa: F401
import zmq
from zmq import ZMQError, devices as zmq_devices


def pickle_dumps(x):
    return _pickle_dumps(x, protocol=HIGHEST_PROTOCOL)


class BasicQueue(queues.Queue):
    def __init__(self, maxsize=0, *, ctx=None):
        if ctx is None:
            ctx = multiprocessing.get_context()
        super().__init__(maxsize, ctx=ctx)

    def put_many(self, xs: Sequence, block=True, timeout=None):
        if not block:
            for x in xs:
                self.put_nowait(x)
            return

        if timeout is None:
            for x in xs:
                self.put(x)
            return

        deadline = monotonic() + timeout
        for x in xs:
            timeout = deadline - monotonic()
            if timeout > 0:
                self.put(x, timeout=timeout)
            else:
                self.put_nowait(x)

    def put_many_nowait(self, xs: Sequence):
        return self.put_many(xs, False)

    def get_many(self, max_n: int, block=True, timeout=None):
        # Adapted from code of cpython.
        if self._closed:
            raise ValueError(f"Queue {self!r} is closed")
        out = []
        n = 0
        if block and timeout is None:
            with self._rlock:
                while n < max_n:
                    out.append(self._recv_bytes())
                    n += 1
                    self._sem.release()
        else:
            if block:
                deadline = monotonic() + timeout
            if not self._rlock.acquire(block, timeout):
                return []
            try:
                while n < max_n:
                    # If there is remaining time, wait up to
                    # that long.
                    # Otherwise, get the next item if it is there
                    # right now (i.e. no waiting) even if we
                    # are already over time. That is, if supply
                    # has piled up, then will get up to the
                    # batch capacity.
                    if block:
                        timeout = max(0, deadline - monotonic())
                        ready = self._poll(timeout)
                    else:
                        ready = self._poll()
                    if not ready:
                        break
                    out.append(self._recv_bytes())
                    n += 1
                    self._sem.release()
            finally:
                self._rlock.release()
        return [pickle_loads(v) for v in out]

    def get_many_nowait(self, max_n):
        return self.get_many(max_n, False)


class ZeroQueue:
    def __init__(self, *,
                 writer_hwm: int = 1000,
                 reader_hwm: int = 1000,
                 ctx=None,
                 host: str = 'tcp://0.0.0.0'):
        '''
        Usually there is no need to name the two parameters `writer_port` and `reader_port`.
        The two ports will be used in a consistent way within this object, but
        externally it does not matter which one is for writing and which one is
        for reading.

        This class is meant for a many-many relationship, that is,
        multiple parties (processes) will put items in the queue,
        and multiple parties (processes) will get items from the queue.
        To support this relationship, an intermediate "collector" socket
        is necessary. If this class is used in a one-many or many-one
        relationship, this collector socket many be unnecessary, hence
        the use of it may incur a small overhead.
        '''
        self.writer_port = None
        self.reader_port = None
        self.host = host

        self._writer = None
        self._reader = None
        self._closed = False

        # Keep track of writers on this queue across processes,
        # not only in "current" process.
        if ctx is None:
            ctx = multiprocessing.get_context()
        self._n_writers_opened = ctx.Value('i', 0)
        self._n_writers_closed = ctx.Value('i', 0)

        # The following happens only in the "originating" object.
        dev = zmq_devices.ThreadDevice(zmq.STREAMER, zmq.PULL, zmq.PUSH)  # pylint: disable=no-member
        self.writer_port = dev.bind_in_to_random_port(self.host)
        self.reader_port = dev.bind_out_to_random_port(self.host)
        dev.setsockopt_in(zmq.IMMEDIATE, True)  # pylint: disable=no-member
        dev.setsockopt_in(zmq.RCVHWM, writer_hwm)  # pylint: disable=no-member
        dev.setsockopt_out(zmq.IMMEDIATE, True)  # pylint: disable=no-member
        dev.setsockopt_out(zmq.SNDHWM, reader_hwm)  # pylint: disable=no-member
        dev.start()
        self._collector = dev

    def __del__(self):
        self.close()

    def __getstate__(self):
        multiprocessing.context.assert_spawning(self)
        return (self.writer_port, self.reader_port, self.host,
                self._n_writers_opened, self._n_writers_closed)

    def __setstate__(self, state):
        (self.writer_port, self.reader_port, self.host,
            self._n_writers_opened, self._n_writers_closed) = state
        self._writer, self._reader = None, None
        self._closed = False
        self._collector = None

    def _get_writer(self):
        writer = self._writer
        if writer is None:
            context = zmq.Context()
            writer = context.socket(zmq.PUSH)  # pylint: disable=no-member
            writer.connect(f'{self.host}:{self.writer_port}')
            self._writer = writer
            with self._n_writers_opened.get_lock():
                self._n_writers_opened.value += 1
        return writer

    def _get_reader(self):
        reader = self._reader
        if reader is None:
            context = zmq.Context()
            reader = context.socket(zmq.PULL)  # pylint: disable=no-member
            reader.connect(f'{self.host}:{self.reader_port}')
            self._reader = reader
        return reader

    def put_bytes(self, data: bytes, block: bool = True, timeout: float = None):
        '''
        `timeout`: seconds
        '''
        if self._closed:
            raise ValueError(f"{self.__class__.__name__} {self!r} is closed")
        writer = self._get_writer()

        if not block:
            try:
                return writer.send(data, zmq.NOBLOCK)  # pylint: disable=no-member
            except ZMQError as e:
                # TODO: there can be other error conditions than Full.
                raise Full from e
        if timeout is not None:
            timeout *= 1000
        if writer.poll(timeout, zmq.POLLOUT):
            return writer.send(data)
        raise Full

    def put_bytes_nowait(self, data):
        return self.put_bytes(data, False)

    def put_many_bytes(self, data: Sequence, block=True, timeout=None):
        '''
        `timeout`: seconds
        '''
        if self._closed:
            raise ValueError(f"{self.__class__.__name__} {self!r} is closed")
        writer = self._get_writer()

        if not block:
            try:
                for v in data:
                    writer.send(v, zmq.NOBLOCK)  # pylint: disable=no-member
                return
            except ZMQError as e:
                # TODO: there can be other error conditions than Full.
                raise Full from e

        if timeout is None:
            for v in data:
                writer.send(v)
            return

        deadline = monotonic() + timeout
        for v in data:
            t = max(0, deadline - monotonic()) * 1000
            if writer.poll(t, zmq.POLLOUT):
                writer.send(v)
            else:
                raise Full

    def put_many_bytes_nowait(self, data):
        return self.put_many_bytes(data, False)

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
            with self._n_writers_closed.get_lock():
                self._n_writers_closed.value += 1
        self._closed = True

    def get_bytes(self, block: bool = True, timeout: float = None):
        '''
        `timeout`: seconds.
        '''
        reader = self._get_reader()

        if not block:
            try:
                return reader.recv(zmq.NOBLOCK)  # pylint: disable=no-member
            except ZMQError as e:
                raise Empty from e
        if timeout is not None:
            timeout *= 1000
        if reader.poll(timeout, zmq.POLLIN):
            return reader.recv()
        raise Empty

    def get_bytes_nowait(self):
        return self.get_bytes(False)

    def get_many_bytes(self, max_n: int, block=True, timeout=None) -> list:
        '''
        `timeout`: seconds.
        '''
        reader = self._get_reader()
        out = []
        n = 0

        if not block:
            try:
                while n < max_n:
                    out.append(reader.recv(zmq.NOBLOCK))  # pylint: disable=no-member
                    n += 1
                return out
            except ZMQError:
                if n:
                    return out
                return []

        if timeout is None:
            while n < max_n:
                out.append(reader.recv())
                n += 1
            return out

        deadline = monotonic() + timeout
        while n < max_n:
            t = max(0, deadline - monotonic()) * 1000
            if reader.poll(t, zmq.POLLIN):
                out.append(reader.recv())
                n += 1
            else:
                break
        return out

    def get_many_bytes_nowait(self, max_n: int):
        return self.get_many_bytes(max_n, False)

    def put(self, obj, block=True, timeout=None):
        data = pickle_dumps(obj)
        return self.put_bytes(data, block, timeout)

    def put_nowait(self, obj):
        return self.put(obj, False)

    def put_many(self, xs, block=True, timeout=None):
        data = [pickle_dumps(x) for x in xs]
        return self.put_many_bytes(data, block, timeout)

    def put_many_nowait(self, xs):
        return self.put_many(xs, False)

    def get(self, block=True, timeout=None):
        data = self.get_bytes(block, timeout)
        return pickle_loads(data)

    def get_nowait(self):
        return self.get(False)

    def get_many(self, max_n: int, block=True, timeout=None) -> list:
        z = self.get_many_bytes(max_n, block=block, timeout=timeout)
        return [pickle_loads(v) for v in z]

    def get_many_nowait(self, max_n: int) -> list:
        return self.get_many(max_n, False)


class FastQueue:
    def __init__(self, maxsize_bytes=10_000_000):
        self._q = faster_fifo.Queue(
            max_size_bytes=maxsize_bytes,
            dumps=pickle_dumps, loads=pickle_loads)
        self._q.reallocate_msg_buffer(1_000_000)

    def put(self, x, block=True, timeout=None):
        if timeout is None:
            timeout = float(3600)
        return self._q.put_many([x], block, timeout)

    def put_nowait(self, x):
        return self.put(x, False)

    def put_many(self, xs: Sequence, block=True, timeout=None):
        if block:
            if timeout is None:
                timeout = float(3600)
            deadline = monotonic() + timeout
        else:
            deadline = monotonic()

        xs = [xs]
        while xs:
            try:
                self._q.put_many(xs[0], timeout=0.01)
            except Full:
                if monotonic() > deadline:
                    raise Full
                if len(xs[0]) > 1:
                    k = int(len(xs[0]) / 2)
                    xs = [xs[0][:k], xs[0][k:]] + xs[1:]
                    # Split the data batch into smaller batches.
                    # This is in case the buffer is too small for the whole batch,
                    # hence would never succeed unless we split the batch into
                    # smaller chunks.
            else:
                xs = xs[1:]

    def put_many_nowait(self, xs):
        return self.put_many(xs, False)

    def get(self, block=True, timeout=None):
        if timeout is None:
            timeout = float(3600)
        z = self.get_many(1, block=block, timeout=timeout)
        if z:
            return z[0]
        raise Empty

    def get_nowait(self):
        return self.get(False)

    def get_many(self, max_n: int, block=True, timeout=None):
        if timeout is None:
            timeout = float(3600)
        try:
            return self._q.get_many(block=block, timeout=timeout, max_messages_to_get=max_n)
        except Empty:
            return []

    def get_many_nowait(self, max_n):
        return self.get_many(max_n, False)


def _BasicQueue(self, maxsize=0):
    return BasicQueue(maxsize=maxsize, ctx=self.get_context())


multiprocessing.context.BaseContext.BasicQueue = _BasicQueue


def _ZeroQueue(self, **kwargs):
    return ZeroQueue(ctx=self.get_context(), **kwargs)


multiprocessing.context.BaseContext.ZeroQueue = _ZeroQueue


def _FastQueue(self, **kwargs):
    return FastQueue(**kwargs)


multiprocessing.context.BaseContext.FastQueue = _FastQueue
