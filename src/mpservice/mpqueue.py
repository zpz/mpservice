import concurrent.futures
import multiprocessing
import threading
from multiprocessing import queues
from pickle import dumps as pickle_dumps, loads as pickle_loads, HIGHEST_PROTOCOL
from queue import Empty, Full
from time import monotonic, sleep

import zmq
from zmq import ZMQError


class _FastQueue(queues.SimpleQueue):
    def __init__(self, *, ctx):
        # if ctx is None:
        #    ctx = context._default_context
        super().__init__(ctx=ctx)
        self._closed = ctx.Event()

    def __getstate__(self):
        z = super().__getstate__()
        return (*z, self._closed)

    def __setstate__(self, state):
        self._closed = state[-1]
        super().__setstate__(state[:-1])

    def get_bytes(self, block=True, timeout=None):
        try:
            if block and timeout is None:
                with self._rlock:
                    return self._reader.recv_bytes()
            if block:
                deadline = monotonic() + timeout
            if not self._rlock.acquire(block, timeout):
                raise Empty
            try:
                if block:
                    timeout = deadline - monotonic()
                    if not self._poll(timeout):
                        raise Empty
                elif not self._poll():
                    raise Empty
                return self._reader.recv_bytes()
            finally:
                self._rlock.release()
        except OSError as e:
            if str(e) == 'handle is closed':
                # `.close()` has been called in the same process.
                raise BrokenPipeError(f"Queue {self!r} is closed") from e
            raise
        except Empty:
            if self.closed():
                # `.close()` has been called in another process.
                raise BrokenPipeError(f"Queue {self!r} is closed")
            raise

    def get_bytes_nowait(self):
        return self.get_bytes(False)

    def get(self, block=True, timeout=None):
        res = self.get_bytes(block, timeout)
        return pickle_loads(res)

    def get_nowait(self):
        return self.get(False)

    def get_many(self, nmax: int, block=True, timeout=None) -> list:
        out = []
        try:
            if block and timeout is not None:
                deadline = monotonic() + timeout
            if not self._rlock.acquire(block, timeout):
                raise Empty
            try:
                n = 0
                while n < nmax:
                    if block:
                        if timeout is None:
                            x = self._reader.recv_bytes()
                        else:
                            if not self._poll(max(0, deadline - monotonic())):
                                raise Empty
                            x = self._reader.recv_bytes()
                    else:
                        if not self._poll():
                            raise Empty
                        x = self._reader.recv_bytes()
                    out.append(x)
                    n += 1
            finally:
                self._rlock.release()
        except OSError as e:
            if str(e) == 'handle is closed':
                # `.close()` has been called in the same process.
                raise BrokenPipeError(f"Queue {self!r} is closed") from e
            raise
        except Empty:
            if self.closed():
                # `.close()` has been called in another process.
                raise BrokenPipeError(f"Queue {self!r} is closed")

        if out:
            return [pickle_loads(o) for o in out]
        raise Empty()

    def get_many_nowait(self, nmax: int) -> list:
        return self.get_many(nmax, False)

    def put_bytes(self, obj, block=True, timeout=None):
        try:
            if self._wlock is None:
                self._writer.send_bytes(obj)
            else:
                if not self._wlock.acquire(block, timeout):
                    raise Full
                try:
                    self._writer.send_bytes(obj)
                finally:
                    self._wlock.release()
        except OSError as e:
            if str(e) == 'handle is closed':
                # `.close()` has been called in the same process.
                raise BrokenPipeError(f"Queue {self!r} is closed") from e
            raise
        except Full:
            if self.closed():
                # `.close()` has been called in another process.
                raise BrokenPipeError(f"Queue {self!r} is closed")
            raise

    def put_bytes_nowait(self, obj):
        self.put_bytes(obj, False)

    def put(self, obj, block=True, timeout=None):
        obj = pickle_dumps(obj, protocol=5)
        self.put_bytes(obj, block, timeout)

    def put_nowait(self, obj):
        self.put(obj, False)

    def close(self):
        # Based on very limited testing, after calling `close()`,
        # in the same process (on the same object), `get` and `put`
        # will raise OSError; in another process, however, `get`
        # and `put` will block forever unless `timeout` is used
        # (upon timeout, closed-ness is checked).
        self._reader.close()
        self._writer.close()
        self._closed.set()

    def closed(self):
        return self._closed.is_set()


def _FastQueue_(self):
    return _FastQueue(ctx=self.get_context())


multiprocessing.context.BaseContext.FastQueue = _FastQueue_
FastQueue = multiprocessing.context.BaseContext.FastQueue


class ZeroQueue:
    def __init__(self,
                 writer_port: int,
                 reader_port: int,
                 *,
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
        assert writer_port != reader_port
        self.writer_port = writer_port
        self.reader_port = reader_port
        self.host = host

        self._writer = None
        self._reader = None
        self._closed = False

        # Keep track of writers on this queue across processes,
        # not only in "current" process.
        if ctx is None:
            ctx = multiprocessing.get_context()
        # self._n_writers_opened = ctx.Value('i', 0)
        # self._n_writers_closed = ctx.Value('i', 0)
        self._n_writers_opened = multiprocessing.Value('i', 0)
        self._n_writers_closed = multiprocessing.Value('i', 0)

        # The following are used only in the "originating" object.
        self._collector = self._start(writer_hwm, reader_hwm)

    def __del__(self):
        print(multiprocessing.current_process().name, '__del__')
        self.close()
        if self._collector is not None:
            # This is the "originating" object.
            if self._n_writers_opened.value < 1:
                assert self._n_writers_closed.value == 0
            else:
                while self._n_writers_closed.value < self._n_writers_opened.value:
                    sleep(0.01)
            # self._collector[0].close(10)
            # self._collector[1].close(10)

    def _start(self, writer_hwm, reader_hwm):
        # This is called by `__init__` when this object is constructed
        # "fresh" in a process. This is not called when this object
        # is passed into another process and re-created via unpickling
        # in the other process.
        # The "original" object in the initiating process should
        # remain valid throughout the life of this queue.

        # TODO: look into "proxy" or "device" of the "queue" type.
        context = zmq.Context.instance()
        receiver = context.socket(zmq.PULL)
        # receiver.set(zmq.IMMEDIATE, True)
        # receiver.hwm = writer_hwm
        receiver.bind(f'{self.host}:{self.writer_port}')
        sender = context.socket(zmq.PUSH)
        # sender.set(zmq.IMMEDIATE, True)
        # sender.hwm = reader_hwm
        sender.bind(f'{self.host}:{self.reader_port}')

        def foo():
            print('-- foo --')
            while True:
                z = receiver.recv()
                sender.send(z)

        t = threading.Thread(target=foo, daemon=True)
        t.start()
        return (receiver, sender)

    def __getstate__(self):
        multiprocessing.context.assert_spawning(self)
        return (self.writer_port, self.reader_port, self.host,
                self._n_writers_opened, self._n_writers_closed)

    def __setstate__(self, state):
        (self.writer_port, self.reader_port, self.host,
                self._n_writers_opened, self._n_writers_closed) = state
        self._writer, self._reader, self._closed = None, None, False
        self._collector = None

    def put_bytes(self, data: bytes, block: bool = True, timeout: float = None):
        '''
        `timeout`: seconds
        '''
        print('put_bytes, host', self.host, 'writer_port', self.writer_port, 'reader_port', self.reader_port)
        if self._closed:
            raise ValueError(f"{self.__class__.__name__} {self!r} is closed")
        writer = self._writer
        if writer is None:
            context = zmq.Context.instance()
            writer = context.socket(zmq.PUSH)
            writer.connect(f'{self.host}:{self.writer_port}')
            self._writer = writer
            with self._n_writers_opened.get_lock():
                self._n_writers_opened.value += 1

        if not block:
            try:
                return writer.send(data, zmq.NOBLOCK)
            except ZMQError as e:
                # TODO: there can be other error conditions than Full.
                raise Full() from e
        if timeout is not None:
            timeout *= 1000
        if writer.poll(timeout, zmq.POLLOUT):
            return writer.send(data)
        raise Full()

    def put_bytes_nowait(self, data):
        return self.put_bytes(data, False)

    def put(self, obj, block=True, timeout=None):
        data = pickle_dumps(obj, protocol=HIGHEST_PROTOCOL)
        return self.put_bytes(data, block, timeout)

    def put_nowait(self, obj):
        return self.put(obj, False)

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
        print(multiprocessing.current_process().name,
                'close, n_writers:', self._n_writers_opened.value, self._n_writers_closed.value)

    def get_bytes(self, block: bool = True, timeout: float = None):
        '''
        `timeout`: seconds.
        '''
        reader = self._reader
        if reader is None:
            context = zmq.Context.instance()
            reader = context.socket(zmq.PULL)
            reader.connect(f'{self.host}:{self.reader_port}')
            self._reader = reader

        if not block:
            try:
                return reader.recv(zmq.NOBLOCK)
            except ZMQError as e:
                raise Empty() from e
        if timeout is not None:
            timeout *= 1000
        if reader.poll(timeout, zmq.POLLIN):
            return reader.recv()
        raise Empty()

    def get_bytes_nowait(self):
        return self.get_bytes(False)

    def get(self, block=True, timeout=None):
        data = self.get_bytes(block, timeout)
        return pickle_loads(data)

    def get_nowait(self):
        return self.get(False)


def _ZeroQueue(self, writer_port, reader_port, *, host='tcp://0.0.0.0'):
    return ZeroQueue(writer_port, reader_port, host=host, ctx=self.get_context())


multiprocessing.context.BaseContext.ZeroQueue = _ZeroQueue
