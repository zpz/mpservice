'''
This module defines a few alternatives to the standard multiprocessing or threading queues for better performance.
They are purpose-built for particular use cases in this package.
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
import sys
import threading
import weakref
from collections import deque
from multiprocessing import context as mp_context
from multiprocessing.connection import Pipe
from multiprocessing.util import Finalize
from queue import Empty, Full
from time import monotonic
from typing import Protocol, Any, Sequence, List

logger = logging.getLogger(__name__)

_ForkingPickler = mp_context.reduction.ForkingPickler


# About changing pickle protocol for multiprocessing:
#  https://stackoverflow.com/questions/45119053/how-to-change-the-serialization-method-used-by-the-multiprocessing-module


class QueueWriter(Protocol):
    def put(self, obj: Any, *, timeout=None) -> None:
        pass

    def put_many(self, objs: Sequence, *, timeout=None) -> None:
        pass

    def close(self) -> None:
        pass


class QueueReader(Protocol):
    def get(self, *, timeout=None) -> Any:
        pass

    def get_many(self, max_n: int, *, first_timeout=None, extra_timeout=None) -> List[Any]:
        pass

    def close(self) -> None:
        pass


class Unique:
    '''
    Naming follows the example of `deque`:

        'de queue'  --> 'deque'
        'uni queue' --> 'unique'
    '''
    def __init__(self, *, ctx=None):
        if ctx is None:
            ctx = multiprocessing.get_context()
        self._reader, self._writer = Pipe(duplex=False)
        self._rlock = ctx.Lock()
        if sys.platform == 'win32':
            self._wlock = None
        else:
            self._wlock = ctx.Lock()
        self._closed = False

    def __getstate__(self):
        mp_context.assert_spawning(self)
        return (self._reader, self._writer, self._rlock, self._wlock)

    def __setstate__(self, state):
        (self._reader, self._writer, self._rlock, self._wlock) = state
        self._closed = None

    def writer(self):
        return UniqueWriter(self._writer, self._wlock)

    def reader(self, buffer_size=None, batch_size=None):
        return UniqueReader(self._reader, self._rlock, buffer_size, batch_size)

    def close(self):
        # This may be called only in the process where this object was created.
        if self._closed is None:
            return
        if self._closed:
            return
        self._writer.close()
        self._reader.close()
        self._closed = True

    def __del__(self):
        if self._closed is not None:
            # In the process where the object was created.
            self.close()


class UniqueWriter:
    '''
    This queue-writer has no size limit.
    Its `put` method does not block.

    This object is not shared between threads,
    and cannot be passed across processes.
    An instance is created by `Unique.writer()`,
    either in the Unique-creating process or in another
    process (into which a `Unique` has been passed).

    Refer to source of `multiprocessing.queues.Queue`.
    '''

    def __init__(self, pipe_writer, write_lock):
        self._writer = pipe_writer
        self._lock = write_lock  # this is None on win32
        self._buffer = deque()
        self._not_empty = threading.Condition()
        self._closed = False
        self._start_thread()

    def _start_thread(self):
        sentinel = object()
        self._thread = threading.Thread(
            target=UniqueWriter._feed,
            args=(self._buffer, self._not_empty, sentinel, self._lock, self._writer),
            daemon=True)
        self._thread.start()
        self._close = Finalize(
            self, UniqueWriter._finalize_close,
            (self._buffer, self._not_empty, sentinel),
            exitpriority=10)
        self._jointhread = Finalize(
            self._thread, UniqueWriter._finalize_join,
            [weakref.ref(self._thread)],
            exitpriority=-5)

    @staticmethod
    def _feed(buffer, not_empty, nomore, lock, writer):
        # This is the only reader of `self._buffer`.
        while True:
            if not buffer:
                with not_empty:
                    not_empty.wait()
            x = buffer.popleft()
            if x is nomore:
                return
            xx = _ForkingPickler.dumps(x)
            if lock is None:
                writer.send_bytes(xx)
            else:
                with lock:
                    writer.send_bytes(xx)

    @staticmethod
    def _finalize_join(twr):
        thread = twr()
        if thread is not None:
            thread.join()

    @staticmethod
    def _finalize_close(buffer, notempty, nomore):
        with notempty:
            buffer.append(nomore)
            notempty.notify()

    def close(self):
        self._closed = True
        close = self._close
        if close:
            self._close = None
            close()

    def __getstate__(self):
        raise TypeError(f"{self.__class__.__name__} does not support pickling")

    def put(self, obj, *, timeout=None):
        # `timeout` is ignored. This function does not block.
        # This is the only writer to `self._buffer`.
        if self._closed:
            raise ValueError(f"{self!r} is closed")
        with self._not_empty:
            self._buffer.append(obj)
            self._not_empty.notify()

    def put_many(self, objs):
        # `timeout` is ignored. This function does not block.
        for obj in objs:
            self.put(obj)


class UniqueReader:
    '''
    This queue-reader uses a size-capped background thread.

    Similar to `UniqueWriter`,
    this object is not shared between threads,
    and cannot be passed across processes.
    An instance is created via `Unique.reader()`,
    either in the Unique-creating process or in another
    process (into which a `Unique` has been passed).
    '''
    def __init__(self, pipe_reader, read_lock, buffer_size: int = None, batch_size: int = None):
        if buffer_size is None:
            if batch_size is None:
                buffer_size = 1024
                batch_size = 1
            else:
                assert 1 <= batch_size
                buffer_size = batch_size
        else:
            assert 1 <= buffer_size
            if batch_size is None:
                batch_size = 1
            else:
                assert batch_size <= buffer_size

        self._reader = pipe_reader
        self._lock = read_lock
        self._buffer_size = buffer_size
        self._batch_size = batch_size

        self._buffer = deque()
        self._mutex = threading.Lock()
        self._not_empty = threading.Condition(self._mutex)
        self._not_full = threading.Condition(self._mutex)

        self._get_called = threading.Event()
        self._closed = False
        self._thread = threading.Thread(target=self._read, daemon=True)
        self._thread.start()

    def _read(self):
        try:
            buffer = self._buffer
            lock = self._lock
            getcalled = self._get_called
            closed = self._closed
            buffersize = self._buffer_size
            batchsize = self._batch_size
            notfull = self._not_full
            notempty = self._not_empty
            recv = self._reader._recv_bytes
            while True:
                if 0 < buffersize <= len(buffer):
                    with notfull:
                        notfull.wait()
                try:
                    with lock:  # read lock
                        # In order to facilitate batching,
                        # we hold the lock and keep getting
                        # data off the pipe for this reader's buffer,
                        # even though there may be other readers waiting.
                        while True:
                            try:
                                x = recv()
                            except EOFError:
                                return
                            x = x.getbuffer()
                            with notempty:
                                buffer.append(x)
                                notempty.notify()
                            if closed:
                                return
                            if getcalled.is_set():
                                # Once `get` or `get_many` has been called,
                                # we release the lock so that other readers
                                # get a chance for the lock.
                                getcalled.clear()
                                break
                            if len(buffer) >= batchsize:
                                break

                except Exception as e:
                    if closed:
                        break
                    logger.error(e)
                    raise
        except Exception as e:
            logger.exception(e)
            raise

    def close(self):
        if self._closed:
            return
        self._closed = True
        if self.qsize():
            # This is not necessarily an error, but user should understand
            # whether this is expected behavior in their particular application.
            logger.warning(f"{self!r} closed with {self.qsize()} data items un-consumed and abandoned")

    def __del__(self):
        self.close()

    def __getstate__(self):
        raise TypeError(f"{self.__class__.__name__} does not support pickling")

    def empty(self):
        return len(self._buffer) == 0

    def full(self):
        return 0 < self._buffer_size <= len(self._buffer)

    def qsize(self):
        return len(self._buffer)

    def get(self, *, timeout=None):
        '''
        `get` and `get_many` are the only readers of `self._buffer`.
        Because this object is never shared between threads,
        ther are no concurrent calls to `get` and `get_many`.
        '''
        if self._closed:
            raise ValueError(f"{self!r} is closed")
        with self._not_empty:
            if len(self._buffer) == 0:
                if timeout is None:
                    timeout = 3600
                elif timeout <= 0:
                    raise Empty
                if not self._not_empty.wait(timeout=timeout):
                    raise Empty
            z = self._buffer.popleft()
            self._not_full.notify()
        self._get_called.set()
        return _ForkingPickler.loads(z)

    def get_many(self, max_n: int, *, first_timeout=None, extra_timeout=None):
        if self._closed:
            raise ValueError(f"{self!r} is closed")
        buffer = self._buffer
        notfull = self._not_full
        notempty = self._not_empty
        if len(buffer) == 0:
            if first_timeout is None:
                first_timeout = 3600
            with notempty:
                if not notempty.wait(timeout=first_timeout):
                    raise Empty
        with notfull:
            out = [buffer.popleft()]
            notfull.notify()
        n = 1

        if max_n > 1:
            if extra_timeout is None:
                extra_timeout = 3600
            deadline = monotonic() + extra_timeout
            while n < max_n:
                if len(buffer) == 0:
                    t = max(0., deadline - monotonic())
                    with notempty:
                        if not notempty.wait(timeout=t):
                            break
                # while len(buffer) and n < max_n:
                #     out.append(buffer.popleft())
                #     n += 1
                # with notfull:  # for optim performance, do not notify after every item
                #     notfull.notify()
                while len(buffer) and n < max_n:
                    with notfull:
                        z = buffer.popleft()
                        notfull.notify()
                    out.append(z)
                    n += 1

        self._get_called.set()
        return [_ForkingPickler.loads(v) for v in out]


def _Unique(self):
    return Unique(ctx=self.get_context())


mp_context.BaseContext.Unique = _Unique


try:
    import faster_fifo
    import faster_fifo_reduction  # noqa: F401
except ImportError:
    faster_fifo = None
else:
    class FastQueue:
        def __init__(self, maxsize_bytes=10_000_000):
            self._q = faster_fifo.Queue(max_size_bytes=maxsize_bytes)

        def empty(self):
            return self._q.empty()

        def close(self):
            return self._q.close()

        def put(self, obj, *, timeout=None):
            if self._q.is_closed():
                raise ValueError(f"{self!r} is closed")
            if timeout is None:
                timeout = 3600
            else:
                timeout = max(0, timeout)
            self._q.put(obj, block=True, timeout=timeout)

        def get(self, *, timeout=None):
            if timeout is None:
                timeout = float(3600)
            else:
                timeout = max(0, timeout)
            if self._q.message_buffer is None:
                self._q.reallocate_msg_buffer(1_000_000)
            return self._q.get(block=True, timeout=timeout)

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

    def _FastQueue(self, **kwargs):
        return FastQueue(**kwargs)

    mp_context.BaseContext.FastQueue = _FastQueue


class BoundedSimpleQueue:
    '''
    The standard `queue.SimpleQueue` is much faster than `queue.Queue` but
    lacks the capability of maxsize control. This class adds a `maxsize`
    parameter to `SimpleQueue`.

    TODO: benchmark.
    '''
    def __init__(self, maxsize=0):
        self.maxsize = maxsize
        self._queue = deque()
        self._mutex = threading.Lock()
        self._not_empty = threading.Condition(self._mutex)
        self._not_full = threading.Condition(self._mutex)

    def put(self, item, block=True, timeout=None):
        with self._not_full:
            if 0 < self.maxsize <= len(self._queue):
                if timeout is None:
                    timeout = 3600
                else:
                    timeout = max(0, timeout)
                if not self._not_full.wait(timeout=timeout):
                    raise Full
            self._queue.append(item)
            self._not_empty.notify()

    def get(self, block=True, timeout=None):
        with self._not_empty:
            if len(self._queue) == 0:
                if timeout is None:
                    timeout = 3600
                else:
                    timeout = max(0, timeout)
                if not self._not_empty.wait(timeout=timeout):
                    raise Empty
            z = self._queue.popleft()
            self._not_full.notify()
        return z

    def put_nowait(self, item):
        self.put(item, False)

    def get_nowait(self):
        return self.get(False)

    def empty(self):
        return len(self._queue) == 0

    def full(self):
        return 0 < self.maxsize <= len(self._queue)

    def qsize(self):
        return len(self._queue)
