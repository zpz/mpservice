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
import pickle
import sys
import threading
import weakref
from collections import deque
from multiprocessing import context as mp_context
from multiprocessing.connection import Pipe, Connection
from multiprocessing.util import Finalize
from queue import Empty, Full
from time import monotonic
from typing import Protocol, Any, Sequence, List

logger = logging.getLogger(__name__)

_ForkingPickler = mp_context.reduction.ForkingPickler


# About changing pickle protocol for multiprocessing:
#  https://stackoverflow.com/questions/45119053/how-to-change-the-serialization-method-used-by-the-multiprocessing-module


class SingleLane:
    '''
    This queue has a single reader and a single writer, possibly in different threads.
    '''
    def __init__(self, maxsize=1_000_000):
        assert 0 <= maxsize
        self.maxsize = maxsize
        self._queue = deque()
        self._mutex = threading.Lock()
        self._not_empty = threading.Condition(self._mutex)
        self._not_full = threading.Condition(self._mutex)
        self._closed = False

    def put(self, item, block=True, timeout=None):
        if self._closed:
            raise ValueError(f"{self!r} is closed")
        with self._not_full:
            if 0 < self.maxsize <= len(self._queue):
                if not block:
                    raise Full
                if not self._not_full.wait(timeout=timeout):
                    raise Full
            self._queue.append(item)
            self._not_empty.notify()

    def get(self, block=True, timeout=None):
        if self._closed:
            raise ValueError(f"{self!r} is closed")
        with self._not_empty:
            if len(self._queue) == 0:
                if not block:
                    raise Empty
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
        # If `maxsize` is 0, there's no limit and it's never full.
        return 0 < self.maxsize <= len(self._queue)

    def qsize(self):
        return len(self._queue)

    def close(self):
        if self._closed:
            return
        if len(self._queue):
            # This is not necessarily an error, but user should understand
            # whether this is expected behavior in their particular application.
            logger.warning(f"{self!r} closed with {self.qsize()} data items un-consumed and abandoned")
        self._closed = True


class QueueWriter(Protocol):
    def put(self, obj: Any) -> None:
        pass

    def put_many(self, objs: Sequence) -> None:
        pass

    def full(self) -> bool:
        pass

    def close(self) -> None:
        pass


class QueueReader(Protocol):
    def get(self, *, timeout=None) -> Any:
        pass

    def get_many(self, max_n: int, *, first_timeout=None, extra_timeout=None) -> List[Any]:
        pass

    def empty(self) -> bool:
        pass

    def close(self) -> None:
        pass


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
    def __init__(self, reader, read_lock, buffer_size: int = None, batch_size: int = None):
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

        self._reader = reader
        self._lock = read_lock

        self._buffer_size = buffer_size
        self._batch_size = batch_size
        self._buffer = SingleLane(buffer_size)

        self._get_called = threading.Event()
        self._thread = threading.Thread(target=self._read, daemon=True)
        self._thread.start()

    def _read(self):
        try:
            buffer = self._buffer
            lock = self._lock
            getcalled = self._get_called
            closed = self._buffer._closed
            batchsize = self._batch_size

            if isinstance(self._reader, Connection):
                def recv(max_n):
                    # `max_n` is ignored
                    z = self._reader._recv_bytes()
                    return [z.getbuffer()]
            else:
                def recv(max_n):
                    z = self._reader.get_many(timeout=float(60), max_messages_to_get=int(max_n))
                    return [bytes(v) for v in z]

            while True:
                if buffer.full():
                    with buffer._not_full:
                        buffer._not_full.wait()
                try:
                    with lock:  # read lock
                        # In order to facilitate batching,
                        # we hold the lock and keep getting
                        # data off the pipe for this reader's buffer,
                        # even though there may be other readers waiting.
                        while True:
                            n = max(1, batchsize - buffer.qsize())
                            try:
                                xx = recv(n)
                            except EOFError:
                                return
                            except Empty:
                                break
                            for x in xx:
                                buffer.put(x)
                            if closed:
                                return
                            if getcalled.is_set():
                                # Once `get` or `get_many` has been called,
                                # we release the lock so that other readers
                                # get a chance for the lock.
                                getcalled.clear()
                                break
                            if buffer.qsize() >= batchsize:
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
        self._buffer.close()

    def __del__(self):
        self.close()

    def __getstate__(self):
        raise TypeError(f"{self.__class__.__name__} does not support pickling")

    def empty(self):
        return self._buffer.empty()

    def full(self):
        return self._buffer.full()

    def qsize(self):
        return self._buffer.qsize()

    def get(self, *, timeout=None):
        '''
        `get` and `get_many` are the only readers of `self._buffer`.
        Because this object is never shared between threads,
        ther are no concurrent calls to `get` and `get_many`.
        '''
        z = self._buffer.get(timeout=timeout)
        self._get_called.set()
        # return _ForkingPickler.loads(z)
        return pickle.loads(z)

    def get_many(self, max_n: int, *, first_timeout=None, extra_timeout=None):
        buffer = self._buffer
        out = [buffer.get(timeout=first_timeout)]
        n = 1

        if max_n > 1:
            if extra_timeout is None:
                extra_timeout = 60
            deadline = monotonic() + extra_timeout
            while n < max_n:
                t = deadline - monotonic()
                try:
                    z = buffer.get(timeout=t)
                except Empty:
                    break
                out.append(z)
                n += 1

        self._get_called.set()
        # return [_ForkingPickler.loads(v) for v in out]
        return [pickle.loads(v) for v in out]


try:
    import faster_fifo
    import faster_fifo_reduction  # noqa: F401

except ImportError:
    faster_fifo = None


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

        def __init__(self, writer, write_lock):
            self._writer = writer
            self._lock = write_lock  # this is None on win32
            self._buffer = SingleLane(0)
            self._closed = False
            self._start_thread()

        def _start_thread(self):
            sentinel = object()
            self._thread = threading.Thread(
                target=UniqueWriter._feed,
                args=(self._buffer, sentinel, self._lock, self._writer),
                daemon=True)
            self._thread.start()
            self._close = Finalize(
                self, UniqueWriter._finalize_close,
                (self._buffer, sentinel),
                exitpriority=10)
            self._jointhread = Finalize(
                self._thread, UniqueWriter._finalize_join,
                [weakref.ref(self._thread)],
                exitpriority=-5)

        @staticmethod
        def _feed(buffer, nomore, lock, writer):
            # This is the only reader of `self._buffer`.
            try:
                while True:
                    x = buffer.get()
                    if x is nomore:
                        return
                    xx = _ForkingPickler.dumps(x)
                    if lock is None:
                        writer.send_bytes(xx)
                    else:
                        with lock:
                            writer.send_bytes(xx)
            except Exception as e:
                logger.exception(e)
                raise

        @staticmethod
        def _finalize_join(twr):
            thread = twr()
            if thread is not None:
                thread.join()

        @staticmethod
        def _finalize_close(buffer, nomore):
            buffer.put(nomore)

        def close(self):
            self._closed = True
            close = self._close
            if close:
                self._close = None
                close()

        def __getstate__(self):
            raise TypeError(f"{self.__class__.__name__} does not support pickling")

        def put(self, obj):
            # This function does not block.
            # This is the only writer to `self._buffer`.
            self._buffer.put(obj)

        def put_many(self, objs):
            for obj in objs:
                self.put(obj)

        def full(self):
            return self._buffer.full()


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

else:

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

        def __init__(self, writer):
            self._writer = writer
            self._closed = False

        def close(self):
            if self._closed:
                return
            self._writer.close()
            self._closed = True

        def __getstate__(self):
            raise TypeError(f"{self.__class__.__name__} does not support pickling")

        def put(self, obj):
            # x = _ForkingPickler.dumps(obj)
            x = pickle.dumps(obj)
            self._writer.put(x)

        def put_many(self, objs):
            if self._closed:
                raise ValueError(f"{self!r} is closed")

            # xx = [_ForkingPickler.dumps(x) for x in objs]

            objs = [pickle.dumps(x) for x in objs]
            while objs:
                try:
                    self._writer.put_many(objs[0], timeout=0.01)
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

        def full(self):
            return self._writer.full()


    def passthrough(x):
        return x


    class Unique:
        '''
        Naming follows the example of `deque`:

            'de queue'  --> 'deque'
            'uni queue' --> 'unique'
        '''
        def __init__(self, *, ctx=None):
            self._q = faster_fifo.Queue(max_size_bytes=10_000_000, loads=passthrough, dumps=passthrough)
            if ctx is None:
                ctx = multiprocessing.get_context()
            self._rlock = ctx.Lock()
            self._closed = False

        def __getstate__(self):
            mp_context.assert_spawning(self)
            return (self._q, self._rlock)

        def __setstate__(self, state):
            (self._q, self._rlock) = state
            self._closed = None

        def writer(self):
            return UniqueWriter(self._q)

        def reader(self, buffer_size=None, batch_size=None):
            return UniqueReader(self._q, self._rlock, buffer_size, batch_size)

        def close(self):
            # This may be called only in the process where this object was created.
            if self._closed is None:
                return
            if self._closed:
                return
            self._q.close()
            self._closed = True

        def __del__(self):
            if self._closed is not None:
                # In the process where the object was created.
                self.close()


def _Unique(self, **kwargs):
    return Unique(ctx=self.get_context(), **kwargs)

mp_context.BaseContext.Unique = _Unique

