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
from multiprocessing.connection import Pipe, Connection
from multiprocessing.util import Finalize
from queue import Empty, Full
from time import monotonic

logger = logging.getLogger(__name__)

_ForkingPickler = mp_context.reduction.ForkingPickler


# About changing pickle protocol for multiprocessing:
#  https://stackoverflow.com/questions/45119053/how-to-change-the-serialization-method-used-by-the-multiprocessing-module


class SingleLane:
    '''
    This queue has a single reader and a single writer, possibly in different threads.
    '''
    def __init__(self, maxsize=1_000_000):
        '''
        `maxisze`: max number of elements in the queue. `0` means no limit.
        '''
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


class UniqueReader:
    '''
    This queue-reader uses a size-capped background thread.

    This object is not shared between threads,
    and cannot be passed across processes.
    An instance is created via `Unique.reader()`,
    either in the Unique-creating process or in another
    process (into which a `Unique` has been passed).
    '''
    def __init__(self, reader, read_lock, buffer_size: int = None, batch_size: int = None):
        # `reader` is either a `Connection` or a `faster_fifo.Queue`.
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
            reader = self._reader
            if isinstance(reader, Connection):
                is_pipe = True
            else:
                # `faster_fifo.Queue`
                is_pipe = False
                if reader.message_buffer is None:
                    reader.reallocate_msg_buffer(1_000_000)

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
                            if is_pipe:
                                try:
                                    z = reader._recv_bytes()
                                    # This will wait forever if there's nothing
                                    # to read.
                                except EOFError:
                                    return
                                buffer.put(z.getbuffer())
                            else:
                                n = max(1, batchsize - buffer.qsize())
                                try:
                                    # `faster_fifo.Queue.get_many` will wait
                                    # up to the specified timeout if there's nothing;
                                    # once there's something to read, it will
                                    # read up to the max count that's already there
                                    # to be read---importantly, it will not spread
                                    # out the wait in order to collect more elements.
                                    z = reader.get_many(
                                        block=True,
                                        timeout=float(60),
                                        max_messages_to_get=int(n))
                                except Empty:
                                    pass
                                else:
                                    for x in z:
                                        buffer.put(bytes(x))
                                        # TODO: understand the need for `bytes` here
                                        # and compare with `getbuffer` above.
                                        # Is there overhead? Can this be simplified?
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

    # def full(self):
    #     return self._buffer.full()

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
        return _ForkingPickler.loads(z)
        # return pickle.loads(z)

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
        return [_ForkingPickler.loads(v) for v in out]
        # return [pickle.loads(v) for v in out]


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

        def __init__(self, writer: Connection, write_lock):
            self._writer = writer
            self._lock = write_lock  # this is None on win32
            self._buffer = SingleLane(0)  # no size limit
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
                    obj = buffer.get()
                    if obj is nomore:
                        return
                    x = _ForkingPickler.dumps(obj)
                    if lock is None:
                        writer.send_bytes(x)
                    else:
                        with lock:
                            writer.send_bytes(x)
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

        def put(self, obj, *, timeout=None):
            # `timeout` is ignored.
            # This function does not block.
            # This is the only writer to `self._buffer`.
            self._buffer.put(obj)

        def put_many(self, objs, *, timeout=None):
            # `timeout` is ignored.
            # This function does not block.
            for obj in objs:
                self._buffer.put(obj)

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
        This object is not shared between threads,
        and cannot be passed across processes.
        An instance is created by `Unique.writer()`,
        either in the Unique-creating process or in another
        process (into which a `Unique` has been passed).
        '''

        def __init__(self, writer: faster_fifo.Queue):
            self._writer = writer
            self._closed = False

        def close(self):
            if self._closed:
                return
            self._closed = True

        def __getstate__(self):
            raise TypeError(f"{self.__class__.__name__} does not support pickling")

        def put(self, obj, *, timeout=None):
            if self._closed:
                raise ValueError(f"{self!r} is closed")
            if timeout is None:
                timeout = 3600 * 24
            x = bytes(_ForkingPickler.dumps(obj))
            self._writer.put(x, timeout=float(timeout))

        def put_many(self, objs, *, timeout=None):
            if self._closed:
                raise ValueError(f"{self!r} is closed")
            if timeout is None:
                timeout = 3600 * 24

            xx = [bytes(_ForkingPickler.dumps(obj)) for obj in objs]
            self._writer.put_many(xx, timeout=float(timeout))

            # # This is safe against extemely large data, but
            # # in this way we can't control timeout.
            # xx = [xx]
            # while xx:
            #     try:
            #         self._writer.put_many(xx[0], timeout=0.01)
            #     except Full:
            #         if len(xx[0]) > 1:
            #             k = int(len(xx[0]) / 2)
            #             objs = [xx[0][:k], xx[0][k:]] + xx[1:]
            #             # Split the data batch into smaller batches.
            #             # This is in case the buffer is too small for the whole batch,
            #             # hence would never succeed unless we split the batch into
            #             # smaller chunks.
            #     else:
            #         xx = xx[1:]

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
        def __init__(self, *, ctx=None, maxsize_bytes: int = 10_000_000):
            if ctx is None:
                ctx = multiprocessing.get_context()
            self._q = faster_fifo.Queue(
                max_size_bytes=maxsize_bytes, loads=passthrough, dumps=passthrough)
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
            if self._closed is None:
                # Not in the process where the Queue was created.
                return
            if self._closed:
                return
            self._q.close()
            self._closed = True

        def __del__(self):
            self.close()


def _Unique(self, **kwargs):
    return Unique(ctx=self.get_context(), **kwargs)


mp_context.BaseContext.Unique = _Unique
