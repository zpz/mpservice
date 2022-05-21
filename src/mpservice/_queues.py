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
import threading
from collections import deque
from multiprocessing import context as mp_context
from queue import Empty, Full

import faster_fifo
import faster_fifo_reduction  # noqa: F401

logger = logging.getLogger(__name__)


# About changing pickle protocol for multiprocessing:
#  https://stackoverflow.com/questions/45119053/how-to-change-the-serialization-method-used-by-the-multiprocessing-module
#  socket.setsockopt(level, optname, value: int)
# You can set the socket buffer size with setsockopt() using the SO_RCVBUFSIZ or SO_SNDBUFSIZ options

# About socket buffer size:
# https://stackoverflow.com/questions/2811006/what-is-a-good-buffer-size-for-socket-programming


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


class Unique:
    '''
    Naming follows the example of `deque`:

        'de queue'  --> 'deque'
        'uni queue' --> 'unique'
    '''
    def __init__(self, *, ctx=None, maxsize_bytes: int = 10_000_000):
        if ctx is None:
            ctx = multiprocessing.get_context()
        self._q = faster_fifo.Queue(max_size_bytes=maxsize_bytes)
        self._rlock = ctx.Lock()
        self._closed = False

    def __getstate__(self):
        mp_context.assert_spawning(self)
        return (self._q, self._rlock)

    def __setstate__(self, state):
        (self._q, self._rlock) = state
        self._closed = None

    def __del__(self):
        self.close()

    def close(self):
        if self._closed is None:
            # Not in the process where the Queue was created.
            return
        if self._closed:
            return
        self._q.close()
        self._closed = True

    def empty(self):
        return self._q.empty()

    def full(self):
        return self._q.full()

    def get(self, *, timeout=None):
        if self._closed:
            raise ValueError(f"{self!r} is closed")
        if timeout is None:
            timeout = 3600 * 24
        return self._q.get(timeout=float(timeout))

    def get_many(self, *, timeout=None, **kwargs):
        if self._closed:
            raise ValueError(f"{self!r} is closed")
        if timeout is None:
            timeout = 3600 * 24
        return self._q.get_many(timeout=float(timeout), **kwargs)

    def put(self, obj, *, timeout=None):
        if self._closed:
            raise ValueError(f"{self!r} is closed")
        if timeout is None:
            timeout = 3600 * 24
        self._q.put(obj, timeout=float(timeout))

    def put_many(self, objs, *, timeout=None):
        if self._closed:
            raise ValueError(f"{self!r} is closed")
        if timeout is None:
            timeout = 3600 * 24

        self._q.put_many(objs, timeout=float(timeout))

        # # This is safe against extemely large data, but
        # # in this way we can't control timeout.
        # xx = [objs]
        # while xx:
        #     try:
        #         self._q.put_many(xx[0], timeout=0.01)
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


def _Unique(self, **kwargs):
    return Unique(ctx=self.get_context(), **kwargs)


mp_context.BaseContext.Unique = _Unique
