# Queue performance benchmarking:
#   https://stackoverflow.com/questions/8463008/multiprocessing-pipe-vs-queue
# quick-quque: https://github.com/Invarato/quick_queue_project
# queue/pipe/zmq benchmarking: https://gist.github.com/kylemcdonald/a2f0dcb86f01d4c57b68ac6a6c7a3068
# https://stackoverflow.com/questions/47085458/why-is-multiprocessing-queue-get-so-slow
# https://stackoverflow.com/questions/43439194/python-multiprocessing-queue-vs-multiprocessing-manager-queue/45236748#45236748
# https://stackoverflow.com/questions/23961669/how-can-i-speed-up-simultaneous-read-and-write-of-multiprocessing-queues
# https://stackoverflow.com/questions/60197392/high-performance-replacement-for-multiprocessing-queue

import logging
import threading
from collections import deque
from queue import Empty, Full

logger = logging.getLogger(__name__)

# Interesting: faster_fifo

# About changing pickle protocol for multiprocessing:
#  https://stackoverflow.com/questions/45119053/how-to-change-the-serialization-method-used-by-the-multiprocessing-module
#  socket.setsockopt(level, optname, value: int)
# You can set the socket buffer size with setsockopt() using the SO_RCVBUFSIZ or SO_SNDBUFSIZ options

# About socket buffer size:
# https://stackoverflow.com/questions/2811006/what-is-a-good-buffer-size-for-socket-programming


class SingleLane:
    """
    This queue has a single reader and a single writer, possibly in different threads.
    """

    def __init__(self, maxsize=1_000_000):
        """
        Parameters
        ----------
        maxisze
            Max number of elements in the queue. 0 means no limit.
        """
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
            logger.warning(
                "%r closed with %d data items un-consumed and abandoned",
                self,
                self.qsize(),
            )
        self._closed = True
