from threading import Lock
from time import perf_counter, sleep


class Full(Exception):
    pass


class Empty(Exception):
    pass


class Ring:
    def __init__(self, maxlen: int):
        self._maxlen = maxlen
        self._n = 0
        self._head = 0  # index of the first/oldest element; -1 means no element; range [-1, maxlen)
        self._tail = 0  # one after the last/newest element; also the index of the next element to be pushed; range [0, maxlen)
        self._data = [None for _ in range(maxlen)]

    def __len__(self):
        return self._n

    def full(self):
        return self.__len__() == self._maxlen

    def empty(self):
        return self.__len__() == 0

    def push(self, x):
        # Push an element at the "tail" (i.e. latest) end.
        if self.full():
            raise Full
        t = self._tail
        self._data[t] = x
        self._n += 1
        t += 1
        if t == self._maxlen:
            t = 0
        self._tail = t

    def pop(self):
        # Remove and return the "head" (i.e. oldest) element.
        if self.empty():
            raise Empty
        h = self._head
        x = self._data[h]
        self._n -= 1
        h += 1
        if h == self._maxlen:
            h = 0
        self._head = h
        return x

    def head(self):
        # Return (w/o removing) the oldest element.
        if self.empty():
            raise Empty
        return self._data[self._head]

    def tail(self):
        # Return (w/o removing) the latest element.
        if self.empty():
            raise Empty
        t = self._tail - 1
        if t < 0:
            t = self._maxlen - 1
        return self._data[t]


class RateLimiter:
    """
    This class is used to impose rate limits in one or more threads.
    If you need rate limiting across processes, you can use this facility
    in a multiprocessing "manager".
    """

    def __init__(self, limit: int, time_window_in_seconds: float | int = 1):
        """
        Suppose ``limit = 10`` and ``time_window_in_seconds=60``, that specifies a
        rate limit on a minute basis. Specifically, it means "at most 10" in **any**
        time window of duration "60 seconds". In other words, the minutes are not
        the "whole minutes" on a wall clock. The rate limit applies in **any** 60-second
        time window carved out starting **anywhere** on the continuous time axis.
        The 10 events do not need to happen **evenly** in the 60-second time window;
        they can very well happen in a burst or multiple bursts.

        The method :meth:`wait` is thread safe. This object can be passed into
        multiple threads and used in each concurrently.
        """
        self.limit = limit
        self._time_window = time_window_in_seconds
        self._tokens = Ring(limit)
        self._lock = Lock()

    def wait(self):
        """
        Once the user is ready to do "the thing" (such as calling an HTTP service)
        that's subject to this rate limit, call this method. This method may block.
        Once this method returns, user can go ahead to do the thing.

        The time of this method's return is recorded internally as one occurrence of that thing.
        """
        with self._lock:
            window_start = perf_counter() - self._time_window
            tokens = self._tokens
            for _ in range(len(tokens)):
                if tokens.head() < window_start:
                    tokens.pop()
                else:
                    break

            if not self._tokens.full():
                self._tokens.push(perf_counter())
                return

            sleep(tokens.head() + self._time_window - perf_counter())
            tokens.pop()
            tokens.push(perf_counter())
