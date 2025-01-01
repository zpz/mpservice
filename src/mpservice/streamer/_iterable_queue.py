import queue
import threading
import time
from collections.abc import Iterator

from mpservice import multiprocessing
from mpservice._common import StopRequested

from ._streamer import T


class IterableQueue(Iterator[T]):
    def __init__(
        self,
        q: queue.Queue
        | queue.SimpleQueue
        | multiprocessing.Queue
        | multiprocessing.SimpleQueue,
        *,
        num_suppliers: int = 1,
        to_stop: threading.Event | multiprocessing.Event = None,
    ):
        """
        `num_suppliers`: number of parties that will supply data elements to the queue by calling :meth:`put`.
            The parties are typically in different threads or processes.
            Each supplier should call :meth:`put_end` exactly once to indicate it is done adding data.
        `to_stop`: this is used by other parts of the application to tell this queue to exit (because
            some error has happened elsewhere), e.g. stop waiting on `get` or `put`.
            If the queue is to be passed between processes, `to_stop` should be a
            `mpservice.multiprocessing.Event`; otherwise, `to_stop` can be either `threading.Event`
            or `mpservice.multiprocessing.Event` (the latter may be required because the object `to_stop`
            needs to be passed between processes in other parts of the user application).

        `None` is used internally as a special indicator. It must not be a valid value in the user application.

        Typical use case:

        In each of the (one or more) supplier threads or processes, do

            q.put(x)
            q.put(y)
            ...
            q.put_end()

        In each of the (one or more) consumer threads or processes, do

            for z in q:
                use(z)

        The consumers collectively consume the data elements that have been put in the queue.
        """
        if isinstance(q, multiprocessing.SimpleQueue):
            if to_stop is not None:
                raise ValueError(
                    f'`to_stop` is not compatible with `q` of type {type(q).__name__}'
                )
                # Because `mpservice.multiprocessing.SimpleQueue.{get, put}` do not take argument `timeout`.
            self._can_timeout = False
        else:
            self._can_timeout = True
        self._q = q
        self._to_stop = to_stop
        self._wait_interval = 1.0
        # User may revise this value after the object is initiated.

        self._num_suppliers = num_suppliers
        if isinstance(q, (queue.Queue, queue.SimpleQueue)):
            self._spare_lids = queue.Queue(maxsize=num_suppliers)
            self._applied_lids = queue.Queue(maxsize=num_suppliers)
            self._used_lids = queue.Queue(maxsize=num_suppliers)
            # self._lock = threading.Lock()
        else:
            self._spare_lids = multiprocessing.Queue(maxsize=num_suppliers)
            self._applied_lids = multiprocessing.Queue(maxsize=num_suppliers)
            self._used_lids = multiprocessing.Queue(maxsize=num_suppliers)
            # self._lock = multiprocessing.Lock()
        for _ in range(num_suppliers):
            self._spare_lids.put(None)
        # User should not touch these internal helper queues.
        # TODO: the name 'lid' is not very good; something implying the "bottom" would be better.
        # TODO: do we need to use a lock to group the access to the helper queues?

    def __getstate__(self):
        # This will fail if the queues are not pickle-able. That would be a user mistake.
        return (
            self._q,
            self._to_stop,
            self._wait_interval,
            self._num_suppliers,
            self._spare_lids,
            self._applied_lids,
            self._used_lids,
            self._can_timeout,
        )

    def __setstate__(self, zz):
        (
            self._q,
            self._to_stop,
            self._wait_interval,
            self._num_suppliers,
            self._spare_lids,
            self._applied_lids,
            self._used_lids,
            self._can_timeout,
        ) = zz

    @property
    def maxsize(self) -> int:
        try:
            return self._q.maxsize
        except AttributeError:
            return self._q._maxsize
        # If you used a SimpleQueue for `__init__`, this would raise `AttributeError`.

    def qsize(self) -> int:
        return self._q.qsize()

    def put(self, x: T, *, timeout=None) -> None:
        """
        Use this method to put data elements in the queue.

        Once finished putting data in the queue (as far as one supplier,
        such as one thread or process, is concerned), call `put_end()` exactly once
        (per supplier).

        User should never call `put(None)`.
        That is reserved to be called by `put_end()` to indicate the end of
        one supplier's data input.
        """
        if not self._can_timeout:
            if timeout is not None:
                raise ValueError(
                    f'`timeout` is not supported for the type of queue used in this object: {type(self._q).__name__}'
                )
            self._q.put(x)
            return

        if timeout is not None:
            if timeout == 0:
                self._q.put_nowait(x)
                return
            if timeout <= self._wait_interval:
                self._q.put(x, timeout=timeout)
                return
            wait_interval = min(self._wait_interval, timeout / 5)
        else:
            wait_interval = self._wait_interval

        t0 = time.perf_counter()
        Full = queue.Full
        while True:
            try:
                self._q.put(x, timeout=wait_interval)
                return
            except Full:
                if self._to_stop is not None and self._to_stop.is_set():
                    raise StopRequested
                if timeout is not None and (time.perf_counter() - t0) >= timeout:
                    raise
                time.sleep(0)  # force a context switch

    def _get(self) -> T:
        """
        You should not call `_get` directly. Instead, use :meth:`__iter__` or :meth:`__next__`.
        """
        if not self._can_timeout:
            return self._q.get()

        Empty = queue.Empty
        while True:
            try:
                z = self._q.get(timeout=self._wait_interval)
                return z
            except Empty:
                if self._to_stop is not None and self._to_stop.is_set():
                    raise StopRequested
                time.sleep(0)  # force a context switch

    def put_end(self, *, wait_for_renew: bool = False) -> None:
        """
        Each "supplier" must call this method exactly once, after it is done putting
        data in the queue. Do not use `put(None)` for this purpose.

        Suppose in a certain use case a queue is populated by a supplier, which has called `put_end`;
        a consumer is designed to call `renew` after it finishes iterating the queue, allowing
        the next round of data population/consumption. Before the consumer calls `renew`,
        this object does not forbid the supplier from calling `put` to add new data elements
        to the queue (unless the user imposes such restriction themselves), although these
        new data elements are not accessible via `__next__` or `__iter__` until the consumer
        has called `renew`. Suppose the supplier gets to call `put_end` before the consumer
        calls `renew`, the object is in an unexpected state. If `wait_for_renew` is `True`,
        this situation is allowed, and `put_end` will wait to go through once `renew` is called.
        If `wait_for_renew` is `False` (the default), exception is raised in this situation.
        """
        # TODO: add an overall `timeout`?
        if wait_for_renew:
            while True:
                try:
                    z = self._spare_lids.get(timeout=self._wait_interval)
                    break
                except queue.Empty:
                    if self._to_stop is not None and self._to_stop.is_set():
                        raise StopRequested
        else:
            try:
                z = self._spare_lids.get(timeout=0.01)
            except queue.Empty:
                raise RuntimeError(
                    '`put_end` is called more than `num_suppliers` times'
                )

        self._applied_lids.put(z)
        self.put(None)
        # A `None` in the queue corresponds to a `None` in `self._applied_lids`.

    def __next__(self) -> T:
        if self._used_lids.full():
            raise StopIteration

        z = self._get()
        if z is None:
            if self._used_lids.full():
                # Other consumers have removed all the lids and confirmed
                # there's no more data to come from the queue.
                # There's no more `None` in `self._applied_lids`.
                self.put(None)
                # Let there always be an end marker so that other consumers
                # can still iterate over this queue and see it's finished.
                # `self._used_lids` remains full, hence the next call
                # to `__next__` will get here again.
                # This does not increase the number of `None`s in the queue
                # as it simply replaces the one that is just taken off the queue.
                raise StopIteration
            z = self._applied_lids.get()
            self._used_lids.put(z)
            if self._used_lids.full():
                # This is the first consumer who sees the queue is exhausted.
                # Put an extra `None` in the queue for other consumers to see.
                # This is needed because we don't assume nor limit the number
                # of consumers to the queue.
                # This is the only extra `None`: there is only one consumer
                # who is the first to see the bottom of the queue, and subsequent
                # consumers will get/put this `None` without increasing its count.
                self.put(None)
                raise StopIteration
            # The queue is not exhausted because all suppliers's end markers ("lids")
            # have not been collected yet.
            return self.__next__()
        return z

    def __iter__(self) -> Iterator[T]:
        while True:
            try:
                yield self.__next__()
            except StopIteration:
                break

    def renew(self):
        """
        This is for special use cases where the queue needs to be "reused" for
        more than one round of iterations.
        In those use cases, typically the consumer (or consuming side if there are
        more than one consumers) calls `renew` exactly once upon finishing iteration
        over the content of the queue. The suppliers can put more data into the queue
        and call `put_end` as usual once done; the consumer then iterates over the queue
        as if there were no previous rounds.

        This method can only be called after one round of consumption (by iteration) is complete.
        It can not be called at the beginning when no data has been placed in the queue,
        because the implementation does not provide a way to tell the object is in such "brand new"
        state.

        The application needs to ensure `renew` is called only once after one round of iteration.
        """
        if not self._used_lids.full():
            raise RuntimeError('the object is not in a renewable state')
        z = self._get()  # take out the extra `None`
        if z is not None:
            raise RuntimeError(f'expecting None, got {z}')

        for _ in range(self._num_suppliers):
            z = self._used_lids.get()
            self._spare_lids.put(z)