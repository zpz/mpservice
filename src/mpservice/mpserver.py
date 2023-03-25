"""
``mpservice.mpserver`` provides classes that use `multiprocessing`_ to perform CPU-bound operations
taking advantage of all the CPUs (i.e. cores) on the machine.

Using `threading`_ to perform IO-bound operations is also supported, although that is not the initial focus.

There are three levels of constructs.

1. On the lowest level is :class:`Worker`. This defines operations on a single input item
   or a batch of items in usual sync code. This is supposed to run in its own process (thread)
   and use that single process (thread) only.

2. On the middle level is :class:`Servlet`. A basic form of Servlet arranges to execute a :class:`Worker` in multiple
   processes (threads). A more "advanced" form of Servlet arranges to executive multiple
   Servlets as a sequence or an ensemble.

3. On the top level is :class:`Server`. A Server
   handles interfacing with the outside world, while passing the "real work" to
   a :class:`Servlet` and relays the latter's result to the requester.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import logging
import multiprocessing
import multiprocessing.queues
import queue
import threading
import warnings
from abc import ABC, abstractmethod
from collections.abc import Iterable, Iterator, Sequence
from queue import Empty
from time import perf_counter, sleep
from typing import Any, Callable, Literal, Optional

import psutil
from overrides import final

from ._queues import SingleLane
from .util import (
    MP_SPAWN_CTX,
    RemoteException,
    SpawnProcess,
    Thread,
    ThreadPoolExecutor,
    TimeoutError,
)

# This modules uses the 'spawn' method to create processes.

# User should import the `TimeoutError` from this module for exception handling purposes.

# Note on the use of RemoteException:
# The motivation of RemoteException is to wrap an Exception object to go through
# pickling (process queue) and pass traceback info (as a str) along with it.
# The unpickling side will get an object of the original Exception class rather
# than a RemoteException object.
# As a result, objects taken off of a process queue will never be RemoteException objects.
# However, if the queue is a thread queue, then a RemoteException object put in it
# will come out as a RemoteException unchanged.


# Set level for logs produced by the standard `multiprocessing` module.
multiprocessing.log_to_stderr(logging.WARNING)

logger = logging.getLogger(__name__)

NOMOREDATA = b"c7160a52-f8ed-40e4-8a38-ec6b84c2cd87"


class ServerBacklogFull(RuntimeError):
    pass


PipelineFull = ServerBacklogFull
"""
Alias to :class:`ServerBacklogFull` for backward compatibility.
Will be removed in 0.13.0.
"""


class EnsembleError(RuntimeError):
    def __init__(self, results: dict):
        nerr = sum(1 if isinstance(v, RemoteException) else 0 for v in results['y'])
        errmsg = None
        for v in results['y']:
            if isinstance(v, RemoteException):
                errmsg = repr(v)
                break
        msg = f"{results['n']}/{len(results['y'])} ensemble members finished, with {nerr} error{'s' if nerr > 1 else ''}; first error: {errmsg}"
        super().__init__(msg, results)
        # self.args[1] is the results

    def __repr__(self):
        return self.args[0]

    def __str__(self):
        return self.args[0]

    def __reduce__(self):
        return type(self), (self.args[1],)


class FastQueue(multiprocessing.queues.SimpleQueue):
    """
    A customization of `multiprocessing.queue.SimpleQueue <https://docs.python.org/3/library/multiprocessing.html#multiprocessing.SimpleQueue>`_,
    this class reduces some overhead in a particular use-case in this module,
    where one consumer of the queue greedily grabs elements out of the queue
    towards a batch-size limit.

    It is not "fast" in the general sense, hence the class may not be useful
    outside of this module.

    This queue is meant to be used between two processes or between a process
    and a thread.

    The main use case of this class is in :meth:`ProcessWorker._build_input_batches`.

    Check out
    `os.read <https://docs.python.org/3/library/os.html#os.read>`_,
    `os.write <https://docs.python.org/3/library/os.html#os.write>`_, and
    `os.close <https://docs.python.org/3/library/os.html#os.close>`_ with file-descriptor args.
    """

    def __init__(self, *, ctx=None):
        if ctx is None:
            ctx = MP_SPAWN_CTX
        super().__init__(ctx=ctx)
        # Replace Lock by RLock to facilitate batching via greedy `get_many`.
        self._rlock = ctx.RLock()


class SimpleQueue(queue.SimpleQueue):
    """
    A customization of `queue.SimpleQueue <https://docs.python.org/3/library/queue.html#queue.SimpleQueue>_`,
    this class is analogous to :class:`FastQueue` but is designed to be used between two threads.
    """

    def __init__(self):
        super().__init__()
        self._rlock = threading.RLock()


class Worker(ABC):
    """
    ``Worker`` defines operations on a single input item or a batch of items
    in usual synchronous code. This is supposed to run in its own process (or thread)
    and use that single process (or thread) only.

    Typically a subclass needs to enhance :meth:`__init__` and implement :meth:`call`,
    and leave the other methods intact.
    """

    @classmethod
    def run(
        cls,
        *,
        q_in: FastQueue | SimpleQueue,
        q_out: FastQueue | SimpleQueue,
        **init_kwargs,
    ):
        """
        A :class:`Servlet` object will arrange to start a :class:`Worker` object
        in a thread or process. This classmethod will be the ``target`` argument
        to `Thread`_ or `Process`_.

        This method creates a :class:`Worker` object and calls its :meth:`~Worker.start` method
        to kick off the work.

        Parameters
        ----------
        q_in
            A queue that carries input elements to be processed.

            In the subclass :class:`ProcessWorker`, ``q_in`` is a :class:`FastQueue`.
            In the subclass :class:`ThreadWorker`, ``q_in`` is either a :class:`FastQueue` or a :class:`SimpleQueue`.
        q_out
            A queue that carries output values.

            In the subclass :class:`ProcessWorker`, ``q_out`` is a :class:`FastQueue`.
            In the subclass :class:`ThreadWorker`, ``q_out`` is either a :class:`FastQueue` or a :class:`SimpleQueue`.

            The elements in ``q_out`` are results for each individual element in ``q_in``.
            "Batching" is an internal optimization for speed;
            ``q_out`` does not contain result batches, but rather results of individuals.
        **init_kwargs
            Passed on to :meth:`__init__`.
        """
        obj = cls(**init_kwargs)
        q_out.put(obj.name)
        # This sends a signal to the caller (or "coordinator")
        # indicating completion of init.
        obj.start(q_in=q_in, q_out=q_out)

    def __init__(
        self,
        *,
        batch_size: Optional[int] = None,
        batch_wait_time: Optional[float] = None,
        batch_size_log_cadence: int = 1_000_000,
    ):
        """
        The main concern here is to set up controls for "batching" via
        the two parameters ``batch_size`` and ``batch_wait_time``.

        If the algorithm can not vectorize the computation, then there is
        no advantage in enabling batching. In that case, the subclass should
        simply fix ``batch_size`` to 0 in their ``__init__`` and invoke
        ``super().__init__`` accordingly.

        The ``__init__`` of a subclass may define additional input parameters;
        they can be passed in through :meth:`run`.

        Parameters
        ----------
        batch_size
            Max batch size; see :meth:`call`.

            Remember to pass in ``batch_size`` in accordance with the implementation
            of :meth:`call`. In other words, if ``batch_size > 0``, then :meth:`call`
            must handle a list input that contains a batch of elements.
            On the other hand, if ``batch_size`` is 0, then the input to :meth:`call`
            is a single element.

            If ``None``, then 0 is used, meaning no batching.

            If ``batch_size=1``, then processing is batched in form without
            speed benefits of batching.
        batch_wait_time
            Seconds, may be 0; the total duration
            to wait for one batch after the first item has arrived.

            For example, suppose ``batch_size`` is 100 and ``batch_wait_time`` is 1.
            After the first item has arrived, if at least 99 items arrive within 1 second,
            then a batch of 100 elements will be produced;
            if less than 99 elements arrive within 1 second, then the wait will stop
            at 1 second, hence a batch of less than 100 elements will be produced;
            the batch could have only one element.

            If 0, then there's no wait. After the first element is obtained,
            if there are more elements in ``q_in`` "right there right now",
            they will be retrieved until a batch of ``batch_size`` elements is produced.
            Any moment when ``q_in`` is empty, the collection will stop,
            and the elements collected so far (less than ``batch_size`` count of them)
            will make a batch.
            In other words, batching happens only for items that are already
            "piled up" in ``q_in`` at the moment.

            To leverage batching, it is recommended to set ``batch_wait_time``
            to a small positive value. Small, so that there is not much futile waiting.
            Positive (as opposed to 0), so that it always waits a little bit
            just in case more elements are coming in.

            When ``batch_wait_time > 0``, it will hurt performance during
            sequential calls (i.e. send a request with a single element, wait for the result,
            then send the next, and so on), because this worker will always
            wait for this long for additional items to come and form a batch,
            yet additional items will never come during sequential calls.
            However, when batching is enabled, sequential calls are not
            the intended use case. Beware of this factor in benchmarking.

            If ``batch_size`` is 0 or 1, then ``batch_wait_time`` should be left unspecified,
            otherwise the only valid value is 0.

            If ``batch_size > 1``, then ``batch_wait_time`` is 0.01 by default.
        batch_size_log_cadence
            Log batch size statistics every this many batches. If ``None``, this log is turned off.

            This log is for debugging and development purposes.

            This is ignored if ``batch_size=0``.
        """
        if batch_size is None or batch_size == 0:
            batch_size = 0
            if batch_wait_time is None:
                batch_wait_time = 0
            else:
                assert batch_wait_time == 0
        elif batch_size == 1:
            if batch_wait_time is None:
                batch_wait_time = 0
            else:
                assert batch_wait_time == 0
        else:
            if batch_wait_time is None:
                batch_wait_time = 0.01
            else:
                assert 0 < batch_wait_time < 1

        self.batch_size = batch_size
        self.batch_size_log_cadence = batch_size_log_cadence
        self.batch_wait_time = batch_wait_time
        self.name = f"{multiprocessing.current_process().name}-{threading.current_thread().name}"

    @abstractmethod
    def call(self, x):
        """
        Private methods wait on the input queue to gather "work orders",
        send them to :meth:`call` for processing,
        collect the outputs of :meth:`call`,  and put them in the output queue.

        If ``self.batch_size == 0``, then ``x`` is a single
        element, and this method returns result for ``x``.

        If ``self.batch_size > 0`` (including 1), then
        ``x`` is a list of input data elements, and this
        method returns a list (or `Sequence`_) of results corresponding
        to the elements in ``x``.
        However, this output, when received by private methods of this class,
        will be split and individually put in the output queue,
        so that the elements in the output queue (``q_out``)
        correspond to the elements in the input queue (``q_in``),
        although *vectorized* computation, or *batching*, has happended internally.

        When batching is enabled (i.e. when ``self.batch_size > 0``), the number of
        elements in ``x`` varies between calls depending on the supply
        in the input queue. The list ``x`` does not have a fixed length.

        Be sure to distinguish batching from the non-batching case where a single
        input is naturally a list. In that case, the output of
        the this method is the result corresponding to the single input ``x``.
        The result could be anything---it may or may not be a list.

        If a subclass fixes ``batch_size`` in its ``__init__`` to be
        0 or nonzero, make sure this method is implemented accordingly.

        If ``__init__`` does not fix the value of ``batch_size``,
        then a particular instance may have been created with or without batching.
        In this case, this method needs to check ``self.batch_size`` and act accordingly,

        If this method raises exceptions, unless the user has specific things to do,
        do not handle them; just let them happen. They will be handled
        in private methods of this class that call this method.
        """
        raise NotImplementedError

    def start(self, *, q_in, q_out):
        """
        This is called by :meth:`run` to kick off the processing loop.

        To stop the processing, pass in the constant ``NOMOREDATA``
        through ``q_in``.
        """
        try:
            if self.batch_size > 1:
                self._start_batch(q_in=q_in, q_out=q_out)
            else:
                self._start_single(q_in=q_in, q_out=q_out)
        except KeyboardInterrupt:
            print(self.name, "stopped by KeyboardInterrupt")
            # The process or thread will exit. Don't print the usual
            # exception stuff as that's not needed when user
            # pressed Ctrl-C.

    def _start_single(self, *, q_in, q_out):
        batch_size = self.batch_size
        while True:
            z = q_in.get()
            if z == NOMOREDATA:
                q_out.put(z)
                q_in.put(z)  # broadcast to one fellow worker
                break

            uid, x = z
            # Element in the input queue is always a 2-tuple, that is, (ID, value).

            # If it's an exception, short-circuit to output.
            if isinstance(x, BaseException):
                x = RemoteException(x)
            if isinstance(x, RemoteException):
                q_out.put((uid, x))
                continue

            try:
                if batch_size:
                    y = self.call([x])[0]
                else:
                    y = self.call(x)
            except Exception as e:
                # There are opportunities to print traceback
                # and details later. Be brief on the logging here.
                y = RemoteException(e)

            q_out.put((uid, y))
            # Element in the output queue is always a 2-tuple, that is, (ID, value).

    def _start_batch(self, *, q_in, q_out):
        def print_batching_info():
            logger.info(
                "%d batches with sizes %d--%d, mean %.1f",
                n_batches,
                batch_size_min,
                batch_size_max,
                batch_size_mean,
            )

        self._batch_buffer = SingleLane(self.batch_size + 10)
        self._batch_get_called = threading.Event()
        collector_thread = Thread(target=self._build_input_batches, args=(q_in, q_out))
        collector_thread.start()

        n_batches = 0
        batch_size_log_cadence = self.batch_size_log_cadence
        try:
            while True:
                if batch_size_log_cadence and n_batches == 0:
                    batch_size_max = -1
                    batch_size_min = 1000000
                    batch_size_mean = 0.0

                batch = self._get_input_batch()
                if batch == NOMOREDATA:
                    q_in.put(batch)  # broadcast to fellow workers.
                    q_out.put(batch)
                    break

                # The batch is a list of (ID, value) tuples.
                uids = [v[0] for v in batch]
                batch = [v[1] for v in batch]
                n = len(batch)

                try:
                    results = self.call(batch)
                except Exception as e:
                    err = RemoteException(e)
                    for uid in uids:
                        q_out.put((uid, err))
                else:
                    for z in zip(uids, results):
                        q_out.put(z)
                # Each element in the output queue is a (ID, value) tuple.

                if batch_size_log_cadence:
                    n_batches += 1
                    batch_size_max = max(batch_size_max, n)
                    batch_size_min = min(batch_size_min, n)
                    batch_size_mean = (
                        batch_size_mean * (n_batches - 1) + n
                    ) / n_batches
                    if n_batches >= batch_size_log_cadence:
                        print_batching_info()
                        n_batches = 0
        finally:
            if batch_size_log_cadence and n_batches:
                print_batching_info()
            _ = collector_thread.result()

    def _build_input_batches(self, q_in, q_out):
        # This background thread get elements from `q_in`
        # and put them in `self._batch_buffer`.
        # Exceptions taken out of `q_in` will be short-circuited
        # to `q_out`.

        threading.current_thread().name = f"{self.name}._build_input_batches"
        buffer = self._batch_buffer
        batchsize = self.batch_size

        while True:
            if buffer.full():
                with buffer._not_full:
                    buffer._not_full.wait()

            # Multiple workers in separate processes may be competing
            # to get data out of this `q_in`.
            with q_in._rlock:
                # Now we've got hold of the read lock.
                # In order to facilitate batching,
                # we hold on to the lock and keep getting
                # data from `q_in` even though other readers are waiting.
                # We let go the lock when certain conditions are met.
                while True:
                    z = q_in.get()  # wait as long as it takes to get one item.
                    while True:
                        if z == NOMOREDATA:
                            buffer.put(z)
                            q_in.put(z)  # broadcast to fellow workers.
                            q_out.put(z)
                            return
                        # Now `z` is a tuple like (uid, x).
                        if isinstance(z[1], BaseException):
                            q_out.put((z[0], RemoteException(z[1])))
                        elif isinstance(z[1], RemoteException):
                            q_out.put(z)
                        else:
                            buffer.put(z)

                        # If `q_in` currently has more data right there
                        # and `buffer` has not reached `batchsize` yet,
                        # keep grabbing more data.
                        if not q_in.empty() and buffer.qsize() < batchsize:
                            z = q_in.get()
                        else:
                            break

                    # Now, either `q_in` is empty or `buffer` already has
                    # a batch-ful of items, and we have retrieved at least one
                    # item during this holding of the lock.

                    if self._batch_get_called.is_set():
                        # `_get_input_batch` has been called in this round;
                        # that is, `self` has already take a (partial) batch
                        # of data away to process. Even though that might have
                        # made `buffer` low at this time, we should let go
                        # the lock to give others a chance to read data.
                        self._batch_get_called.clear()
                        break
                    if buffer.qsize() >= batchsize:
                        # `buffer` has reached `batchsize`, which is the most
                        # that `_get_input_batch` will take in one call.
                        # Even if `buffer` is not full, we no longer has priority
                        # for more data. Release the lock to give others
                        # a chance.
                        break

    def _get_input_batch(self):
        # This function gets a batch from `self._batch_buffer`.

        extra_timeout = self.batch_wait_time
        batchsize = self.batch_size
        buffer = self._batch_buffer
        out = buffer.get()
        if out == NOMOREDATA:
            return out
        out = [out]
        n = 1

        deadline = perf_counter() + extra_timeout
        # Timeout starts after the first item is obtained.

        while n < batchsize:
            t = deadline - perf_counter()
            # `t` is the remaining time to wait.
            # If `extra_timeout == 0`, then `t <= 0`.
            # If `t <= 0`, will still get an item if it is already
            # in the buffer.
            try:
                z = buffer.get(timeout=max(0, t))
                # If `extra_timeout == 0`, then `timeout=0`,
                # hence will get an item w/o wait.
            except Empty:
                break
            if z == NOMOREDATA:
                # Return the batch so far.
                # Put this indicator back in the buffer.
                # Next call to this method will get
                # the indicator.
                buffer.put(z)
                break
            out.append(z)
            n += 1

        self._batch_get_called.set()
        return out


class CpuAffinity:
    """
    ``CpuAffinity`` specifies which CPUs (or cores) a process should run on.

    This operation is known as "pinning a process to certain CPUs"
    or "setting the CPU/processor affinity of a process".

    Setting and getting CPU affinity is done via |psutil.cpu_affinity|_.

    .. |psutil.cpu_affinity| replace:: ``psutil.Process().cpu_affinity``
    .. _psutil.cpu_affinity: https://psutil.readthedocs.io/en/latest/#psutil.Process.cpu_affinity
    .. see https://jwodder.github.io/kbits/posts/rst-hyperlinks/
    """

    def __init__(self, target: Optional[int | Sequence[int]] = None, /):
        """
        Parameters
        ----------
        target
            The CPUs to pin the current process to.

            If ``None``, no pinning is done. This object is used only to query the current affinity.
            (I believe all process starts in an un-pinned status.)

            If an int, it is the zero-based index of the CPU. Valid values are 0, 1,...,
            the number of CPUs minus 1. If a list, the elements are CPU indices.
            Duplicate values will be removed. Invalid values will raise ``ValueError``.

            If ``[]``, pin to all eligible CPUs.
        """
        if target is not None:
            if isinstance(target, int):
                target = [target]
            else:
                assert all(isinstance(v, int) for v in target)
                # `psutil` would truncate floats but I don't like that.
        self.target = target

    def __repr__(self):
        return f"{self.__class__.__name__}({self.target})"

    def __str__(self):
        return self.__repr__()

    def set(self) -> None:
        """
        Set CPU affinity to the value passed into :meth:`__init__`.
        If that value was ``None``, do nothing.
        """
        if self.target is not None:
            psutil.Process().cpu_affinity(self.target)

    def get(self) -> list[int]:
        """Return the current CPU affinity."""
        return psutil.Process().cpu_affinity()


class ProcessWorker(Worker):
    @classmethod
    def run(cls, *, cpus: CpuAffinity, **kwargs):
        """
        This classmethod runs in the worker process to construct
        the worker object and start its processing loop.

        This function is the parameter ``target`` to :class:`~mpservice.util.SpawnProcess`.
        As such, elements in ``**kwargs`` go through pickling,
        hence they should consist
        mainly of small, Python builtin types such as string, number, small dict's, etc.
        Be careful about passing custom class objects in ``**kwargs``.

        Parameters
        ----------
        cpus
            Specify which CPUs the current process should run on.
        """
        cpus.set()
        super().run(**kwargs)


class ThreadWorker(Worker):
    """
    Use this class if the operation is I/O bound (e.g. calling an external service
    to get some info), and computation is very light compared to the I/O part.
    Another use-case of this class is to perform some very simple and quick
    pre-processing or post-processing.
    """


def make_threadworker(func: Callable[[Any], Any]) -> type[ThreadWorker]:
    """
    This function defines and returns a simple :class:`ThreadWorker` subclass
    for quick, "on-the-fly" use.
    This can be useful when we want to introduce simple servlets
    for pre-processing and post-processing.

    Parameters
    ----------
    func
        This function is what happens in the method :meth:`~Worker.call`.
    """

    class MyThreadWorker(ThreadWorker):
        def call(self, x):
            return func(x)

    MyThreadWorker.__name__ = f"ThreadWorker-{func.__name__}"
    return MyThreadWorker


PassThrough = make_threadworker(lambda x: x)
"""
Example use of this class::

    def combine(x):
        '''
        Combine the ensemble elements depending on the results
        as well as the original input.
        '''
        x, *y = x
        assert len(y) == 3
        if x < 100:
            return sum(y) / len(y)
        else:
            return max(y)

    s = EnsembleServlet(
            ThreadServlet(PassThrough),
            ProcessServlet(W1),
            ProcessServlet(W2)
            ProcessServlet(W3),
        )
    ss = SequentialServlet(s, ThreadServlet(make_threadworker(combine)))
"""


class Servlet(ABC):
    @abstractmethod
    def start(self, q_in, q_out) -> None:
        raise NotImplementedError

    @abstractmethod
    def stop(self) -> None:
        '''
        When this method is called, there shouldn't be "pending" work
        in the servlet. If for whatever reason, the servlet is in a busy,
        messy state, it is not required to "wait" for things to finish.
        The priority is to ensure the processes and threads are stopped.

        The primary mechanism to stop a servlet is to put
        the special constant ``NOMOREDATA`` in the input queue.
        The user should have done that; but just to be sure,
        this method may do that again.
        '''
        raise NotImplementedError

    @property
    @abstractmethod
    def input_queue_type(self) -> Literal['thread', 'process']:
        '''
        Indicate whether the input queue can be a "thread queue"
        (i.e. ``queue.Queue``) or needs to be a "process queue"
        (i.e. ``multiprocessing.queues.Queue``).
        If "thread", caller can provide either a thread queue or a
        process queue. If "process", caller must provide a process queue.
        '''
        raise NotImplementedError

    @property
    @abstractmethod
    def output_queue_type(self) -> Literal['thread', 'process']:
        '''
        Indicate whether the output queue can be a "thread queue"
        (i.e. ``queue.Queue``) or needs to be a "process queue"
        (i.e. ``multiprocessing.queues.Queue``).
        If "thread", caller can provide either a thread queue or a
        process queue. If "process", caller must provide a process queue.
        '''
        raise NotImplementedError

    @property
    @abstractmethod
    def workers(self) -> list[Worker]:
        raise NotImplementedError


class ProcessServlet(Servlet):
    def __init__(
        self,
        worker_cls: type[ProcessWorker],
        *,
        cpus: Optional[Sequence[CpuAffinity | None | int | Sequence[int]]] = None,
        name: Optional[str] = None,
        **kwargs,
    ):
        """
        Parameters
        ----------
        worker_cls
            A subclass of :class:`ProcessWorker`.
        cpus
            Specifies how many processes to create and how they are pinned
            to specific CPUs.

            The default is ``None``, indicating a single unpinned process.

            Otherwise, a list of :class:`CpuAffinity` objects.
            For convenience, values of primitive types are also accepted;
            they will be used to construct `CpuAffinity` objects.
            The number of processes created is the number of elements in ``cpus``.
            The CPU spec is very flexible. For example,

            ::

                cpus=[[0, 1, 2], [0], [2, 3], [4, 5, 6], 4, None]

            This instructs the servlet to create 6 processes, each running an instance
            of ``worker_cls``. The CPU affinity of each process is as follows:

            1. CPUs 0, 1, 2
            2. CPU 0
            3. CPUs 2, 3
            4. CPUs 4, 5, 6
            5. CPU 4
            6. Any CPU, no pinning
        name
            The main part of the names of the worker processes.
            If not specified, the name of this class is used.
            Each process is named after this value plus its CPU affinity info.
        **kwargs
            Passed to the ``__init__`` method of ``worker_cls``.

        Notes
        -----
        When the servlet has multiple processes, the output stream does not follow
        the order of the elements in the input stream.
        """
        assert issubclass(worker_cls, ProcessWorker)
        self._worker_cls = worker_cls
        self._name = name or worker_cls.__name__
        if cpus is None:
            self._cpus = [CpuAffinity(None)]
        else:
            self._cpus = [
                v if isinstance(v, CpuAffinity) else CpuAffinity(v) for v in cpus
            ]
        self._init_kwargs = kwargs
        self._workers = []
        self._started = False

    def start(self, q_in: FastQueue, q_out: FastQueue):
        """
        Create the requested number of processes, in each starting an instance
        of ``self._worker_cls``.

        Parameters
        ----------
        q_in
            A queue with input elements. Each element will be passed to and processed by
            exactly one worker process.
        q_out
            A queue for results.
        """
        assert not self._started
        for cpu in self._cpus:
            # Create as many processes as the length of `cpus`.
            # Each process is pinned to the specified cpu core.
            if cpu.target is None:
                sname = self._name
            else:
                sname = f"{self._name}-{cpu}"
            logger.info("adding worker <%s> at CPU %s ...", sname, cpu)
            self._workers.append(
                SpawnProcess(
                    target=self._worker_cls.run,
                    name=sname,
                    kwargs={
                        "q_in": q_in,
                        "q_out": q_out,
                        "cpus": cpu,
                        **self._init_kwargs,
                    },
                )
            )
            self._workers[-1].start()
            name = q_out.get()
            logger.debug("   ... worker <%s> is ready", name)

        self._q_in = q_in
        self._q_out = q_out
        logger.info("servlet %s is ready", self._name)
        self._started = True

    def stop(self):
        """Stop the workers."""
        assert self._started
        self._q_in.put(NOMOREDATA)
        for w in self._workers:
            _ = w.result()
        self._workers = []
        self._started = False

    @property
    def input_queue_type(self):
        return "process"

    @property
    def output_queue_type(self):
        return "process"

    @property
    def workers(self):
        return self._workers


class ThreadServlet(Servlet):
    def __init__(
        self,
        worker_cls: type[ThreadWorker],
        *,
        num_threads: Optional[int] = None,
        name: Optional[str] = None,
        **kwargs,
    ):
        """
        Parameters
        ----------
        worker_cls
            A subclass of :class:`ThreadWorker`
        num_threads
            The number of threads to create. Each thread will host and run
            an instance of ``worker_cls``.
        name
            The main part of the names of the worker processes.
            If not specified, the name of this class is used.
            Each thread is named after this value plus a serial number.
        **kwargs
            Passed on the ``__init__`` method of ``worker_cls``.

        Notes
        -----
        When the servlet has multiple threads, the output stream does not follow
        the order of the elements in the input stream.
        """
        assert issubclass(worker_cls, ThreadWorker)
        self._worker_cls = worker_cls
        self._name = name or worker_cls.__name__
        self._num_threads = num_threads or 1
        self._init_kwargs = kwargs
        self._workers = []
        self._started = False

    def start(self, q_in: FastQueue | SimpleQueue, q_out: FastQueue | SimpleQueue):
        """
        Create the requested number of threads, in each starting an instance
        of ``self._worker_cls``.

        Parameters
        ----------
        q_in
            A queue with input elements. Each element will be passed to and processed by
            exactly one worker thread.
        q_out
            A queue for results.

            ``q_in`` and ``q_out`` are either :class:`FastQueue`\\s (for processes)
            or :class:`SimpleQueue`\\s (for threads). Because this servlet may be connected to
            either :class:`ProcessServlet`\\s or :class:`ThreadServlet`\\s, either type of queues may
            be appropriate. In contrast, for :class:`ProcessServlet`, the input and output
            queues are both :class:`FastQueue`\\s.
        """
        assert not self._started
        for ithread in range(self._num_threads):
            sname = f"{self._name}-{ithread}"
            logger.info("adding worker <%s> in thread ...", sname)
            w = Thread(
                target=self._worker_cls.run,
                name=sname,
                kwargs={
                    "q_in": q_in,
                    "q_out": q_out,
                    **self._init_kwargs,
                },
            )
            w.start()
            self._workers.append(w)
            name = q_out.get()
            logger.debug("   ... worker <%s> is ready", name)

        self._q_in = q_in
        self._q_out = q_out
        logger.info("servlet %s is ready", self._name)
        self._started = True

    def stop(self):
        """Stop the worker threads."""
        assert self._started
        self._q_in.put(NOMOREDATA)
        for w in self._workers:
            _ = w.result()
        self._workers = []
        self._started = False

    @property
    def input_queue_type(self):
        return "thread"

    @property
    def output_queue_type(self):
        return "thread"

    @property
    def workers(self):
        return self._workers


class SequentialServlet(Servlet):
    """
    A ``SequentialServlet`` represents
    a sequence of operations performed in order,
    one operations's output becoming the next operation's input.

    Each operation is performed by a "servlet", that is,
    a :class:`ProcessServlet` or :class:`ThreadServlet` or :class:`SequentialServlet`
    or :class:`EnsembleServlet`.

    If any member servlet has multiple workers (threads or processes),
    the output stream does not follow the order of the elements in the input stream.
    """

    def __init__(self, *servlets: Servlet):
        assert len(servlets) > 0
        self._servlets = servlets
        self._qs = []
        self._started = False

    def start(self, q_in, q_out):
        """
        Start the member servlets.

        A main concern is to connect the servlets by "pipes", or queues,
        for input and output, in addition to the very first ``q_in``,
        which carries input items from the "outside world" and the very last ``q_out``,
        which sends output items to the "outside world".

        Each item in ``q_in`` goes to the first member servlet;
        the result goes to the second servlet; and so on.
        The result out of the last servlet goes to ``q_out``.

        The types of ``q_in`` and ``q_out`` are decided by the caller.
        The types of intermediate queues are decided within this function.
        As a rule, use :class:`SimpleQueue` between two threads; use :class:`FastQueue`
        between two processes or between a process and a thread.
        """
        assert not self._started
        nn = len(self._servlets)
        q1 = q_in
        for i, s in enumerate(self._servlets):
            if i + 1 < nn:  # not the last one
                if (
                    s.output_queue_type == 'thread'
                    and self._servlets[i + 1].input_queue_type == 'thread'
                ):
                    q2 = SimpleQueue()
                else:
                    q2 = FastQueue()
                self._qs.append(q2)
            else:
                q2 = q_out
            s.start(q1, q2)
            q1 = q2
        self._q_in = q_in
        self._started = True

    def stop(self):
        """Stop the member servlets."""
        assert self._started
        self._q_in.put(NOMOREDATA)
        for q in self._qs:
            q.put(NOMOREDATA)
        for s in self._servlets:
            s.stop()
        self._qs = []
        self._started = False

    @property
    def workers(self):
        return [w for s in self._servlets for w in s.workers]

    @property
    def input_queue_type(self):
        return self._servlets[0].input_queue_type

    @property
    def output_queue_type(self):
        return self._servlets[-1].output_queue_type


class EnsembleServlet(Servlet):
    """
    A ``EnsembleServlet`` represents
    an ensemble of operations performed in parallel on each input item.
    The list of results, corresponding to the order of the operators,
    is returned as the result.

    Each operation is performed by a "servlet", that is,
    a :class:`ProcessServlet` or :class:`ThreadServlet` or :class:`SequentialServlet`
    or :class:`EnsembleServlet`.

    If ``fail_fast`` is ``True`` (the default), once one ensemble member raises an
    Exception, an ``EnsembleError`` will be returned. The other ensemble members
    that arrive after this will be ignored. This is not necessarily "fast"; the main point
    is that the item in question results in an Exception rather than a list that contains
    Exception(s). If ``fail_fast`` is ``False``, then Exception results, if any, are included
    in the result list. If all entries in the list are Exceptions, then the result list
    is replaced by an ``EnsembleError``.

    The output stream does not need to follow the order of the elements in the input stream.
    """

    def __init__(self, *servlets: Servlet, fail_fast: bool = True):
        assert len(servlets) > 1
        self._servlets = servlets
        self._started = False
        self._fail_fast = fail_fast

    def _reset(self):
        self._qin = None
        self._qout = None
        self._qins = []
        self._qouts = []
        self._uid_to_results = {}
        self._threads = []

    def start(self, q_in, q_out):
        """
        Start the member servlets.

        A main concern is to wire up the parallel execution of all the servlets
        on each input item.

        ``q_in`` and ``q_out`` contain inputs from and outputs to
        the "outside world". Their types, either :class:`FastQueue` or :class:`SimpleQueue`,
        are decided by the caller.
        """
        assert not self._started
        self._reset()
        self._qin = q_in
        self._qout = q_out
        for s in self._servlets:
            q1 = SimpleQueue() if s.input_queue_type == 'thread' else FastQueue()
            q2 = SimpleQueue() if s.output_queue_type == 'thread' else FastQueue()
            s.start(q1, q2)
            self._qins.append(q1)
            self._qouts.append(q2)
        t = Thread(target=self._dequeue)
        t.start()
        self._threads.append(t)
        t = Thread(target=self._enqueue)
        t.start()
        self._threads.append(t)
        self._started = True

    def _enqueue(self):
        threading.current_thread().name = f"{self.__class__.__name__}._enqueue"
        qin = self._qin
        qout = self._qout
        qins = self._qins
        catalog = self._uid_to_results
        nn = len(qins)
        while True:
            v = qin.get()
            if v == NOMOREDATA:  # sentinel for end of input
                for q in qins:
                    q.put(v)  # send the same sentinel to each servlet
                qout.put(v)
                return

            uid, x = v
            if isinstance(x, BaseException):
                x = RemoteException(x)
            if isinstance(x, RemoteException):
                # short circuit exception to the output queue
                qout.put((uid, x))
                continue
            z = {"y": [None] * nn, "n": 0}
            # `y` is the list of outputs from the servlets, in order.
            # `n` is number of outputs already received.
            catalog[uid] = z
            for q in qins:
                q.put((uid, x))
                # Element in the queue to each servlet is in the standard format,
                # i.e. a (ID, value) tuple.

    def _dequeue(self):
        threading.current_thread().name = f"{self.__class__.__name__}._dequeue"
        qout = self._qout
        qouts = self._qouts
        catalog = self._uid_to_results
        fail_fast = self._fail_fast
        nn = len(qouts)
        while True:
            all_empty = True
            for idx, q in enumerate(qouts):
                while not q.empty():
                    # Get all available results out of this queue.
                    # They are for different requests.
                    # This is needed because the output elements in a member servlet
                    # may not follow the order of the input elements.
                    # If they did, we could just take the head of the output stream
                    # of each member servlet, and assemble them as the output for one
                    # input element.
                    all_empty = False
                    v = q.get()
                    if v == NOMOREDATA:
                        qout.put(v)
                        return
                    uid, y = v
                    # `y` can be an exception object or a regular result.
                    z = catalog.get(uid)
                    if z is None:
                        # The entry has been removed due to "fail fast"
                        continue

                    if isinstance(y, BaseException):
                        y = RemoteException(y)

                    z["y"][idx] = y
                    z["n"] += 1

                    if fail_fast and isinstance(y, RemoteException):
                        # If fail fast, then the first exception causes
                        # this to return an EnsembleError as result.
                        catalog.pop(uid)
                        try:
                            raise EnsembleError(z)
                        except Exception as e:
                            qout.put((uid, RemoteException(e)))
                    elif z['n'] == nn:
                        # All results for this request have been collected.
                        # If the results contain any exception member, then
                        # ``fail_fast`` must be ``False``. In this case,
                        # the result list is replaced by an EnsembleError
                        # only if all members are exceptions.
                        catalog.pop(uid)
                        if all(isinstance(v, RemoteException) for v in z['y']):
                            try:
                                raise EnsembleError(z)
                            except Exception as e:
                                y = RemoteException(e)
                        else:
                            y = z["y"]
                        qout.put((uid, y))
            if all_empty:
                sleep(0.01)  # TODO: what is a good duration?

    def stop(self):
        # Caller is responsible to put a NOMOREDATA item in the input queue.
        assert self._started
        self._qin.put(NOMOREDATA)
        for s in self._servlets:
            s.stop()
        for t in self._threads:
            _ = t.result()
        self._reset()
        self._started = False

    @property
    def workers(self):
        return [w for s in self._servlets for w in s.workers]

    @property
    def input_queue_type(self):
        return 'thread'

    @property
    def output_queue_type(self):
        return 'thread'


Sequential = SequentialServlet
"""An alias to :class:`SequentialServlet` for backward compatibility.

.. deprecated:: 0.11.8
    Will be removed in 0.13.0.
    Use ``SequentialSevlet`` instead.
"""


Ensemble = EnsembleServlet
"""An alias to :class:`EnsembleServlet` for backward compatibility.

.. deprecated:: 0.11.8
    Will be removed in 0.13.0.
    Use ``EnsembleSevlet`` instead.
"""


class SwitchServlet(Servlet):
    """
    SwitchServlet contains multiple member servlets (which are provided to :meth:`__init__`).
    Each input element is passed to and processed by exactly one of the members
    based on the output of the method :meth:`switch`.

    This is somewhat analogous to the "switch" construct in some
    programming languages.
    """

    def __init__(self, *servlets: Servlet):
        assert len(servlets) > 0
        self._servlets = servlets
        self._started = False

    def _reset(self):
        self._qin = None
        self._qout = None
        self._qins = []
        self._thread_enqueue = None

    def start(self, q_in, q_out):
        assert not self._started
        self._reset()
        self._qin = q_in
        self._qout = q_out
        for s in self._servlets:
            q1 = SimpleQueue() if s.input_queue_type == 'thread' else FastQueue()
            s.start(q1, q_out)
            self._qins.append(q1)
        self._thread_enqueue = Thread(target=self._enqueue)
        self._thread_enqueue.start()
        self._started = True

    def stop(self):
        # Caller is responsible to put a NOMOREDATA item in the input queue.
        assert self._started
        self._qin.put(NOMOREDATA)
        for s in self._servlets:
            s.stop()
        _ = self._thread_enqueue.result()
        self._reset()
        self._started = False

    @abstractmethod
    def switch(self, x) -> int:
        '''
        Parameters
        ----------
        x
          An element received via the input queue, that is,
          the parameter ``q_in`` to :meth:`start`. If the current servlet
          is preceded by another servlet, then ``x`` is an output of the
          other servlet.

          In principle, this method should not modify ``x``.
          It is expected to be a quick check on ``x`` to determine
          which member servlet should process it.

          ``x`` is never an instance of Exception or RemoteException.
          That case is already taken care of before this method is called.
          This class should not be used to handle such exception cases.

        Returns
        -------
        int
          The index of the member servlet that will receive and process ``x``.
          This number is between 0 and the number of member servlets minus 1,
          inclusive.
        '''
        raise NotImplementedError

    def _enqueue(self):
        threading.current_thread().name = f"{self.__class__.__name__}._enqueue"
        qin = self._qin
        qout = self._qout
        qins = self._qins
        while True:
            v = qin.get()
            if v == NOMOREDATA:  # sentinel for end of input
                for q in qins:
                    q.put(v)  # send the same sentinel to each servlet
                qout.put(v)
                return

            uid, x = v
            if isinstance(x, BaseException):
                x = RemoteException(x)
            if isinstance(x, RemoteException):
                # short circuit exception to the output queue
                qout.put((uid, x))
                continue
            idx = self.switch(x)
            qins[idx].put((uid, x))

    @property
    def input_queue_type(self):
        return 'thread'

    @property
    def output_queue_type(self):
        if all(s.output_queue_type == 'thread' for s in self._servlets):
            return 'thread'
        return 'process'

    @property
    def workers(self):
        return [w for s in self._servlets for w in s.workers]


class Server:
    @classmethod
    @final
    def get_mp_context(cls):
        """
        If subclasses need to use additional Queues, Locks, Conditions, etc,
        they should create them out of this context.
        This returns a spawn context.
        Subclasses should not customize this method.
        """
        return MP_SPAWN_CTX

    # TODO: how to track helper processes created by subclasses, so that
    # the sys info log in ``_gather_output`` can include them?

    def __init__(
        self,
        servlet: Servlet,
        *,
        main_cpu: int = 0,
        backlog: int = 1024,
        sys_info_log_cadence: int = None,
    ):
        """
        Parameters
        ----------
        servlet
            The servlet to run by this server.

        main_cpu
            Specifies the cpu for the "main process",
            i.e. the process in which this server objects resides.

        backlog
            Max number of requests concurrently in progress within this server,
            all pipes/servlets/stages combined.

            For each request received, a UUID is assigned to it.
            An entry is added to an internal book-keeping dict.
            Then this ID along with the input data enter the processing pipeline.
            Coming out of the pipeline is the ID along with the result.
            The book-keeping record is found by the ID, used, and removed from the dict.
            The result is returned to the requester.
            The ``backlog`` value is simply the size limit on this internal
            book-keeping dict.
        sys_info_log_cadence
            .. deprecated:: 0.12.1. Will be removed in 0.13.0.
        """
        self.servlet = servlet

        assert backlog > 0
        self._backlog = backlog

        if main_cpu is not None:
            # Pin this coordinating thread to the specified CPUs.
            if isinstance(main_cpu, int):
                cpus = [main_cpu]
            else:
                assert isinstance(main_cpu, list)
                cpus = main_cpu
            psutil.Process().cpu_affinity(cpus=cpus)

        if sys_info_log_cadence is not None:
            warnings.warn(
                "The parameter `sys_info_log_cadence` is deprecated in version 0.12.1 and will be removed in 0.13.0",
                DeprecationWarning,
                stacklevel=2,
            )
        self._started = False

    def __enter__(self):
        """
        Start the servlet and get the server ready to take requests.
        """
        assert not self._started

        self._threads = []

        self._uid_to_futures = {}
        # Size of this dict is capped at `self._backlog`.
        # A few places need to enforce this size limit.

        self._input_buffer = queue.SimpleQueue()
        # This has unlimited size; `put` never blocks (as long as
        # memory is not blown up!). Input requests respect size limit
        # of `_uid_to_futures`, but is not blocked when putting
        # into this queue. A background thread takes data out of this
        # queue and puts them into `_q_in`, which could block.

        self._q_in = (
            SimpleQueue() if self.servlet.input_queue_type == 'thread' else FastQueue()
        )
        self._q_out = (
            SimpleQueue() if self.servlet.output_queue_type == 'thread' else FastQueue()
        )
        self.servlet.start(self._q_in, self._q_out)

        t = Thread(target=self._gather_output)
        t.start()
        self._threads.append(t)
        t = Thread(target=self._onboard_input)
        t.start()
        self._threads.append(t)

        self._started = True
        return self

    def __exit__(self, *args, **kwargs):
        assert self._started
        # msg = exit_err_msg(self, exc_type, exc_value, exc_traceback)
        # if msg:
        #     logger.error(msg)

        self._input_buffer.put(NOMOREDATA)

        self.servlet.stop()

        for t in self._threads:
            _ = t.result()

        # Reset CPU affinity.
        psutil.Process().cpu_affinity(cpus=[])
        self._started = False

    async def async_call(
        self, x, /, *, timeout: int | float = 60, backpressure: bool = True
    ):
        """
        When this is called, this server is usually backing a (http or other) service.
        Concurrent async calls to this object may happen.
        In such use cases, :meth:`call` and :meth:`stream` should not be called to this object
        at the same time.

        Parameters
        ----------
        x
            Input data element.
        timeout
            In seconds. If result is not ready after this time, :class:`~mpservice.util.TimeoutError` is raised.

            There are two situations where timeout happens.
            At first, ``x`` is placed in an input queue for processing.
            This step is called "enqueue".
            If the queue is full for the moment, the code will wait.
            If a spot does not become available during the ``timeout`` period,
            the :class:`~mpservice.util.TimeoutError` message will be "... seconds enqueue".

            Once ``x`` is placed in the input queue, code will wait for the result at the end
            of an output queue. If result is not yet ready when the ``timeout`` period
            is over, the :class:`~mpservice.util.TimeoutError` message will be ".. seconds total".
            This wait, as well as the error message, includes the time that has been spent
            in the "enqueue" step, that is, the timer starts upon receiving the request.
        backpressure
            If ``True``, and the input queue is full, do not wait; raise :class:`ServerBacklogFull`
            right away. If ``False``, wait on the input queue for as long as
            ``timeout`` seconds. Effectively, the input queue is considered full if
            there are ``backlog`` count of ongoing (i.e. received but not yet returned) requests
            in the server, where ``backlog`` is a parameter to :meth:`__init__`.
        """
        fut = await self._async_enqueue(x, timeout=timeout, backpressure=backpressure)
        return await self._async_wait_for_result(fut)

    def call(self, x, /, *, timeout: int | float = 60):
        """
        This is called in "embedded" mode for sporadic uses.
        It is not designed to serve high load from multi-thread concurrent
        calls. To process large amounts in embedded model, use :meth:`stream`.

        The parameters ``x`` and ``timeout`` have the same meanings as in :meth:`async_call`.
        """
        fut = self._enqueue(x, timeout)
        return self._wait_for_result(fut)

    def stream(
        self,
        data_stream: Iterable,
        /,
        *,
        return_x: bool = False,
        return_exceptions: bool = False,
        timeout: int | float = 60,
    ) -> Iterator:
        """
        Use this method for high-throughput processing of a long stream of
        data elements. In theory, this method achieves the throughput upper-bound
        of the server, as it saturates the pipeline.

        The order of elements in the stream is preserved, i.e.,
        elements in the output stream corresponds to elements
        in the input stream in the same order.

        Parameters
        ----------
        data_stream
            An iterable, possibly unlimited, of input data elements.
        return_x
            If ``True``, each output element is a length-two tuple
            containing the input data and the result.
            If ``False``, each output element is just the result.
        return_exceptions
            If ``True``, any Exception object will be produced in the output stream
            in place of the would-be regular result.
            If ``False``, exceptions will be propagated right away, crashing the program.
        timeout
            Interpreted the same as in :meth:`call` and :meth:`async_call`.

            In a streaming task, "timeout" is usually not a concern compared
            to overall throughput. You can usually leave it at the default value.
        """

        def _enqueue(tasks, return_exceptions):
            threading.current_thread().name = (
                f"{self.__class__.__name__}.stream._enqueue"
            )
            # Putting input data in the queue does not need concurrency.
            # The speed of sequential push is as fast as it can go.
            _enq = self._enqueue
            try:
                for x in data_stream:
                    try:
                        fut = _enq(x, timeout)
                        timedout = False
                    except Exception as e:
                        if return_exceptions:
                            timedout = True
                            fut = concurrent.futures.Future()
                            fut.set_exception(e)
                        else:
                            logger.error("exception '%r' happened for input '%s'", e, x)
                            raise
                    tasks.put((x, fut, timedout))
                # Exceptions in `fut` is covered by `return_exceptions`.
                # Uncaught exceptions will propagate and cause the thread to exit in
                # exception state. This exception is not covered by `return_exceptions`;
                # it will be detected in the main thread.
            finally:
                tasks.put(NOMOREDATA)

        tasks = queue.SimpleQueue()
        executor = ThreadPoolExecutor(1)
        t = executor.submit(_enqueue, tasks, return_exceptions)

        _wait = self._wait_for_result

        while True:
            z = tasks.get()
            if z == NOMOREDATA:
                if t.done:
                    if t.exception():
                        raise t.exception()
                else:
                    executor.shutdown()
                break

            x, fut, timedout = z
            if timedout:
                # This happens only when `return_exceptions` is True.
                if return_x:
                    yield x, fut.exception()
                else:
                    yield fut.exception()
            else:
                try:
                    y = _wait(fut)
                    # May raise TimeoutError or an exception out of RemoteException.
                except Exception as e:
                    if return_exceptions:
                        if return_x:
                            yield x, e
                        else:
                            yield e
                    else:
                        logger.error("exception '%r' happened for input %r", e, x)
                        raise
                else:
                    if return_x:
                        yield x, y
                    else:
                        yield y

    async def _async_enqueue(self, x, timeout, backpressure):
        t0 = perf_counter()
        deadline = t0 + timeout

        while len(self._uid_to_futures) >= self._backlog:
            if backpressure:
                raise ServerBacklogFull(len(self._uid_to_futures))
                # If this is behind a HTTP service, should return
                # code 503 (Service Unavailable) to client.
            if (t := perf_counter()) > deadline:
                raise TimeoutError(f"{t - t0:.3f} seconds enqueue")
            await asyncio.sleep(min(0.1, deadline - t))
            # It's OK if this sleep is a little long,
            # because the pipe is full and busy.

        # fut = asyncio.Future()
        fut = concurrent.futures.Future()
        fut.data = {"t0": t0, "timeout": timeout}
        uid = id(fut)
        self._uid_to_futures[uid] = fut
        self._input_buffer.put((uid, x))
        return fut

    async def _async_wait_for_result(self, fut):
        t0 = fut.data["t0"]
        t2 = t0 + fut.data["timeout"]
        while not fut.done():
            timenow = perf_counter()
            if timenow > t2:
                fut.cancel()
                raise TimeoutError(f"{timenow - t0:.3f} seconds total")
            await asyncio.sleep(min(0.01, t2 - timenow))
        return fut.result()
        # This could raise an exception originating from RemoteException.

        # TODO: I don't understand why the following (along with
        # corresponding change in `_async_enqueue`) seems to work but
        # is very, very slow.

        # t0 = fut.data['t0']
        # t2 = t0 + fut.data['timeout']
        # try:
        #     return await asyncio.wait_for(fut, timeout=max(0, t2 - perf_counter()))
        # except asyncio.TimeoutError:
        #     # `wait_for` has already cancelled `fut`.
        #     raise TimeoutError(f"{perf_counter() - t0:.3f} seconds total")

    def _enqueue(self, x, timeout):
        # This method is called by `call` or `stream`.
        # There are no concurrent calls to this method.
        t0 = perf_counter()
        deadline = t0 + timeout

        while len(self._uid_to_futures) >= self._backlog:
            if (t := perf_counter()) >= deadline:
                raise TimeoutError(f"{t - t0:.3f} seconds enqueue")
            sleep(min(0.1, deadline - t))
            # It's OK if this sleep is a little long,
            # because the pipe is full and busy.

        fut = concurrent.futures.Future()
        fut.data = {"t0": t0, "timeout": timeout}
        uid = id(fut)
        self._uid_to_futures[uid] = fut
        self._input_buffer.put((uid, x))
        return fut

    def _wait_for_result(self, fut):
        t0 = fut.data["t0"]
        t2 = t0 + fut.data["timeout"]
        try:
            return fut.result(timeout=max(0, t2 - perf_counter()))
            # this may raise an exception originating from RemoteException
        except concurrent.futures.TimeoutError as e:
            fut.cancel()
            raise TimeoutError(f"{perf_counter() - t0:.3f} seconds total") from e

    def _onboard_input(self):
        qin = self._input_buffer
        qout = self._q_in
        while True:
            x = qin.get()
            qout.put(x)
            if x == NOMOREDATA:
                break

    def _gather_output(self):
        threading.current_thread().name = f"{self.__class__.__name__}._gather_output"
        q_out = self._q_out
        futures = self._uid_to_futures

        while True:
            z = q_out.get()
            if z == NOMOREDATA:
                break
            uid, y = z
            fut = futures.pop(uid)
            try:
                if isinstance(y, RemoteException):
                    y = y.exc
                if isinstance(y, BaseException):
                    fut.set_exception(y)
                else:
                    fut.set_result(y)
                fut.data["t1"] = perf_counter()
            except (
                concurrent.futures.InvalidStateError,
                asyncio.InvalidStateError,
            ):
                if fut.cancelled():
                    # Could have been cancelled due to TimeoutError.
                    pass
                else:
                    # Unexpected situation; to be investigated.
                    raise
