"""
``mpservice.mpserver`` provides classes that use `multiprocessing`_ to perform CPU-bound operations
taking advantage of all the CPUs (i.e. cores) on the machine.
Using `threading`_ to perform IO-bound operations is equally supported, although it was not the initial focus.

There are three levels of constructs.

1. On the lowest level is :class:`Worker`. This defines operations on a single input item
   or a batch of items in usual sync code. This is supposed to run in its own process (or thread)
   and use that single process (or thread) only. In other words, to keep things simple, the user-defined
   behavior of :class:`Worker` should not launch processes or threads.

2. On the middle level is :class:`Servlet`. A basic form of Servlet arranges to execute a :class:`Worker` in one or more
   processes (or threads). More advanced forms of Servlet arrange to executive multiple
   Servlets as a sequence or an ensemble, or select a Servlet (from a set of Servlets) to process
   a particular input element based on certain conditions.

3. On the top level is :class:`Server` (or :class:`AsyncServer`). A Server
   handles interfacing with the outside world, while passing the "real work" to
   a :class:`Servlet` and relays the latter's result back to the outside world.
"""

from __future__ import annotations

__all__ = [
    'ServerBacklogFull',
    'Worker',
    'make_worker',
    'PassThrough',
    'Servlet',
    'ProcessServlet',
    'ThreadServlet',
    'SequentialServlet',
    'EnsembleServlet',
    'SwitchServlet',
    'Server',
    'AsyncServer',
    'TimeoutError',
]

import asyncio
import concurrent.futures
import logging
import multiprocessing
import multiprocessing.queues
import os
import queue
import threading
from abc import ABC, abstractmethod
from collections.abc import AsyncIterable, AsyncIterator, Iterable, Iterator, Sequence
from datetime import datetime, timezone
from queue import Empty
from time import perf_counter, sleep
from typing import Any, Callable, Literal, final

from ._common import TimeoutError
from ._queues import SingleLane
from .multiprocessing import MP_SPAWN_CTX, Process
from .multiprocessing.remote_exception import EnsembleError, RemoteException
from .streamer import Parmapper, async_fifo_stream, fifo_stream
from .threading import Thread

# This modules uses the 'spawn' method to create processes.

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


class ServerBacklogFull(RuntimeError):
    def __init__(self, n, x=None):
        super().__init__(n, x)

    def __str__(self):
        n, x = self.args
        if x is None:
            return f'Server is at capacity with {n} items in proces; new request is rejected immediately due to back-pressure'
        return f'Server is at capacity with {n} items in proces; new request is rejected after waiting for {x:.3f} seconds'


class _SimpleProcessQueue(multiprocessing.queues.SimpleQueue):
    """
    A customization of `multiprocessing.queue._SimpleThreadQueue <https://docs.python.org/3/library/multiprocessing.html#multiprocessing._SimpleThreadQueue>`_,
    this class reduces some overhead in a particular use-case in this module,
    where one consumer of the queue greedily grabs elements out of the queue
    towards a batch-size limit.

    This queue is meant to be used between two processes or between a process
    and a thread.

    The main use case of this class is in :meth:`Worker._build_input_batches`.

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


class _SimpleThreadQueue(queue.SimpleQueue):
    """
    A customization of
    `queue._SimpleThreadQueue <https://docs.python.org/3/library/queue.html#queue._SimpleThreadQueue>`_,
    this class is analogous to :class:`_SimpleProcessQueue` but is designed to be used between two threads.
    """

    def __init__(self):
        super().__init__()
        self._rlock = threading.RLock()


class Worker:
    """
    ``Worker`` defines operations on a single input item or a batch of items
    in usual synchronous code. This is supposed to run in its own process (or thread)
    and use that single process (or thread) only.

    Typically a subclass needs to enhance :meth:`__init__` and implement :meth:`call`,
    and leave the other methods intact.

    A ``Worker`` object is not created and used by itself. It is always started by a
    :class:`ProcessServlet` or a :class:`ThreadServlet`.
    """

    @classmethod
    def run(
        cls,
        *,
        q_in: _SimpleProcessQueue | _SimpleThreadQueue,
        q_out: _SimpleProcessQueue | _SimpleThreadQueue,
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

            If used in a :class:`ProcessServlet`, ``q_in`` is a :class:`_SimpleProcessQueue`.
            If used in a :class:`ThreadServlet`, ``q_in`` is either a :class:`_SimpleProcessQueue` or a :class:`_SimpleThreadQueue`.
        q_out
            A queue that carries output values.

            If used in a :class:`ProcessServlet`, ``q_out`` is a :class:`_SimpleProcessQueue`.
            If used in a :class:`ThreadServlet`, ``q_out`` is either a :class:`_SimpleProcessQueue` or a :class:`_SimpleThreadQueue`.

            The elements in ``q_out`` are results for each individual element in ``q_in``.
            "Batching" is an internal optimization for speed;
            ``q_out`` does not contain result batches, but rather results of individuals.
        **init_kwargs
            Passed on to :meth:`__init__`.

            If the worker is going to run in a child process, then elements in ``**kwargs`` go through pickling,
            hence they should consist mainly of small, Python builtin types such as string, number, small dict's, etc.
            Be careful about passing custom class objects in ``**kwargs``.
        """
        try:
            obj = cls(**init_kwargs)
        except Exception:
            q_out.put(None)
            raise
        q_out.put(obj.name)
        # This sends a signal to the caller (or "coordinator")
        # indicating completion of init.
        obj.start(q_in=q_in, q_out=q_out)

    def __init__(
        self,
        *,
        worker_index: int,
        batch_size: int | None = None,
        batch_wait_time: float | None = None,
        cpu_affinity: int | list[int] | None = None,
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
        worker_index
            0-based sequential number of the worker in a "servlet".
            A subclass may use this to distinguish the worker processes/threads
            in the same Servlet and give them some different treatments,
            although they do essentially the same thing. For example,
            let each worker use one particular GPU.

            This argument is provided in :meth:`Servlet.start` when starting
            the worker. A subclass does not worry about providing this argument;
            it simply uses it if needed. The parameter has a proper value
            in ``__init__`` and continues to be available as an instance attribute.

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

        cpu_affinity
            Which CPUs this worker process is going to be "pinned" to.
            If `None`, no pinning.

            If this worker is used in a `ThreadServlet`, this parameter is not specified in the call,
            and its value is `None` and is unused.
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

        self.worker_index = worker_index
        self.batch_size = batch_size
        self.batch_size_log_cadence = 1_000_000
        # Log batch size statistics every this many batches. If ``None``, this log is turned off.
        # This log is for debugging and development purposes.
        # This is ignored if ``batch_size=0``.

        self.batch_wait_time = batch_wait_time
        self.name = f'{multiprocessing.current_process().name}-{threading.current_thread().name}'

        if cpu_affinity is not None:
            if isinstance(cpu_affinity, int):
                cpu_affinity = [cpu_affinity]
            else:
                cpu_affinity = sorted(set(cpu_affinity))
            os.sched_setaffinity(0, cpu_affinity)
        self.cpu_affinity = cpu_affinity
        # If `None`, `os.sched_getaffinity` will return all CPUs.

        self.num_stream_threads: int = 0
        # See :meth:`stream`.

        self.preprocess: Callable[[Any], Any]
        """
        If a subclass has a method ``preprocess`` or an attribute ``preprocess`` that is a free-standing
        function, this method or function must take one data element (not a batch) as the sole, positional
        argument. This processes/transforms the data, and the output is used in :meth:`call`. If this function
        raises an exception, this element is not sent to :meth:`call`; instead, the exception object
        is short-circuited to the output queue.

        When ``self.batch_size > 1``, if :meth:`call` needs to take care of an element of the batch that
        might fail a pre-condition, it is tedious to properly assemble the "good" and "bad" elements to further processing
        or to output in right order. This ``preprocess`` mechanism helps to deal with that situation.

        When a subclass is designed to do non-batching work, this attribute is not necessary, because the same
        concern can be handled in :meth:`call` directly.

        When ``self.preprocess`` is defined, it is used in :meth:`_start_single` and :meth:`_build_input_batches`.

        If a ``Server`` contains a single servlet, which uses this ``Worker``, then the functionalities of this
        ``self.preprocess`` can be largely provided by the parameter ``preprocessor`` to ``Server.stream``.
        In those case, there is no need for this ``self.preprocess``.
        """

    def call(self, x):
        """
        Private methods of this class wait on the input queue to gather "work orders",
        send them to :meth:`call` for processing,
        collect the outputs of :meth:`call`, and put them in the output queue.

        If ``self.batch_size == 0``, then ``x`` is a single
        element, and this method returns result for ``x``.

        `x` is not an ``Exception`` or ``RemoteException`` object;
        such a value would have been routed to the outgoing pipe and not
        passed to this method.
        The same is true for elements of `x` when `self.batch_size > 0`.

        If ``self.batch_size > 0`` (including 1), then
        ``x`` is a list of input data elements, and this
        method returns a list (or `Sequence`_) of results corresponding
        to the elements in ``x``.
        However, this output, when received by private methods of this class,
        will be split and individually put in the output queue,
        so that the elements in the output queue (``q_out``)
        correspond to the elements in the input queue (``q_in``),
        although *vectorized* computation, or *batching*, has happened internally.

        When batching is enabled (i.e. when ``self.batch_size > 0``), the number of
        elements in ``x`` varies between calls depending on the supply
        in the input queue. The list ``x`` does not have a fixed length.

        Be sure to distinguish the case with batching (``batch_size > 0``)
        and the case w/o batching (``batch_size = 0``) where a single
        input is a list. In the latter case, the output of
        this method is the result corresponding to the single input ``x``.
        The result could be anything---it may or may not be a list.

        If a subclass fixes ``batch_size`` in its ``__init__`` to be
        0 or nonzero, make sure this method is implemented accordingly.

        If ``__init__`` does not fix the value of ``batch_size``,
        then a particular instance may have been created with or without batching.
        In this case, this method needs to check ``self.batch_size`` and act accordingly,

        If this method raises exceptions, unless the user has specific things to do,
        do not handle them; just let them happen. They will be handled
        in private methods of this class that call this method.

        Usually this is the only method a subclass needs to customize.
        In rare cases, a subclass may want to customize :meth:`stream` instead of
        or in addition to :meth:`call`.
        """
        raise NotImplementedError

    def stream(self, xx: Iterable) -> Iterator:
        """
        `xx` is an iterable of input `x` to :meth:`call`.
        (If `self.batch_size > 0, then `xx` is an iterable of batches.)
        This function yields the results of :meth:`call` for the elements of `xx`,
        in the right order. If any invocation of :meth:`call` raises an exception,
        the exception object is yielded.

        The elements of `xx` (or elements of the elements of ``x`` when `self.batch_size > 0`)
        are not instances of `Exception` or `RemoteException`. Such values would have
        been routed to the outgoing pipe and not passed to this method.

        The background loop in :meth:`start` calls this method and does not
        call :meth:`call` directly.

        This method is provided mainly for the special use cases where a subclass wants to
        set `self.num_stream_threads` to a positive number, thereby use threading concurrency
        in this method.

        If a subclass re-implements this method without calling :meth:`call`, then
        :meth:`call` does not need to be implemented, because this method is the only place
        of this class that calls :meth:`call`.
        """
        if self.num_stream_threads < 1:
            for x in xx:
                try:
                    y = self.call(x)
                except Exception as e:
                    y = e
                yield y
        else:
            # This branch runs :meth:`call` in threads under these condictions:
            #
            #   - the main operations in :meth:`call` is IO bound that releases the GIL
            #   - the threads share some common context that is set up in this worker's :meth:`__init__`
            #
            # If the second condition is not met, one could just use a :class:`ThreadServlet` with multiple
            # thread workers that run independently of each other.
            #
            # To use this branch, a subclass needs to set `self.num_stream_threads` appropriately
            # after calling `super().__init__`. See tests for an example.
            yield from Parmapper(
                xx,
                self.call,
                executor='thread',
                concurrency=self.num_stream_threads,
                return_exceptions=True,
                parmapper_name=f'{self.__class__.__name__}.stream',
            )

    def cleanup(self, exc=None):
        """
        This method is called when the object exits its service loop and stops.
        This is the place for cleanup code, e.g. releasing resources, exiting
        context managers (that have been entered in :meth:`_init_`), etc.
        """
        pass

    def start(self, *, q_in, q_out):
        """
        This is called by :meth:`run` to kick off the processing loop.

        To stop the processing, pass in the constant ``None``
        through ``q_in``.
        """
        try:
            if self.batch_size > 1:
                self._start_batch(q_in=q_in, q_out=q_out)
            else:
                self._start_single(q_in=q_in, q_out=q_out)
        except KeyboardInterrupt as e:
            q_in.put(None)  # broadcast to one fellow worker
            q_out.put(None)
            print(self.name, 'stopped by KeyboardInterrupt')
            # The process or thread will exit. Don't print the usual
            # exception stuff as that's not needed when user
            # pressed Ctrl-C.
            self.cleanup(e)
            # TODO: do we need to `raise` here?
        except BaseException as e:
            q_in.put(None)
            q_out.put(None)
            self.cleanup(e)
            raise
        else:
            self.cleanup()

    def _start_single(self, *, q_in, q_out):
        def get_input(q_in, q_out, q_uid):
            batched = self.batch_size > 0
            preprocess = getattr(self, 'preprocess', None)

            while True:
                z = q_in.get()
                if z is None:
                    q_in.put(z)  # broadcast to one fellow worker
                    q_out.put(z)
                    break

                uid, x = z

                if preprocess is not None:
                    if not isinstance(x, (Exception, RemoteException)):
                        try:
                            x = preprocess(x)
                        except Exception as e:
                            x = e

                # If it's an exception, short-circuit to output.
                if isinstance(
                    x, Exception
                ):  # `RemteException` is not a subclass of `Exception`.
                    x = RemoteException(x)
                if isinstance(x, RemoteException):
                    q_out.put((uid, x))
                    continue

                q_uid.put(uid)
                if batched:
                    yield [x]
                else:
                    yield x

        q_uid = queue.SimpleQueue()
        batched = self.batch_size > 0
        for y in self.stream(get_input(q_in, q_out, q_uid)):
            if isinstance(y, Exception):
                y = RemoteException(y)
                # There are opportunities to print traceback
                # and details later. Be brief on the logging here.
            else:
                if batched:
                    y = y[0]
            uid = q_uid.get()
            q_out.put((uid, y))
            # Element in the output queue is always a 2-tuple, that is, (ID, value).

    def _start_batch(self, *, q_in, q_out):
        def print_batching_info():
            logger.info(
                '%s: %d batches with sizes %d--%d, mean %.1f',
                self.name,
                n_batches,
                batch_size_min,
                batch_size_max,
                batch_size_mean,
            )

        self._batch_buffer = SingleLane(self.batch_size + 10)
        self._batch_get_called = threading.Event()
        collector_thread = Thread(
            target=self._build_input_batches,
            args=(q_in, q_out),
            name=f'{self.name}._build_input_batches',
        )
        collector_thread.start()

        def get_input(q_in, q_out, q_uids):
            while True:
                batch = self._get_input_batch()
                if batch is None:
                    q_in.put(batch)  # broadcast to fellow workers.
                    q_out.put(batch)
                    break

                # The batch is a list of (ID, value) tuples.
                us = [v[0] for v in batch]
                batch = [v[1] for v in batch]

                q_uids.put(us)
                yield batch

        n_batches = 0
        batch_size_log_cadence = self.batch_size_log_cadence
        q_uids = queue.SimpleQueue()

        try:
            for yy in self.stream(get_input(q_in, q_out, q_uids)):
                if batch_size_log_cadence and n_batches == 0:
                    batch_size_max = -1
                    batch_size_min = 1000000
                    batch_size_mean = 0.0

                uids = q_uids.get()
                if isinstance(yy, Exception):
                    err = RemoteException(yy)
                    for u in uids:
                        q_out.put((u, err))
                else:
                    for z in zip(uids, yy):
                        q_out.put(z)
                # Each element in the output queue is a (ID, value) tuple.

                if batch_size_log_cadence:
                    n = len(uids)
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
                # Finally, log this if `batch_size_log_cadence` is "truthy"
                # and there has been any unlogged batch.
                print_batching_info()
            collector_thread.join()

    def _build_input_batches(self, q_in, q_out):
        # This background thread get elements from `q_in`
        # and put them in `self._batch_buffer`.
        # Exceptions taken out of `q_in` will be short-circuited
        # to `q_out`.

        buffer = self._batch_buffer
        batchsize = self.batch_size
        preprocess = getattr(self, 'preprocess', None)

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
                        if z is None:
                            buffer.put(z)
                            q_in.put(z)  # broadcast to fellow workers.
                            q_out.put(z)
                            return
                        uid, x = z

                        if preprocess is not None:
                            if not isinstance(x, (Exception, RemoteException)):
                                try:
                                    x = preprocess(x)
                                except Exception as e:
                                    x = e

                        if isinstance(x, Exception):
                            q_out.put((uid, RemoteException(x)))
                        elif isinstance(x, RemoteException):
                            q_out.put((uid, x))
                        else:
                            buffer.put((uid, x))

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
                        # Even if `buffer` is not full, we no longer have priority
                        # for more data. Release the lock to give others
                        # a chance.
                        break

    def _get_input_batch(self):
        # This function gets a batch from `self._batch_buffer`.

        extra_timeout = self.batch_wait_time
        batchsize = self.batch_size
        buffer = self._batch_buffer
        out = buffer.get()
        if out is None:
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
            if z is None:
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


class Servlet(ABC):
    """
    A :class:`Servlet` manages the execution of one or more :class:`Worker`.
    We make a distinction between "simple" servlets, including :class:`ProcessServlet` and :class:`ThreadServlet`,
    and "compound" servlets, including :class:`SequentialServlet`, :class:`EnsembleServlet`,
    and :class:`SwitchServlet`.

    A simple servlet arranges to execute one :class:`Worker` in requested number of processes (or threads).
    Optionally, it can specify exactly which CPU(s) each worker process should use.
    Each input item is passed to and processed by exactly one of the processes (or threads).

    A compound servlet arranges to execute multiple :class:`Servlet`\\s as a sequence or an ensemble.
    In addition, there is :class:`SwitchServlet` that acts as a "switch"
    in front of a set of Servlets.
    There's a flavor of recursion in this definition in that a member servlet can very well be
    a compound servlet.

    Great power comes from this recursive definition.
    In principle, we can freely compose and nest the :class:`Servlet` types.
    For example, suppose `W1`, `W2`,..., are :class:`Worker` subclasses,
    then we may design such a workflow,

    ::

        s = SequentialServlet(
                ProcessServlet(W1),
                EnsembleServlet(
                    ThreadServlet(W2),
                    SequentialServlet(ProcessServlet(W3), ThreadServlet(W4)),
                    ),
                EnsembleServlet(
                    Sequetial(ProcessServlet(W5), ProcessServlet(W6)),
                    Sequetial(ProcessServlet(W7), ThreadServlet(W8), ProcessServlet(W9)),
                    ),
            )
    """

    @abstractmethod
    def start(self, q_in, q_out) -> None:
        raise NotImplementedError

    @abstractmethod
    def stop(self) -> None:
        """
        When this method is called, there shouldn't be "pending" work
        in the servlet. If for whatever reason, the servlet is in a busy,
        messy state, it is not required to "wait" for things to finish.
        The priority is to ensure the processes and threads are stopped.

        The primary mechanism to stop a servlet is to put
        the special constant ``None`` in the input queue.
        The user should have done that; but just to be sure,
        this method may do that again.
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def input_queue_type(self) -> Literal['thread', 'process']:
        """
        Indicate whether the input queue can be a "thread queue"
        (i.e. ``queue.Queue``) or needs to be a "process queue"
        (i.e. ``multiprocessing.queues.Queue``).
        If "thread", caller can provide either a thread queue or a
        process queue. If "process", caller must provide a process queue.
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def output_queue_type(self) -> Literal['thread', 'process']:
        """
        Indicate whether the output queue can be a "thread queue"
        (i.e. ``queue.Queue``) or needs to be a "process queue"
        (i.e. ``multiprocessing.queues.Queue``).
        If "thread", caller can provide either a thread queue or a
        process queue. If "process", caller must provide a process queue.
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def workers(self) -> list[Worker]:
        raise NotImplementedError

    @property
    @abstractmethod
    def children(self) -> list:
        raise NotImplementedError

    def _debug_info(self) -> dict:
        zz = []
        for ch in self.children:
            if hasattr(ch, '_debug_info'):
                zz.append(ch._debug_info())
            else:
                zz.append(
                    {
                        'type': ch.__class__.__name__,
                        'name': ch.name,
                        'is_alive': ch.is_alive(),
                    }
                )
        return {
            'type': self.__class__.__name__,
            'children': zz,
        }


class ProcessServlet(Servlet):
    """
    Use this class if the operation is CPU bound.
    """

    def __init__(
        self,
        worker_cls: type[Worker],
        *,
        cpus: None | int | Sequence[None | int | Sequence[int]] = None,
        worker_name: str = None,
        **kwargs,
    ):
        """
        Parameters
        ----------
        worker_cls
            A subclass of :class:`Worker`.
        cpus
            Specifies how many processes to create and how they are pinned
            to specific CPUs.

            The default is ``None``, indicating a single unpinned process.

            If an int, indicating number of (unpinned) processes.

            Otherwise, a list specifiying CPU pinning.
            Each element of the list specifies the CPU pinning of one process.
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
        worker_name
            Prefix to the name of the worker processes. If not provided, a default is
            constructed based on the class name.
        **kwargs
            Passed to the ``__init__`` method of ``worker_cls``.

        Notes
        -----
        When the servlet has multiple processes, the output stream does not need to follow
        the order of the elements in the input stream.
        """
        self._worker_cls = worker_cls
        if cpus is None:
            self._cpus = [None]
        else:
            if isinstance(cpus, int):
                cpus = [None for _ in range(cpus)]
            self._cpus = cpus
        self._init_kwargs = kwargs
        self._workers = []
        self._worker_name = worker_name
        self._started = False

    def start(self, q_in: _SimpleProcessQueue, q_out: _SimpleProcessQueue):
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
        basename = self._worker_name or f'{self._worker_cls.__name__}-process'
        for worker_index, cpu in enumerate(self._cpus):
            # Create as many processes as the length of `cpus`.
            # Each process is pinned to the specified cpu core.
            sname = f'{basename}-{worker_index}'
            logger.info('adding worker <%s> at CPU %s ...', sname, cpu)
            p = Process(
                target=self._worker_cls.run,
                name=sname,
                kwargs={
                    'q_in': q_in,
                    'q_out': q_out,
                    'worker_index': worker_index,
                    'cpu_affinity': cpu,
                    **self._init_kwargs,
                },
            )
            p.start()
            name = q_out.get()
            if name is None:
                p.join()  # this will raise exception b/c worker __init__ failed
            self._workers.append(p)
            logger.debug('   ... worker <%s> is ready', name)

        self._q_in = q_in
        self._q_out = q_out
        logger.info('servlet %s is ready', self._worker_cls.__name__)
        self._started = True

    def stop(self):
        """Stop the workers."""
        assert self._started
        self._q_in.put(None)
        for w in self._workers:
            w.join()
        self._workers = []
        self._started = False

    @property
    def input_queue_type(self):
        return 'process'

    @property
    def output_queue_type(self):
        return 'process'

    @property
    def workers(self):
        return self._workers

    @property
    def children(self):
        return self._workers


class ThreadServlet(Servlet):
    """
    Use this class if the operation is I/O bound (e.g. calling an external service
    to get some info), and computation is very light compared to the I/O part.
    Another use-case of this class is to perform some very simple and quick
    pre-processing or post-processing.
    """

    def __init__(
        self,
        worker_cls: type[Worker],
        *,
        num_threads: None | int = None,
        worker_name: str | None = None,
        **kwargs,
    ):
        """
        Parameters
        ----------
        worker_cls
            A subclass of :class:`Worker`
        num_threads
            The number of threads to create. Each thread will host and run
            an instance of ``worker_cls``. Default ``None`` means 1.
        worker_name
            Prefix to the name of the worker threads. If not provided, a default is
            constructed based on the class name.
        **kwargs
            Passed on the ``__init__`` method of ``worker_cls``.

        Notes
        -----
        When the servlet has multiple threads, the output stream does not need to follow
        the order of the elements in the input stream.
        """
        self._worker_cls = worker_cls
        self._num_threads = num_threads or 1
        self._init_kwargs = kwargs
        self._workers = []
        self._worker_name = worker_name
        self._started = False

    def start(
        self,
        q_in: _SimpleProcessQueue | _SimpleThreadQueue,
        q_out: _SimpleProcessQueue | _SimpleThreadQueue,
    ):
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

            ``q_in`` and ``q_out`` are either :class:`_SimpleProcessQueue`\\s (for processes)
            or :class:`_SimpleThreadQueue`\\s (for threads). Because this servlet may be connected to
            either :class:`ProcessServlet`\\s or :class:`ThreadServlet`\\s, either type of queues may
            be appropriate. In contrast, for :class:`ProcessServlet`, the input and output
            queues are both :class:`_SimpleProcessQueue`\\s.
        """
        assert not self._started
        basename = self._worker_name or f'{self._worker_cls.__name__}-thread'
        for ithread in range(self._num_threads):
            sname = f'{basename}-{ithread}'
            logger.info('adding worker <%s> in thread ...', sname)
            w = Thread(
                target=self._worker_cls.run,
                name=sname,
                kwargs={
                    'q_in': q_in,
                    'q_out': q_out,
                    'worker_index': ithread,
                    **self._init_kwargs,
                },
            )
            w.start()
            name = q_out.get()
            if name is None:
                w.join()  # this will raise exception b/c worker __init__ failed
            self._workers.append(w)
            logger.debug('   ... worker <%s> is ready', name)

        self._q_in = q_in
        self._q_out = q_out
        logger.info('servlet %s is ready', self._worker_cls.__name__)
        self._started = True

    def stop(self):
        """Stop the worker threads."""
        assert self._started
        self._q_in.put(None)
        for w in self._workers:
            w.join()
        self._workers = []
        self._started = False

    @property
    def input_queue_type(self):
        return 'thread'

    @property
    def output_queue_type(self):
        return 'thread'

    @property
    def workers(self):
        return self._workers

    @property
    def children(self):
        return self._workers


class SequentialServlet(Servlet):
    """
    A ``SequentialServlet`` represents
    a sequence of operations performed in order,
    one operations's output becoming the next operation's input.

    Each operation is performed by a "servlet", that is, an instance of any subclass
    of :class:`Servlet`, including :class:`SequentialServlet`.
    However, a `SequentialServlet` as a direct member of another `SequentialServlet`
    may not be beneficial---you may as well flatten it out.

    If any member servlet has multiple workers (threads or processes),
    the output stream does not need to follow the order of the elements in the input stream.
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
        As a rule, use :class:`_SimpleThreadQueue` between two threads; use :class:`_SimpleProcessQueue`
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
                    q2 = _SimpleThreadQueue()
                else:
                    q2 = _SimpleProcessQueue()
                self._qs.append(q2)
            else:
                q2 = q_out
            s.start(q1, q2)
            q1 = q2
        self._q_in = q_in
        self._q_out = q_out
        self._started = True

    def stop(self):
        """Stop the member servlets."""
        assert self._started
        for s in self._servlets:
            s.stop()
        self._qs = []
        self._started = False

    @property
    def workers(self):
        return [w for s in self._servlets for w in s.workers]

    @property
    def children(self):
        return self._servlets

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

    Each operation is performed by a "servlet", that is, an instance of
    any subclass of :class:`Servlet`.

    If ``fail_fast`` is ``True`` (the default), once one ensemble member raises an
    Exception, an ``EnsembleError`` will be returned. Results of the other ensemble members
    that arrive afterwards will be ignored. This is not necessarily "fast"; the main point
    is that the item in question results in an Exception rather than a list that contains
    Exception(s).

    If ``fail_fast`` is ``False``, then Exception results, if any, are included
    in the result list. If all entries in the list are Exceptions, then the result list
    is replaced by an ``EnsembleError``. Here is the logic: if the result list contains
    some valid results and some Exceptions, user may consider it "partially" successful
    and may choose to make use of the valid results in some way; if every ensemble member has failed,
    then the ensemble has failed, hence the result is a single ``EnsembleError``.

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
        the "outside world". Their types, either :class:`_SimpleProcessQueue` or :class:`_SimpleThreadQueue`,
        are decided by the caller.
        """
        assert not self._started
        self._reset()
        self._qin = q_in
        self._qout = q_out
        for s in self._servlets:
            q1 = (
                _SimpleThreadQueue()
                if s.input_queue_type == 'thread'
                else _SimpleProcessQueue()
            )
            q2 = (
                _SimpleThreadQueue()
                if s.output_queue_type == 'thread'
                else _SimpleProcessQueue()
            )
            s.start(q1, q2)
            self._qins.append(q1)
            self._qouts.append(q2)
        t = Thread(target=self._dequeue, name=f'{self.__class__.__name__}._dequeue')
        t.start()
        self._threads.append(t)
        t = Thread(target=self._enqueue, name=f'{self.__class__.__name__}._enqueue')
        t.start()
        self._threads.append(t)
        self._started = True

    def _enqueue(self):
        qin = self._qin
        qout = self._qout
        qins = self._qins
        catalog = self._uid_to_results
        nn = len(qins)
        while True:
            z = qin.get()
            if z is None:  # sentinel for end of input
                for q in qins:
                    q.put(z)  # send the same sentinel to each servlet
                # qout.put(z)
                return

            uid, x = z
            if isinstance(x, BaseException):
                x = RemoteException(x)
            if isinstance(x, RemoteException):
                # short circuit exception to the output queue
                qout.put((uid, x))
                continue

            z = {'y': [None] * nn, 'n': 0}
            # `y` is the list of outputs from the servlets, in order.
            # `n` is number of outputs already received.
            catalog[uid] = z
            for q in qins:
                q.put((uid, x))
                # Element in the queue to each servlet is in the standard format,
                # i.e. a (ID, value) tuple.

    def _dequeue(self):
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
                    if v is None:
                        qout.put(v)
                        return
                        # TODO: this is a little problematic---should we
                        # wait for all ensemble members to see `None`, thus
                        # "driving out" all regular work, before exiting?

                    uid, y = v
                    # `y` can be an exception object or a regular result.
                    z = catalog.get(uid)
                    if z is None:
                        # The entry has been removed due to "fail fast"
                        continue

                    if isinstance(y, BaseException):
                        y = RemoteException(y)

                    z['y'][idx] = y
                    z['n'] += 1

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
                            y = z['y']
                        qout.put((uid, y))

            if all_empty:
                sleep(0.005)  # TODO: what is a good duration?

    def stop(self):
        assert self._started
        for s in self._servlets:
            s.stop()
        self._qin.put(None)
        for t in self._threads:
            t.join()
        self._reset()
        self._started = False

    @property
    def workers(self):
        return [w for s in self._servlets for w in s.workers]

    @property
    def children(self):
        return self._servlets

    @property
    def input_queue_type(self):
        return 'thread'

    @property
    def output_queue_type(self):
        return 'thread'


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

        # Create one queue for each member servlet.
        # Any input element received from `q_in` is passed to
        # `self.switch` to determine which member servlet should
        # process this input; then the input is placed in
        # the appropriate queue.
        for s in self._servlets:
            q1 = (
                _SimpleThreadQueue()
                if s.input_queue_type == 'thread'
                else _SimpleProcessQueue()
            )
            s.start(q1, q_out)
            self._qins.append(q1)

        self._thread_enqueue = Thread(
            target=self._enqueue, name=f'{self.__class__.__name__}._enqueue'
        )
        self._thread_enqueue.start()
        self._started = True

    def stop(self):
        assert self._started
        for s in self._servlets:
            s.stop()
        self._qin.put(None)
        self._thread_enqueue.join()
        self._reset()
        self._started = False

    @abstractmethod
    def switch(self, x) -> int:
        """
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
          This method should not be used to handle such exception cases.

        Returns
        -------
        int
          The index of the member servlet that will receive and process ``x``.
          This number is between 0 and the number of member servlets minus 1,
          inclusive.
        """
        raise NotImplementedError

    def _enqueue(self):
        qin = self._qin
        qout = self._qout
        qins = self._qins
        while True:
            v = qin.get()
            if v is None:  # sentinel for end of input
                for q in qins:
                    q.put(v)  # send the same sentinel to each servlet
                # qout.put(v)
                return

            uid, x = v
            if isinstance(x, BaseException):
                x = RemoteException(x)
            if isinstance(x, RemoteException):
                # short circuit exception to the output queue
                qout.put((uid, x))
                continue

            # Determine which member servlet should process `x`:
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

    @property
    def children(self):
        return self._servlets


def _enter_server(self, gather_args: tuple = None):
    self._q_in = (
        _SimpleThreadQueue()
        if self.servlet.input_queue_type == 'thread'
        else _SimpleProcessQueue()
    )
    self._q_out = (
        _SimpleThreadQueue()
        if self.servlet.output_queue_type == 'thread'
        else _SimpleProcessQueue()
    )
    self.servlet.start(self._q_in, self._q_out)

    if isinstance(self._q_in, _SimpleThreadQueue):
        self._input_buffer = self._q_in
        self._onboard_thread = None
    else:
        self._input_buffer = queue.SimpleQueue()
        # This has unlimited size; `put` never blocks (as long as
        # memory is not blown up!). Input requests respect size limit
        # of `_uid_to_futures`, but is not blocked when putting
        # into this queue. A background thread takes data out of this
        # queue and puts them into `_q_in`, which could block due to socket
        # buffer size limit.

        def _onboard_input():
            qin = self._input_buffer
            qout = self._q_in
            while True:
                x = qin.get()
                qout.put(x)
                if x is None:
                    break

        self._onboard_thread = Thread(
            target=_onboard_input, name=f'{self.__class__.__name__}._onboard_input'
        )
        self._onboard_thread.start()

    self._gather_thread = Thread(
        target=self._gather_output,
        name=f'{self.__class__.__name__}._gather_output',
        args=gather_args or (),
    )
    self._gather_thread.start()


def _server_debug_info(self):
    now = perf_counter()
    futures = sorted(
        (
            {
                **fut.data,
                'id': k,
                'age': now - fut.data['t0'],
                'is_cancelled': fut.cancelled(),
                'is_done_but_not_cancelled': fut.done() and not fut.cancelled(),
            }
            for k, fut in self._uid_to_futures.items()
        ),
        key=lambda x: x['age'],
    )

    if self._onboard_thread is None:
        onboard_thread = None
    else:
        onboard_thread = 'is_alive' if self._onboard_thread.is_alive() else 'done'

    return {
        'type': self.__class__.__name__,
        'datetime': str(datetime.now(timezone.utc)),
        'perf_counter': now,
        'capacity': self.capacity,
        'servlet': self.servlet._debug_info(),
        'active_processes': [str(v) for v in multiprocessing.active_children()],
        'active_threads': [str(v) for v in threading.enumerate()],
        'onboard_thread': onboard_thread,
        'gather_thread': 'is_alive' if self._gather_thread.is_alive() else 'done',
        'backlog': futures,
    }


class Server:
    """
    The "interfacing" and "scheduling" code of :class:`Server`
    runs in the "main process".
    Two usage patterns are supported, namely making individual
    calls to the service to get individual results, or flowing
    a (potentially unlimited) stream of data through the service
    to get a stream of results.

    A typical setup looks like this::

        servlet = SequentialServlet(...)  # or other types of servlets
        server = Server(servlet)
        with server:
            z = server.call('abc')

            for x, y in server.stream(data, return_x=True):
                print(x, y)


    Code in the "workers" (of `servlet`) should raise exceptions as it normally does, without handling them,
    if it considers the situation to be non-recoverable, e.g. input is of wrong type.
    The exceptions will be funneled through the pipelines and raised to the end-user
    with useful traceback info.

    The user's main work is implementing the operations in the "workers".
    Another task (of some trial and error) by the user is experimenting with
    CPU allocations among workers to achieve best performance.

    :class:`Server` has an async counterpart named :class:`AsyncServer`.
    """

    @final
    @classmethod
    def get_mp_context(cls):
        """
        If subclasses need to use additional Queues, Locks, Conditions, etc,
        they should create them out of this context.
        This returns a spawn context.

        Subclasses should not customize this method.
        """
        return MP_SPAWN_CTX

    def __init__(
        self,
        servlet: Servlet,
        *,
        capacity: int = 256,
    ):
        """
        Parameters
        ----------
        servlet
            The servlet to run by this server.

            The ``servlet`` has not "started". Its :meth:`~Servlet.start` will be called
            in :meth:`__enter__`.

        capacity
            Max number of requests concurrently in progress within this server,
            all pipes/servlets/stages combined.

            For each request received, a UUID is assigned to it.
            An entry is added to an internal book-keeping dict.
            Then this ID along with the input data enter the processing pipeline.
            Coming out of the pipeline is the ID along with the result.
            The book-keeping record is found by the ID, used, and removed from the dict.
            Removal of finished requests from the book-keeping dict makes room
            for new requests.
            The ``capacity`` value is simply the size limit on this internal
            book-keeping dict.

            .. seealso: documentation of the method :meth:`call`.
        """
        self.servlet = servlet
        assert capacity > 0
        self._capacity = capacity
        self._uid_to_futures = {}
        # Size of this dict is capped at `self._capacity`.
        # A few places need to enforce this size limit.

    def __getstate__(self):
        raise TypeError(f"cannot pickle '{self.__class__.__name__!r}' object")

    @property
    def capacity(self) -> int:
        """
        The value of the parameter `capacity` to :meth:`__init__`.
        """
        return self._capacity

    @property
    def backlog(self) -> int:
        """
        The number of items currently being processed in the server.
        They may be in various stages.
        """
        return len(self._uid_to_futures)

    def __enter__(self):
        """
        The main operations conducted in this method include:

        - Start the servlet (hence creating and starting :class:`Worker` instances).

        - Set up queues between the main thread (this "server" object) and the workers
          for passing input elements (and intermediate results) and results.

        - Set up background threads responsible for taking incoming calls and gathering/returning results.
          Roughly speaking, when :meth:`call` is invoked, its input is handled by a thread, which
          places the input in a pipeline with appropriate book-keeping; it then waits for the result,
          which becomes available once another thread gathers the result from (another end of) the pipeline.
        """
        self._pipeline_notfull = threading.Condition()
        _enter_server(self)
        return self

    def __exit__(self, *args):
        """
        The main operations conducted in this method include:

        - Place a special sentinel in the pipeline to indicate the end of operations; all :class:`Worker`\\s
          in the servlet will eventually see the sentinel and exit.
        - Wait for the servlet and all helper threads to exit.
        """
        self.servlet.stop()
        self._gather_thread.join()
        if self._onboard_thread is not None:
            self._input_buffer.put(None)
            self._onboard_thread.join()

    def call(self, x, /, *, timeout: int | float = 60, backpressure: bool = True):
        """
        Serve one request with input ``x``, return the result.

        This method is thread-safe, meaning it can be called from multiple threads
        concurrently.

        If the operation failed due to :class:`ServerBacklogFull`, :class:`TimeoutError`,
        or any exception in any parts of the server, the exception is propagated.

        Parameters
        ----------
        x
            Input data element.
        timeout
            In seconds. If result is not ready after this time, :class:`TimeoutError` is raised.

            There are two situations where timeout happens.
            At first, ``x`` is placed in an input queue for processing.
            This step is called "enqueue".
            If the queue is full for the moment, the code will wait (if ``backpressure`` is ``False``).
            If a spot does not become available during the ``timeout`` period,
            the :class:`ServerBacklogFull` error message will be "... seconds enqueue".

            Effectively, the input queue is considered full if
            there are ``capacity`` count of ongoing (i.e. received but not yet finished) requests
            in the server in all stages combined, where ``capacity`` is a parameter to :meth:`__init__`.

            Once ``x`` is placed in the input queue, code will wait for the result to come out
            at the end of an output queue. If result is not yet ready when the ``timeout`` period
            is over, the :class:`TimeoutError` message will be ".. seconds total".
            This waitout period includes the time that has been spent
            in the "enqueue" step, that is, the timer starts upon receiving the request,
            i.e. at the beginning of the function ``call``.
        backpressure
            If ``True``, and the input queue is full (that is, ``self.backlog == self.capacity``),
            do not wait; raise :class:`ServerBacklogFull`
            right away. If ``False``, wait on the input queue for as long as
            ``timeout`` seconds.

            The exception ``ServerBacklogFull`` may indicate an erronous situation---somehow
            the server is (almost) stuck and can not process requests as quickly as expected---or
            a valid situation---the server is getting requests faster than its expected load.
        """
        fut = self._enqueue(x, timeout, backpressure)
        return self._wait_for_result(fut)

    def _enqueue(
        self, x, timeout: float, backpressure: bool
    ) -> concurrent.futures.Future:
        # This method is called by `call` or `stream`.
        # This method is thread-safe.
        t0 = perf_counter()
        pipeline = self._uid_to_futures

        fut = concurrent.futures.Future()
        fut.data = {
            't0': t0,
            't1': t0,  # end of enqueuing, to be updated
            'deadline': t0 + timeout,
        }
        uid = id(fut)

        with self._pipeline_notfull:
            if len(pipeline) >= self._capacity:
                if backpressure:
                    raise ServerBacklogFull(len(pipeline))
                if not self._pipeline_notfull.wait(timeout * 0.99):
                    raise ServerBacklogFull(len(pipeline), perf_counter() - t0)

            self._input_buffer.put((uid, x))
            pipeline[uid] = fut
            # See doc of counterpart methods in `AsyncServer`.

        fut.data['t1'] = perf_counter()
        return fut

    def _wait_for_result(self, fut: concurrent.futures.Future):
        # This method is thread-safe.
        try:
            return fut.result(timeout=fut.data['deadline'] - perf_counter())
            # If timeout is negative, it doesn't wait.
            # This may raise an exception originating from RemoteException
        except concurrent.futures.TimeoutError as e:
            fut.cancel()
            t0 = fut.data['t0']
            fut.data['t_cancelled'] = perf_counter()
            raise TimeoutError(
                f"{fut.data['t1'] - t0:.3f} seconds enqueue, {perf_counter() - t0:.3f} seconds total"
            ) from e

    def _gather_output(self) -> None:
        q_out = self._q_out
        pipeline = self._uid_to_futures

        q_notify = queue.SimpleQueue()

        def notify():
            q = q_notify
            pipeline_notfull = self._pipeline_notfull
            while True:
                z = q.get()
                if z is None:
                    break
                with pipeline_notfull:
                    pipeline_notfull.notify()

        notification_thread = Thread(target=notify)
        notification_thread.start()

        try:
            while True:
                z = q_out.get()
                if z is None:
                    break
                uid, y = z

                try:
                    fut = pipeline.pop(uid)
                except KeyError:
                    # This should not happen, but see doc of `_enqueue`
                    # `dict.pop` is atomic; see https://stackoverflow.com/a/17326099/6178706
                    logger.warning(
                        f'the Future object for uid `{uid}` is not found in the backlog ledger'
                    )
                    continue

                if isinstance(y, RemoteException):
                    y = y.exc
                if not fut.cancelled():
                    if isinstance(y, BaseException):
                        fut.set_exception(y)
                    else:
                        fut.set_result(y)
                fut.data['t2'] = perf_counter()
                q_notify.put(1)
        finally:
            q_notify.put(None)
            notification_thread.join()

    def debug_info(self) -> dict:
        """
        Return a dict with various pieces of info, about the status and health of the server,
        that may be helpful for debugging. Example content includes `self.backlog`,
        active child processes, status (alive/dead) of helper threads.

        The content of the returned dict is str, int, or other types that are friendly to `json`.
        """
        return _server_debug_info(self)

    def stream(
        self,
        data_stream: Iterable,
        /,
        *,
        return_x: bool = False,
        return_exceptions: bool = False,
        timeout: int | float = 3600,
        preprocessor: Callable = None,
    ) -> Iterator:
        """
        Use this method for high-throughput processing of a long stream of
        data elements. In theory, this method achieves the throughput upper-bound
        of the server, as it saturates the pipeline.

        The order of elements in the stream is preserved, i.e.,
        elements in the output stream correspond to elements
        in the input stream in the same order.

        This method is thread-safe, that is, multiple threads can call this method concurrently
        with their respective input streams. The server will serve the requests interleaved
        as fast as it can. In that case, you may want to use ``return_exceptions=True``
        in each of the calls so that one's exception does not propagate and halt the program.

        It is also fine to have calls to :meth:`call` and :meth:`stream` concurrently
        (from multiple threads, for example).

        Parameters
        ----------
        data_stream
            An (possibly unlimited) iterable of input data elements.
        return_x
            If ``True``, each output element is a length-two tuple
            containing the input data and the result.
            If ``False``, each output element is just the result.
        return_exceptions
            If ``True``, any Exception object will be produced in the output stream
            in place of the would-be regular result.
            If ``False``, exceptions will be propagated right away, crashing the program.
        timeout
            Interpreted the same as in :meth:`call`.

            In a streaming task, "timeout" is usually not a concern compared
            to overall throughput. You can usually leave it at the default value or make it
            even larger as needed.
        preprocessor
            See :func:`fifo_stream`.
        """
        return fifo_stream(
            data_stream,
            self._enqueue,
            name=f'{self.__class__.__name__}.worker_thread',
            return_x=return_x,
            return_exceptions=return_exceptions,
            capacity=self.capacity,
            timeout=timeout,
            backpressure=False,
            preprocessor=preprocessor,
        )


class AsyncServer:
    """
    An ``AsyncServer`` object must be started in an async context manager.
    The primary methods :meth:`call` and :meth:`stream` are async.

    Most concepts and usage are analogous to :class:`Server`.
    """

    @final
    @classmethod
    def get_mp_context(cls):
        return MP_SPAWN_CTX

    def __init__(
        self,
        servlet: Servlet,
        *,
        capacity: int = 256,
    ):
        self.servlet = servlet
        assert capacity > 0
        self._capacity = capacity
        self._uid_to_futures = {}
        # Size of this dict is capped at `self._capacity`.
        # A few places need to enforce this size limit.

    def __getstate__(self):
        raise TypeError(f"cannot pickle '{self.__class__.__name__!r}' object")

    @property
    def capacity(self) -> int:
        return self._capacity

    @property
    def backlog(self) -> int:
        return len(self._uid_to_futures)

    async def __aenter__(self):
        self._pipeline_notfull = asyncio.Condition()
        self._pipeline_notfull_notifications = {}
        _enter_server(self, (asyncio.get_running_loop(),))
        return self

    async def __aexit__(self, *args):
        self.servlet.stop()
        self._gather_thread.join()

        pipenotfull = self._pipeline_notfull
        notifs = self._pipeline_notfull_notifications
        while notifs:
            # This could happen, e.g. a request has timedout and left,
            # then after a while its result comes out and `_gather_output` sends
            # a notification, but no more request is waiting.
            # If these "pending" notifications are not taken care of, will get warnings like this:
            #
            #   sys:1: RuntimeWarning: coroutine 'AsyncServer._gather_output.<locals>.notify' was never awaited
            async with pipenotfull:
                try:
                    await asyncio.wait_for(pipenotfull.wait(), 0.01)
                except asyncio.TimeoutError:
                    pass

        if self._onboard_thread is not None:
            self._input_buffer.put(None)
            self._onboard_thread.join()

    async def call(self, x, /, *, timeout: int | float = 60, backpressure: bool = True):
        """
        When this is called, this server is usually backing a (http or other) service
        using some async framework.
        Concurrent async calls to this method may happen.

        .. seealso:: :meth:`Server.call`
        """
        fut = await self._enqueue(x, timeout=timeout, backpressure=backpressure)
        return await self._wait_for_result(fut)

    async def _enqueue(self, x, timeout: float, backpressure: bool) -> asyncio.Future:
        t0 = perf_counter()
        pipeline = self._uid_to_futures

        fut = asyncio.get_running_loop().create_future()
        fut.data = {
            't0': t0,
            't1': t0,  # end of enqueuing; to be updated
            'deadline': t0 + timeout,
        }
        uid = id(fut)

        async with self._pipeline_notfull:
            if len(pipeline) >= self._capacity:
                if backpressure:
                    raise ServerBacklogFull(len(pipeline))
                    # If this is behind a HTTP service, should return
                    # code 503 (Service Unavailable) to client.
                try:
                    await asyncio.wait_for(
                        self._pipeline_notfull.wait(), timeout * 0.99
                    )
                except (
                    asyncio.TimeoutError,
                    TimeoutError,
                ):  # should be the first one, but official doc referrs to the second
                    raise ServerBacklogFull(len(pipeline), perf_counter() - t0)

            # We can't accept situation that an entry is placed in `pipeline`
            # but not in `_input_buffer`, for that entry would be stuck in `pipeline`
            # and never taken out.
            #
            # For that to happen, this function's execution needs to be abandoned after
            # `pipeline[uid] = fut` but before `self._input_buffer.put((uid, x))`.
            # Although this doesn't seem likely, I can think of one scenario:
            #
            #   User imposes a timeout on the call to `_enqueue`. Then timeout
            #   (and consequently `asyncio.CancelledError`) could happen anywhere,
            #   I guess.
            #
            # If that is ever an issue or concern, there are two solutions:
            # (1) put the entry in `_input_buffer` first, and `pipeline` second; in combination,
            #     change `pipeline.pop(uid)` in `_gather_output` to `pipeline.pop(uid, None)`;
            # (2) in `call`, protect the calll to `_enqueue` by an `asyncio.shield`.

            self._input_buffer.put((uid, x))
            pipeline[uid] = fut

        fut.data['t1'] = perf_counter()  # enqueing finished if `t1` != `t0`
        return fut

    async def _wait_for_result(self, fut: asyncio.Future):
        try:
            await asyncio.wait_for(fut, fut.data['deadline'] - perf_counter())
        except (asyncio.TimeoutError, TimeoutError):
            t0 = fut.data['t0']
            fut.cancel()
            fut.data['t_cancelled'] = perf_counter()  # time of abandonment
            raise TimeoutError(
                f"{fut.data['t1'] - t0:.3f} seconds enqueue, {perf_counter() - t0:.3f} seconds total"
            )
            # `fut` is also cancelled; to confirm
        except asyncio.CancelledError:
            fut.cancel()
            fut.data['t_cancelled'] = perf_counter()  # time of abandonment
            raise

        # If this call is cancelled by caller, then `fut` is also cancelled.

        return fut.result()
        # This could raise an exception originating from RemoteException.

    def _gather_output(self, loop) -> None:
        q_out = self._q_out
        pipeline = self._uid_to_futures
        pipeline_notfull = self._pipeline_notfull

        async def notify():
            async with pipeline_notfull:
                pipeline_notfull.notify()

        notifications = self._pipeline_notfull_notifications  # {}

        while True:
            z = q_out.get()
            if z is None:
                break
            uid, y = z
            try:
                fut = pipeline.pop(uid)
            except KeyError:
                # This should not happen, but see doc of `_enqueue`
                # `dict.pop` is atomic; see https://stackoverflow.com/a/17326099/6178706
                logger.warning(
                    f'the Future object for uid `{uid}` is not found in the backlog ledger'
                )
                continue

            if not fut.cancelled():
                if isinstance(y, RemoteException):
                    y = y.exc
                if isinstance(y, BaseException):
                    loop.call_soon_threadsafe(fut.set_exception, y)
                else:
                    loop.call_soon_threadsafe(fut.set_result, y)
                fut.data['t2'] = perf_counter()

            f = asyncio.run_coroutine_threadsafe(notify(), loop)
            notifications[id(f)] = f
            f.add_done_callback(lambda fut: notifications.pop(id(fut)))

    def debug_info(self) -> dict:
        return _server_debug_info(self)

    async def stream(
        self,
        data_stream: AsyncIterable,
        /,
        *,
        return_x: bool = False,
        return_exceptions: bool = False,
        timeout: int | float = 3600,
        preprocessor: Callable = None,
    ) -> AsyncIterator:
        """
        Calls to :meth:`stream` and :meth:`call` can happen at the same time
        (i.e. interleaved); multiple calls to :meth:`stream` can also happen
        at the same time by different "users" (in the same thread).

        .. seealso:: :meth:`Server.stream`
        """
        async for z in async_fifo_stream(
            data_stream,
            self._enqueue,
            capacity=self._capacity,
            timeout=timeout,
            backpressure=False,
            preprocessor=preprocessor,
            return_x=return_x,
            return_exceptions=return_exceptions,
        ):
            yield z


def make_worker(func: Callable[[Any], Any]) -> type[Worker]:
    """
    This function defines and returns a simple :class:`Worker` subclass
    for quick, "on-the-fly" use.
    This can be useful when we want to introduce simple servlets
    for pre-processing and post-processing.

    Parameters
    ----------
    func
        This function is what happens in the method :meth:`~Worker.call`.
    """

    class MyWorker(Worker):
        def call(self, x):
            return func(x)

    MyWorker.__name__ = f'Worker-{func.__name__}'
    return MyWorker


class PassThrough(Worker):
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
        ss = SequentialServlet(s, ThreadServlet(make_worker(combine)))
    """

    def call(self, x):
        return x
