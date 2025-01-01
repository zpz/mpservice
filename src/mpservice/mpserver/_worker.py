import logging
import multiprocessing
import multiprocessing.queues
import os
import queue
import threading
from collections.abc import Iterable, Iterator
from queue import Empty
from time import perf_counter
from typing import Any, Callable

from mpservice._queues import SingleLane
from mpservice.multiprocessing import MP_SPAWN_CTX
from mpservice.multiprocessing.remote_exception import RemoteException
from mpservice.streamer import Parmapper
from mpservice.threading import Thread

logger = logging.getLogger(__name__)


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
