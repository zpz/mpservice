'''Service using multiprocessing to perform CPU-bound operations
making use of multiple CPUs (i.e. cores) on the machine.

Each "worker" runs in a separate process. There can be multiple
workers forming a sequence or an ensemble. Exact CPU allocation---which
CPUs are used by which workers---can be controlled
to achieve high efficiency.

The "interface" between the service and the outside world
resides in the "main process".
Two usage patterns are supported, namely making (concurrent) individual
calls to the service to get individual results, or flowing
a potentially unlimited stream of data through the service
to get a stream of results. Each usage supports a sync API and an async API.

The interface and scheduling code in the main process
is all provided, and usually does not need much customization.
One task of trial and error by the user is experimenting with
CPU allocations among workers to achieve best performance.

The user's main work is implementing the operations in the "workers".
These are conventional sync functions.

Typical usage:

    class MyServlet1(Servlet):
        def __init_(self, ..):
            ...

        def call(self, x):
            ...

    class MyServlet2(Servlet):
        def __init__(self, ...):
            ...

        def call(self, x):
            ...

    class MyServer(SequentialServer):
        def __init__(self, ...):
            ...
            self.add_servlet(MyServlet1, ...)
            self.add_servlet(MyServlet2, ...)

    with MyServer(...) as server:
        y = server.call(x)

        output = server.stream(data)
        for y in output:
            ...

Reference: [Service Batching from Scratch, Again](https://zpz.github.io/blog/batched-service-redesign/).
This article describes roughly version 0.7.2.
'''

import asyncio
import concurrent.futures
import logging
import multiprocessing
import queue
import threading
import time
import traceback
import warnings
from abc import ABCMeta, abstractmethod
from concurrent.futures import Future
from multiprocessing import synchronize, Queue
from time import perf_counter
from typing import List, Type, Sequence, Union, Callable

import psutil
from overrides import EnforceOverrides, overrides

from .remote_exception import RemoteException
from .socket import SocketServer, write_record
from .util import forward_logs, logger_thread, IterQueue, FutureIterQueue


# Set level for logs produced by the standard `multiprocessing` module.
multiprocessing.log_to_stderr(logging.WARNING)

logger = logging.getLogger(__name__)


class TimeoutError(Exception):
    pass


class EnqueueTimeout(TimeoutError):
    def __init__(self, duration, queue_idx):
        super().__init__(duration, queue_idx)

    def __str__(self):
        return f"enqueue timed out after {self.args[0]} seconds at queue {self.args[1]}"


class TotalTimeout(TimeoutError):
    def __init__(self, duration):
        super().__init__(duration)

    def __str__(self):
        return f"total timed out after {self.args[0]} seconds"


class Servlet(metaclass=ABCMeta):
    # Typically a subclass needs to enhance
    # `__init__` and implement `call`.

    @classmethod
    def run(cls, *,
            q_in: Queue,
            q_out: Queue,
            q_err: Queue,
            q_in_lock: synchronize.Lock,
            q_log: Queue,
            should_stop: multiprocessing.Event,
            cpus: Sequence[int] = None,
            **init_kwargs):
        '''
        This classmethod runs in the worker process to construct
        the worker object and start its processing loop.

        This function is the parameter `target` to `multiprocessing.Process`
        called by `MPServer` in the main process. As such, elements
        in `init_kwargs` go through pickling, hence they should consist
        mainly of small, native types such as string, number,small dict.
        Be careful about passing custom class objects in `init_kwargs`.
        '''
        forward_logs(q_log)
        if cpus:
            psutil.Process().cpu_affinity(cpus=cpus)
        obj = cls(**init_kwargs)
        obj.start(q_in=q_in,
                  q_out=q_out,
                  q_err=q_err,
                  q_in_lock=q_in_lock,
                  should_stop=should_stop,
                  )

    def __init__(self, *,
                 batch_size: int = None,
                 batch_wait_time: float = None):
        '''
        `batch_size`: max batch size; see `call`.

        `batch_wait_time`: seconds, may be 0; the total duration
            to wait for one batch after the first item has arrived.

        To leverage batching, it is recommended to set `batch_wait_time`
        to a small positive value. If it is 0, worker will never wait
        when another item is not already there in the pipeline;
        in other words, batching happens only for items that are already
        "piled up" at the moment.

        When `batch_wait_time > 0`, it will hurt performance during
        sequential calls to the service, because worker will always
        wait for this long for additional items to come and form a batch,
        while additional item will never come during sequential use.
        However, when batching is enabled, sequential calls are not
        the intended use case. Beware of this in benchmarking.

        Remember to pass in `batch_size` in accordance with the implementation
        of `call`.

        If the algorithm can not vectorize the computation, then there is
        no advantage in enabling batching. In that case, the subclass should
        simply fix `batch_size` to 0 in their `__init__`.

        The `__init__` of a subclass may define additional input parameters;
        they can be passed in through `run`.
        '''
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
        self.batch_wait_time = batch_wait_time
        self.name = multiprocessing.current_process().name

    def start(self, *, q_in, q_out, q_err, q_in_lock, should_stop):
        q_err.put(self.name)
        if self.batch_size > 1:
            self._start_batch(q_in=q_in, q_out=q_out, q_err=q_err,
                              q_in_lock=q_in_lock, should_stop=should_stop)
        else:
            self._start_single(q_in=q_in, q_out=q_out, q_err=q_err, should_stop=should_stop)

    def _start_single(self, *, q_in, q_out, q_err, should_stop):
        batch_size = self.batch_size
        while True:
            try:
                uid, x = q_in.get(timeout=0.5)
            except queue.Empty:
                if should_stop.is_set():
                    break
                continue

            try:
                if batch_size:
                    y = self.call([x])[0]
                else:
                    y = self.call(x)
                q_out.put((uid, y))

            except Exception:
                # There are opportunities to print traceback
                # and details later using the `RemoteException`
                # object. Be brief on the logging here.
                err = RemoteException()
                q_err.put((uid, err))

    def _start_batch(self, *, q_in, q_out, q_err, q_in_lock, should_stop):
        batch_size = self.batch_size
        batch_wait_time = self.batch_wait_time

        batch_size_max = -1
        batch_size_min = 1000000
        batch_size_mean = 0.0
        n_batches = 0

        while True:
            batch = []
            uids = []
            n = 0
            with q_in_lock:
                # Hold the lock to let one worker get as many
                # as possible in order to leverage batching.
                while True:
                    try:
                        uid, x = q_in.get(timeout=0.5)
                        break
                    except queue.Empty:
                        if should_stop.is_set():
                            if n_batches and n_batches % 1000 != 0:
                                logger.info('batch size stats (count, max, min, mean): %d, %d, %d, %.1f',
                                            n_batches, batch_size_max, batch_size_min, batch_size_mean)
                            return

                batch.append(x)
                uids.append(uid)
                n += 1

                time_left = batch_wait_time
                wait_until = perf_counter() + time_left
                # Wait time starts after getting the first item,
                # because however long the first item takes to come
                # is not the worker's fault.

                while n < batch_size:
                    try:
                        # If there is remaining time, wait up to
                        # that long.
                        # Otherwise, get the next item if it is there
                        # right now (i.e. no waiting) even if we
                        # are already over time. That is, if supply
                        # has piled up, then will get up to the
                        # batch capacity.
                        if time_left > 0.001:
                            uid, x = q_in.get(timeout=time_left)
                        else:
                            uid, x = q_in.get_nowait()
                    except queue.Empty:
                        break

                    batch.append(x)
                    uids.append(uid)
                    n += 1
                    time_left = wait_until - perf_counter()

            try:
                results = self.call(batch)
            except Exception:
                err = RemoteException()
                for uid in uids:
                    q_err.put((uid, err))
            else:
                for uid, y in zip(uids, results):
                    q_out.put((uid, y))

            n_batches += 1
            batch_size_max = max(batch_size_max, n)
            batch_size_min = min(batch_size_min, n)
            batch_size_mean = (batch_size_mean * (n_batches - 1) + n) / n_batches
            if n_batches % 1000 == 0:
                logger.info('batch size stats (count, max, min, mean): %d, %d, %d, %.1f',
                            n_batches, batch_size_max, batch_size_min, batch_size_mean)

    @abstractmethod
    def call(self, x):
        # If `self.batch_size == 0`, then `x` is a single
        # element, and this method returns result for `x`.
        #
        # If `self.batch_size > 0` (including 1), then
        # `x` is a list of input data elements, and this
        # method returns a list of results corresponding
        # to the elements in `x`.
        # However, the service (see near the end of `_start_batch`)
        # will split the resultant list
        # into single results for individual elements of `x`.
        # This is *vectorized* computation, or *batching*,
        # handled by this service pipeline.
        #
        # When batching is enabled, the number of
        # elements in `x` varies between calls depending on the supply
        # of data. The list `x` does not have a fixed length.
        #
        # Be sure to distinguish this from the case where a single
        # input is naturally a list. In that case, the output of
        # the current method is the result corresponding to
        # the single input `x`. The result could be anything---it
        # may or may not be a list.
        #
        # If a subclass fixes `batch_size` in its `__init__` to be
        # 0 or nonzero, make sure the current method is implemented
        # accordingly. If `__init__` has no requirement on the value
        # of `batch_size`, then the current method needs to check
        # `self.batch_size` and act accordingly, because it is up to
        # the uer in `MPServer` to specify a zero or nonzero `batch_size`
        # for this worker.
        #
        # In case of exceptions, unless the user has specific things to do,
        # do not handle them; just let them happen. They will be handled
        # in `_start_batch` and `_start_single`.
        raise NotImplementedError


class MPServer(EnforceOverrides, metaclass=ABCMeta):
    MP_CLASS = multiprocessing.get_context('spawn')
    # This class attribute is provided because in some cases
    # one may want to use `torch.multiprocessing`, which is
    # a drop-in replacement for the standard `multiprocessing`
    # with some enhancements related to data sharing between
    # processes.

    SLEEP_ENQUEUE = 0.00015
    SLEEP_DEQUEUE = 0.00011
    TIMEOUT_ENQUEUE = 1
    TIMEOUT_TOTAL = 10

    def __init__(self, *,
                 max_queue_size: int = None,
                 cpus: Sequence[int] = None,
                 ):
        '''
        `cpus`: specifies the cpu for the "main process",
        i.e. the process in which this server objects resides.
        '''
        self.max_queue_size = max_queue_size or 1024
        self._q_in_lock: List[synchronize.Lock] = []
        self._q_err: Queue = self.MP_CLASS.Queue(self.max_queue_size)
        self._q_log: Queue = self.MP_CLASS.Queue()
        self._servlets: List[multiprocessing.Process] = []
        self._uid_to_futures = {}

        self._should_stop = self.MP_CLASS.Event()
        # `_add_servlet` can only be called after this.
        # In other words, subclasses should call `super().__init__`
        # at the beginning of their `__init__` rather than
        # at the end.

        self._thread_pool = concurrent.futures.ThreadPoolExecutor(128)
        self._thread_tasks = {}

        self.started = 0
        if cpus:
            # Pin this coordinating thread to the specified CPUs.
            psutil.Process().cpu_affinity(cpus=cpus)

    def __del__(self):
        if self.started:
            self.stop()

    def __enter__(self):
        # After adding servlets, all other methods of this object
        # should be used with context manager `__enter__`/`__exit__`
        # or `start`/`stop`.
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        if exc_type:
            logger.error("Exiting %s with exception:\n%s\n%s\n%s",
                         self.__class__.__name__,
                         exc_type, exc_value,
                         ''.join(traceback.format_tb(exc_traceback)),
                         )
            print(f"Exiting {self.__class__.__name__} with exception:")
            print('exc_type:', exc_type)
            print('exc_value:', exc_value)
            print('traceback:')
            traceback.print_tb(exc_traceback)
        self.stop()

    def start(self, sequential: bool = True):
        assert self._servlets
        self.started += 1
        if self.started > 1:
            # Re-entry.
            return

        t = self._thread_pool.submit(logger_thread, self._q_log)
        t.add_done_callback(self._thread_tasks_done_callback)
        self._thread_tasks[t] = None

        if sequential:
            # Start the servlets one by one.
            n = len(self._servlets)
            k = 0
            for m in self._servlets:
                logger.debug(f"starting servlet in Process {m}")
                m.start()
                name = self._q_err.get()
                k += 1
                logger.info(f"{k}/{n}: servlet <{name}> is ready")
        else:
            # Start the servlets concurrently.
            # In some situations, concurrent servlet launch
            # may have issues, e.g. all servlets read the same
            # large file on startup, or all servlets download
            # the same dataset on startup.
            n = 0
            for m in self._servlets:
                logger.debug(f"starting servlet in Process {m}")
                m.start()
                n += 1
            k = 0
            while k < n:
                name = self._q_err.get()
                k += 1
                logger.info(f"{k}/{n}: servlet <{name}> is ready")

        t = self._thread_pool.submit(self._gather_output)
        t.add_done_callback(self._thread_tasks_done_callback)
        self._thread_tasks[t] = None
        t = self._thread_pool.submit(self._gather_error)
        t.add_done_callback(self._thread_tasks_done_callback)
        self._thread_tasks[t] = None

    def stop(self):
        assert self.started > 0
        self.started -= 1
        if self.started > 0:
            # Exiting one nested level.
            return

        self._q_log.put(None)
        self._should_stop.set()
        for m in self._servlets:
            m.join()
        done, not_done = concurrent.futures.wait(list(self._thread_tasks), timeout=10)
        for t in not_done:
            t.cancel()

        self._thread_pool.shutdown()
        # TODO: in Python 3.9+, use argument `cancel_futures=True`.

        # Reset CPU affinity.
        psutil.Process().cpu_affinity(cpus=[])

    async def async_call(self, x, /, *,
                         enqueue_timeout: Union[int, float] = None,
                         total_timeout: Union[int, float] = None,
                         ):
        enqueue_timeout, total_timeout = self._resolve_timeout(
            enqueue_timeout=enqueue_timeout, total_timeout=total_timeout,
        )
        uid, fut = await self._async_call_enqueue(x, enqueue_timeout=enqueue_timeout)
        return await self._async_call_wait_for_result(uid, fut, total_timeout=total_timeout)

    def call(self, x, /, *,
             enqueue_timeout: Union[int, float] = None,
             total_timeout: Union[int, float] = None,
             ):
        '''
        Sometimes a subclass may want to override this method
        to add preprocessing of the input and postprocessing of
        the output, while calling `super().call(...)` in the middle.

        However, be careful to maintain consistency between `call` and `stream`.
        '''
        enqueue_timeout, total_timeout = self._resolve_timeout(
            enqueue_timeout=enqueue_timeout, total_timeout=total_timeout,
        )

        uid, fut = self._call_enqueue(x, enqueue_timeout=enqueue_timeout)
        return self._call_wait_for_result(uid, fut, total_timeout=total_timeout)

    def stream(self, data_stream, /, *,
               return_exceptions: bool = False,
               return_x: bool = False,
               backlog: int = 1024,
               ):
        # The order of elements in the stream is preserved, i.e.,
        # elements in the output stream corresponds to elements
        # in the input stream in the same order.

        # For streaming, "timeout" is not a concern.
        # The concern is overall throughput.

        data_in_pipe = IterQueue(backlog)
        results = FutureIterQueue(
            backlog, return_x=return_x, return_exceptions=return_exceptions)

        # TODO:
        # use async tasks in another thread to do the 'wait'; this would
        # continue to hold the timeout settings meaningful.
        # https://stackoverflow.com/a/65780581/6178706

        def _enqueue():
            enqueue = self._call_enqueue
            for x in data_stream:
                ff = Future()
                results.put(ff)
                try:
                    uid, fut = enqueue(x, enqueue_timeout=600)
                except Exception as e:
                    ff.set_result((x, e))
                else:
                    fut.data['x'] = x
                    data_in_pipe.put((uid, fut, ff))
            data_in_pipe.close()
            results.close()

        def _wait():
            wait_for_result = self._call_wait_for_result
            for uid, fut, ff in data_in_pipe:
                try:
                    y = wait_for_result(uid, fut, total_timeout=3600)
                except Exception as e:
                    y = e
                ff.set_result((fut.data['x'], y))

        t = self._thread_pool.submit(_enqueue)
        t.add_done_callback(self._thread_tasks_done_callback)
        self._thread_tasks[t] = None

        t = self._thread_pool.submit(_wait)
        t.add_done_callback(self._thread_tasks_done_callback)
        self._thread_tasks[t] = None

        return results

    def _thread_tasks_done_callback(self, t):
        try:
            del self._thread_tasks[t]
        except KeyError:
            warnings.warn(f"the Future object `{t}` was already removed")

    def _add_servlet(self,
                     servlet: Type[Servlet],
                     *,
                     q_in,
                     q_out,
                     cpus: list = None,
                     workers: int = None,
                     name: str = None,
                     **init_kwargs):
        # `servlet` is the class object, not instance.
        assert not self.started

        q_in_lock = self.MP_CLASS.Lock()
        self._q_in_lock.append(q_in_lock)
        q_err = self._q_err
        q_log = self._q_log

        cpus = self._resolve_cpus(cpus=cpus, workers=workers)

        name = name or servlet.__name__

        for cpu in cpus:
            # Pinned to the specified cpu core.
            sname = f"{name}-{','.join(map(str, cpu))}"
            logger.info('adding servlet <%s> at CPU %s', sname, cpu)

            self._servlets.append(
                self.MP_CLASS.Process(
                    target=servlet.run,
                    name=sname,
                    kwargs={
                        'q_in': q_in,
                        'q_out': q_out,
                        'q_err': q_err,
                        'cpus': cpu,
                        'q_in_lock': q_in_lock,
                        'q_log': q_log,
                        'should_stop': self._should_stop,
                        **init_kwargs,
                    },
                )
            )

    def _resolve_cpus(self, *, cpus: list = None, workers: int = None):
        n_cpus = psutil.cpu_count(logical=True)

        # Either `workers` or `cpus`, but not both,
        # can be specified.
        if workers:
            # Number of workers is specified.
            # Create this many processes, each pinned
            # to one core. One core will host mulitple
            # processes if `workers` exceeds the number
            # of cores.
            assert not cpus
            assert 0 < workers <= n_cpus * 4
            cpus = list(reversed(range(n_cpus))) * 4
            cpus = sorted(cpus[:workers])
            # List[int]
        elif cpus:
            assert isinstance(cpus, list)
            # Create as many processes as the length of `cpus`.
            # Each element of `cpus` specifies cpu pinning for
            # one process. `cpus` could contain repeat numbers,
            # meaning multiple processes can be pinned to the same
            # cpu.
            # This provides the ultimate flexibility, e.g.
            #    [[0, 1, 2], [0], [2, 3], [4, 5, 6], 4, None]
        else:
            # Create as many processes as there are cores,
            # one process pinned to one core.
            cpus = list(range(n_cpus))
            # List[int]

        for i, cpu in enumerate(cpus):
            if isinstance(cpu, int):
                cpu = [cpu]
                cpus[i] = cpu
            assert all(0 <= c < n_cpus for c in cpu)

        return cpus

    def _resolve_timeout(self, *,
                         enqueue_timeout: float = None,
                         total_timeout: float = None):
        '''
        `enqueue_timeout` is the max waittime for entering
        the queue of this service. Unpon entering `call`
        or `async_call`, if the data queue to workers of the service
        (see `self._input_queues`) is full, it will wait up to
        this long before timeout.

        `total_timeout` is the max total time spent in `call`
        or `async_call` before timeout. Upon entering `call`
        or `async_call`, the input is placed in a queue (timeout
        if that can't be done within `enqueue_timeout`), then
        we'll wait for the result. If result does not come within
        `total_timeout`, it will timeout. This waittime is counted
        starting from the beginning of `call` or `async_call`, not
        starting after the input has been placed in the queue.
        '''
        if enqueue_timeout is None:
            enqueue_timeout = self.TIMEOUT_ENQUEUE
        assert enqueue_timeout >= 0
        if total_timeout is None:
            total_timeout = max(self.TIMEOUT_TOTAL, enqueue_timeout * 10)
        assert total_timeout > 0
        if enqueue_timeout > total_timeout:
            enqueue_timeout = total_timeout
        return enqueue_timeout, total_timeout

    @abstractmethod
    def _input_queues(self):
        raise NotImplementedError

    @abstractmethod
    def _gather_output(self):
        # Subclass implementation of this function is responsible
        # for removing the finished entry from `self._uid_to_futures`.
        raise NotImplementedError

    def _gather_error(self):
        threading.current_thread().name = 'ErrorCollector'
        q_err = self._q_err
        futures = self._uid_to_futures
        should_stop = self._should_stop
        while self.started:
            try:
                uid, err = q_err.get(timeout=1)
            except queue.Empty:
                if should_stop.is_set():
                    break
            else:
                fut = futures.pop(uid, None)
                if fut is None:  # timed-out in `call` or `async_call`
                    logger.info(
                        'got error for an already-cancelled task: %r', err)
                    continue
                try:
                    # `err` is a RemoteException object.
                    fut.set_exception(err)
                except asyncio.InvalidStateError as e:
                    if fut.cancelled():
                        logger.warning('Future object is already cancelled')
                    else:
                        logger.exception(e)
                        raise

    def _enqueue_future(self, x):
        t0 = perf_counter()
        fut = Future()
        uid = id(fut)
        self._uid_to_futures[uid] = fut
        fut.data = {'t0': t0}
        return uid, fut, t0

    async def _async_call_enqueue(self, x, *, enqueue_timeout):
        uid, fut, t0 = self._enqueue_future(x)

        qs = self._input_queues()
        t1 = t0 + enqueue_timeout
        for iq, q in enumerate(qs):
            while True:
                try:
                    q.put_nowait((uid, x))
                    break
                except queue.Full:
                    timenow = perf_counter()
                    if timenow >= t1:
                        fut.cancel()
                        del self._uid_to_futures[uid]
                        raise EnqueueTimeout(timenow - t0, iq)
                    await asyncio.sleep(min(self.SLEEP_ENQUEUE, t1 - timenow))

        fut.data['t1'] = perf_counter()
        return uid, fut

    async def _async_call_wait_for_result(self, uid, fut, *, total_timeout):
        t0 = fut.data['t0']
        t2 = t0 + total_timeout
        while not fut.done():
            timenow = perf_counter()
            if timenow >= t2:
                fut.cancel()
                self._uid_to_futures.pop(uid, None)
                # `uid` could have been deleted by
                # `gather_results` during very subtle
                # timing coincidence.
                raise TotalTimeout(timenow - t0)
            await asyncio.sleep(min(self.SLEEP_DEQUEUE, t2 - timenow))
        # await asyncio.wait_for(fut, timeout=t0 + total_timeout - perf_counter())
        # NOTE: `asyncio.wait_for` seems to be blocking for the
        # `timeout` even after result is available.
        fut.data['t2'] = perf_counter()
        return fut.result()
        # This could raise RemoteException.

    def _call_enqueue(self, x, *, enqueue_timeout):
        uid, fut, t0 = self._enqueue_future(x)

        qs = self._input_queues()
        t1 = t0 + enqueue_timeout
        for iq, q in enumerate(qs):
            timenow = perf_counter()
            try:
                # `enqueue_timeout` is the combined time across input queues,
                # not time per queue.
                if timenow >= t1:
                    q.put_nowait((uid, x))
                else:
                    q.put((uid, x), timeout=t1 - timenow)
            except queue.Full:
                fut.cancel()
                del self._uid_to_futures[uid]
                raise EnqueueTimeout(perf_counter() - t0, iq)

        fut.data['t1'] = perf_counter()
        return uid, fut

    def _call_wait_for_result(self, uid, fut, *, total_timeout):
        t0 = fut.data['t0']
        t2 = t0 + total_timeout
        try:
            res = fut.result(timeout=t2 - perf_counter())
        except concurrent.futures.TimeoutError:
            fut.cancel()
            self._uid_to_futures.pop(uid, None)
            # `uid` could have been deleted by
            # `gather_results` during very subtle
            # timing coincidence.
            raise TotalTimeout(perf_counter() - t0)
        fut.data['t2'] = perf_counter()
        return res


class SequentialServer(MPServer):
    '''
    A sequence of operations performed in order,
    the previous op's result becoming the subsequent
    op's input.
    '''

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._q_in_out: List[Queue] = [
            self.MP_CLASS.Queue(self.max_queue_size)]

    def add_servlet(self, servlet: Type[Servlet], **kwargs):
        q_in = self._q_in_out[-1]
        q_out: Queue = self.MP_CLASS.Queue(self.max_queue_size)
        self._q_in_out.append(q_out)

        self._add_servlet(
            servlet,
            q_in=q_in,
            q_out=q_out,
            **kwargs,
        )

    @overrides
    def _gather_output(self):
        threading.current_thread().name = 'ResultCollector'
        q_out = self._q_in_out[-1]
        futures = self._uid_to_futures
        should_stop = self._should_stop

        while self.started:
            try:
                uid, y = q_out.get(timeout=1)
            except queue.Empty:
                if should_stop.is_set():
                    break
            else:
                fut = futures.pop(uid, None)
                if fut is None:
                    # timed-out in `async_call` or `call`.
                    continue
                try:
                    fut.set_result(y)
                except asyncio.InvalidStateError as e:
                    if fut.cancelled():
                        logger.warning('Future object is already cancelled')
                    else:
                        logger.exception(e)
                        raise

    @overrides
    def _input_queues(self):
        return self._q_in_out[:1]


class EnsembleServer(MPServer):
    '''
    A number of operations performed on the same input
    in parallel, the list of results gathered and combined
    to form a final result.
    '''

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._q_in: List[Queue] = []
        self._q_out: List[Queue] = []

    def add_servlet(self, servlet: Type[Servlet], **kwargs):
        q_in: Queue = self.MP_CLASS.Queue(self.max_queue_size)
        q_out: Queue = self.MP_CLASS.Queue(self.max_queue_size)
        self._q_in.append(q_in)
        self._q_out.append(q_out)

        self._add_servlet(
            servlet,
            q_in=q_in,
            q_out=q_out,
            **kwargs,
        )

    @abstractmethod
    def ensemble(self, x, results: list):
        '''
        Take results of all components and return a final result
        by combining the individual results in some way.
        It is assumed here that this function is quick and cheap.
        If this is not true, some optimization is needed.

        `results`: results of the servlets in the order they were
        added by `add_servlet`.
        '''
        raise NotImplementedError

    @overrides
    def _enqueue_future(self, x):
        uid, fut, t0 = super()._enqueue_future(x)
        fut.data.update(x=x, res=[None] * len(self._q_out), n=0)
        return uid, fut, t0

    @overrides
    def _gather_output(self):
        threading.current_thread().name = 'ResultCollector'
        futures = self._uid_to_futures
        n_results_needed = len(self._q_out)

        while self.started:
            for idx, q_out in enumerate(self._q_out):
                while not q_out.empty():
                    # Get all available results out of this queue.
                    # They are for different requests.
                    uid, y = q_out.get_nowait()
                    fut = futures.get(uid)
                    if fut is None:
                        # timed-out in `async_call` or `call`.
                        continue

                    extra = fut.data
                    extra['res'][idx] = y
                    extra['n'] += 1
                    if extra['n'] == n_results_needed:
                        # All results for this request have been collected.
                        del futures[uid]
                        try:
                            z = self.ensemble(extra['x'], extra['res'])
                            fut.set_result(z)
                        except asyncio.InvalidStateError as e:
                            if fut.cancelled():
                                logger.warning(
                                    'Future object is already cancelled')
                            else:
                                logger.exception(e)
                                raise
                        except Exception as e:
                            fut.set_exception(e)
                    # No sleep. Get results out of the queue as quickly as possible.
            time.sleep(self.SLEEP_DEQUEUE)

    @overrides
    def _input_queues(self):
        return self._q_in


class SimpleServlet(Servlet):
    def __init__(self, *, func, batch_size: int = None, **kwargs):
        super().__init__(batch_size=batch_size)
        self._kwargs = kwargs
        self._func = func

    def call(self, x):
        return self._func(x, **self._kwargs)


class SimpleServer(SequentialServer):
    '''
    One worker process per CPU core, each worker running
    the specified function.
    '''

    def __init__(self,
                 func: Callable, /, *,
                 max_queue_size: int = None,
                 cpus: list = None,
                 workers: int = None,
                 batch_size: int = 0,
                 **kwargs
                 ):
        '''
        `func`: a function that takes an input value,
            which will be the value provided in calls to the server,
            plus `kwargs`. This function must be defined on the module
            level (can't be defined within a function), and can't be
            a lambda.
        '''
        super().__init__(max_queue_size=max_queue_size)

        self.add_servlet(SimpleServlet,
                         cpus=cpus,
                         workers=workers,
                         batch_size=batch_size,
                         func=func,
                         **kwargs)


class MPSocketServer(SocketServer):
    def __init__(self, server: MPServer, **kwargs):
        super().__init__(**kwargs)
        self._server = server
        self._enqueue_timeout, self._total_timeout = server._resolve_timeout()

    def __repr__(self):
        return f'{self.__class__.__name__}({repr(self._server)})'

    def __str__(self):
        return self.__repr__()

    @overrides
    async def set_server_option(self, name: str, value):
        if name == 'timeout':
            self._enqueue_timeout, self._total_timeout = self._server._resolve_timeout(
                enqueue_timeout=value[0], total_timeout=value[1])
            return
        await super().set_server_option(name, value)

    @overrides
    async def before_startup(self):
        self._server.__enter__()

    @overrides
    async def after_shutdown(self):
        self._server.__exit__(None, None, None)

    @overrides
    async def handle_request(self, data, writer):
        y = await self._server.async_call(
            data, enqueue_timeout=self._enqueue_timeout,
            total_timeout=self._total_timeout)
        await write_record(writer, y, encoder=self._encoder)
