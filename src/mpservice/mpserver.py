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
import multiprocessing as mp
import queue
import threading
import time
from abc import ABCMeta, abstractmethod
from multiprocessing import synchronize
from typing import List, Type, Sequence, Union, Callable
from uuid import uuid4

import psutil  # type: ignore

from .exception import RemoteException
from . import streamer


logger = logging.getLogger(__name__)


class TimeoutError(Exception):
    pass


class EnqueueTimeout(TimeoutError):
    pass


class TotalTimeout(TimeoutError):
    pass


class Servlet(metaclass=ABCMeta):
    # Typically a subclass needs to enhance
    # `__init__` and implement `call`.

    @classmethod
    def run(cls, *,
            q_in: mp.Queue,
            q_out: mp.Queue,
            q_err: mp.Queue,
            q_in_lock: synchronize.Lock,
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
        if cpus:
            psutil.Process().cpu_affinity(cpus=cpus)
        obj = cls(**init_kwargs)
        obj.start(q_in=q_in,
                  q_out=q_out,
                  q_err=q_err,
                  q_in_lock=q_in_lock)

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
        simple fix `batch_size` to 0 in their `__init__`.

        The `__init__` of a subclass may define additional input parameters;
        they can be passed in through `run`.
        '''
        self.batch_size = batch_size or 0
        self.batch_wait_time = batch_wait_time or 0
        self.name = f'{self.__class__.__name__}--{mp.current_process().name}'

    def start(self, *, q_in, q_out, q_err, q_in_lock):
        q_err.put('ready')
        logger.info('%s started', self.name)
        if self.batch_size > 1:
            self._start_batch(q_in=q_in, q_out=q_out, q_err=q_err,
                              q_in_lock=q_in_lock)
        else:
            self._start_single(q_in=q_in, q_out=q_out, q_err=q_err)

    def _start_single(self, *, q_in, q_out, q_err):
        batch_size = self.batch_size
        while True:
            uid, x = q_in.get()
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

    def _start_batch(self, *, q_in, q_out, q_err, q_in_lock):
        batch_size = self.batch_size
        batch_wait_time = self.batch_wait_time
        perf_counter = time.perf_counter

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
                uid, x = q_in.get()
                batch.append(x)
                uids.append(uid)
                n += 1

                wait_until = perf_counter() + batch_wait_time
                # Wait time starts after getting the first item,
                # because however long the first item takes to come
                # is not the worker's fault.

                while n < batch_size:
                    time_left = wait_until - perf_counter()
                    try:
                        # If there is remaining time, wait up to
                        # that long.
                        # Otherwise, get the next item if it is there
                        # right now (i.e. no waiting) even if we
                        # are already over time. That is, if supply
                        # has piled up, then will get up to the
                        # batch capacity.
                        if time_left > 0:
                            # Or should we do `if time_left > 0.001`?
                            uid, x = q_in.get(timeout=max(0.001, time_left))
                        else:
                            uid, x = q_in.get_nowait()
                    except queue.Empty:
                        break

                    batch.append(x)
                    uids.append(uid)
                    n += 1

            n_batches += 1
            batch_size_max = max(batch_size_max, n)
            batch_size_min = min(batch_size_min, n)
            batch_size_mean = (batch_size_mean *
                               (n_batches - 1) + n) / n_batches
            if n_batches % 1000 == 0:
                logger.info('batch size stats (count, max, min, mean): %d, %d, %d, %.1f',
                            n_batches, batch_size_max, batch_size_min, batch_size_mean)

            try:
                results = self.call(batch)
            except Exception:
                err = RemoteException()
                for uid in uids:
                    q_err.put((uid, err))
            else:
                for uid, y in zip(uids, results):
                    q_out.put((uid, y))

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
        raise NotImplementedError


class MPServer(metaclass=ABCMeta):
    MP_CLASS = mp
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
        self._q_err: mp.Queue = self.MP_CLASS.Queue(self.max_queue_size)

        self._t_gather_results: threading.Thread = None  # type: ignore

        self._servlets: List[mp.Process] = []
        self._uid_to_futures = {}

        self.started = 0
        if cpus:
            # Pin this coordinating thread to the specified CPUs.
            psutil.Process().cpu_affinity(cpus=cpus)

    def __del__(self):
        if self.started:
            self.stop()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.stop()

    def start(self, sequential: bool = True):
        assert self._servlets
        self.started += 1
        if self.started > 1:
            # Re-entry.
            return

        if sequential:
            # Start the servlets one by one.
            n = len(self._servlets)
            k = 0
            for m in self._servlets:
                m.start()
                z = self._q_err.get()
                assert z == 'ready'
                k += 1
                logger.info(f"servlet processes ready: {k}/{n}")
        else:
            # Start the servlets concurrently.
            # In some situations, concurrent servlet launch
            # may have issues, e.g. all servlets read the same
            # large file on startup, or all servlets download
            # the same dataset on startup.
            n = 0
            for m in self._servlets:
                m.start()
                n += 1
            k = 0
            while k < n:
                z = self._q_err.get()
                assert z == 'ready'
                k += 1
                logger.info(f"servlet processes ready: {k}/{n}")

        self._t_gather_results = threading.Thread(target=self._gather_results)
        self._t_gather_results.start()

    def stop(self):
        assert self.started > 0
        self.started -= 1
        if self.started > 0:
            # Exiting one nested level.
            return

        self._t_gather_results.join()
        self._t_gather_results = None

        for m in self._servlets:
            # if m.is_alive():
            m.terminate()
            m.join()

        # Reset CPU affinity.
        psutil.Process().cpu_affinity(cpus=[])

    async def async_call(self,
                         x,
                         *,
                         enqueue_timeout: Union[int, float] = None,
                         total_timeout: Union[int, float] = None,
                         ):
        enqueue_timeout, total_timeout = self._resolve_timeout(
            enqueue_timeout=enqueue_timeout, total_timeout=total_timeout,
        )
        time0 = time.perf_counter()
        uid, fut = await self._async_call_enqueue(
            x, qs=self._input_queues(),
            t0=time0, enqueue_timeout=enqueue_timeout)
        return await self._async_call_wait_for_result(
            uid=uid, fut=fut,
            t0=time0, total_timeout=total_timeout)

    def call(self,
             x,
             *,
             enqueue_timeout: Union[int, float] = None,
             total_timeout: Union[int, float] = None,
             ):
        '''
        Sometimes a subclass may want to override this method
        to add preprocessing of the input and postprocessing of
        the output, while calling `super().call(...)` in the middle.

        However, be careful to maintain consistency between the
        `call`/`async_call` methods and the `stream`/`async_stream` methods.
        '''
        enqueue_timeout, total_timeout = self._resolve_timeout(
            enqueue_timeout=enqueue_timeout, total_timeout=total_timeout,
        )

        time0 = time.perf_counter()
        uid, fut = self._call_enqueue(
            x, qs=self._input_queues(),
            t0=time0, enqueue_timeout=enqueue_timeout)
        return self._call_wait_for_result(
            uid=uid, fut=fut, t0=time0, total_timeout=total_timeout)

    def async_stream(self, data_stream, *,
                     return_exceptions: bool = False,
                     return_x: bool = False,
                     ) -> streamer.AsyncStream:
        # The order of elements in the stream is preserved, i.e.,
        # elements in the output stream corresponds to elements
        # in the input stream in the same order.

        # What this method does can be achieved by a streamer.AsyncStream
        # using `self.async_call` as a "transformer".
        # However, this method is expected to achieve optimal
        # performance, whereas the efficiency achieved by
        # "transfomer" depends on tweaking the "workers" parameter.

        enqueue_timeout, total_timeout = self._resolve_timeout()

        async def _enqueue(x, return_x):
            time0 = time.perf_counter()
            uid, fut = await self._async_call_enqueue(
                x, qs=self._input_queues(),
                t0=time0, enqueue_timeout=enqueue_timeout)
            if return_x:
                return x, fut, uid
            else:
                return fut, uid

        async def _dequeue(x, return_x):
            if return_x:
                x, fut, uid = x
            else:
                fut, uid = x
            time0 = time.perf_counter()
            z = await self._async_call_wait_for_result(
                uid=uid, fut=fut,
                t0=time0, total_timeout=total_timeout)
            if return_x:
                return x, z
            return z

        if not isinstance(data_stream, streamer.AsyncStream):
            data_stream = streamer.AsyncStream(data_stream)

        return (
            data_stream
            .transform(
                _enqueue, workers=1, return_x=return_x,
                return_exceptions=return_exceptions)
            .transform(
                _dequeue, workers=1, return_x=return_x,
                return_exceptions=return_exceptions)
        )

    def stream(self, data_stream, *,
               return_exceptions: bool = False,
               return_x: bool = False,
               ) -> streamer.Stream:
        # The order of elements in the stream is preserved, i.e.,
        # elements in the output stream corresponds to elements
        # in the input stream in the same order.

        # What this method does can be achieved by a streamer.Stream
        # using `self.call` as a "transformer".
        # However, this method is expected to achieve optimal
        # performance, whereas the efficiency achieved by
        # "transfomer" depends on tweaking the "workers" parameter.

        enqueue_timeout, total_timeout = self._resolve_timeout()

        def _enqueue(x, return_x):
            fut = concurrent.futures.Future()
            time0 = time.perf_counter()
            uid, fut = self._call_enqueue(
                x, qs=self._input_queues(),
                t0=time0, enqueue_timeout=enqueue_timeout)
            if return_x:
                return x, uid, fut
            else:
                return uid, fut

        def _dequeue(x, return_x):
            if return_x:
                x, uid, fut = x
            else:
                uid, fut = x
            time0 = time.perf_counter()
            z = self._call_wait_for_result(
                uid=uid, fut=fut,
                t0=time0, total_timeout=total_timeout)
            z = fut.result()
            if return_x:
                return x, z
            return z

        if not isinstance(data_stream, streamer.Stream):
            data_stream = streamer.Stream(data_stream)

        return (data_stream
                .transform(
                    _enqueue, workers=1, return_x=return_x,
                    return_exceptions=return_exceptions)
                .transform(
                    _dequeue, workers=1, return_x=return_x,
                    return_exceptions=return_exceptions)
                )

    def _add_servlet(self,
                     servlet: Type[Servlet],
                     *,
                     q_in,
                     q_out,
                     cpus: list = None,
                     workers: int = None,
                     **init_kwargs):
        # `servlet` is the class object, not instance.
        assert not self.started

        q_in_lock = self.MP_CLASS.Lock()
        self._q_in_lock.append(q_in_lock)
        q_err = self._q_err

        cpus = self._resolve_cpus(cpus=cpus, workers=workers)

        for cpu in cpus:
            if cpu is None:
                # Not pinned to any core.
                logger.info('adding servlet %s', servlet.__name__)
            else:
                # Pinned to the specified cpu core.
                logger.info('adding servlet %s at CPU %s',
                            servlet.__name__, cpu)

            self._servlets.append(
                self.MP_CLASS.Process(
                    target=servlet.run,
                    name=f'servlet-{cpu}',
                    kwargs={
                        'q_in': q_in,
                        'q_out': q_out,
                        'q_err': q_err,
                        'cpus': cpu,
                        'q_in_lock': q_in_lock,
                        **init_kwargs,
                    },
                )
            )

    def _resolve_cpus(self, *, cpus: list = None, workers: int = None):
        n_cpus = psutil.cpu_count(logical=True)

        # Either `workers` or `cpus`, but not both.
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
        if that can't be doen within `enqueue_timeout`), then
        we'll wait for the result. If result does not come within
        `total_timeout`, it will timeout. This waittime is counted
        starting from the beginning of `call` or `async_call`, not
        starting after the input has been placed in the queue.
        '''
        if enqueue_timeout is None:
            enqueue_timeout = self.TIMEOUT_ENQUEUE
        elif enqueue_timeout < 0:
            enqueue_timeout = 0
        if total_timeout is None:
            total_timeout = max(self.TIMEOUT_TOTAL, enqueue_timeout * 10)
        else:
            assert total_timeout > 0, "total_timeout must be > 0"
        if enqueue_timeout > total_timeout:
            enqueue_timeout = total_timeout
        return enqueue_timeout, total_timeout

    @abstractmethod
    def _init_future_struct(self, x, fut):
        raise NotImplementedError

    def _enqueue_future(self, x, fut):
        z = self._init_future_struct(x, fut)
        while True:
            uid = str(uuid4())
            if self._uid_to_futures.setdefault(uid, z) is z:
                return uid

    @abstractmethod
    def _input_queues(self):
        raise NotImplementedError

    def _gather_results(self):
        # This is not a "public" method in that end-user
        # should not call this method. But this is an important
        # top-level method.
        while True:
            if not self.started:
                return
            self._gather_output()
            self._gather_error()
            time.sleep(self.SLEEP_DEQUEUE)

    @abstractmethod
    def _gather_output(self):
        # Subclass implementation of this function is responsible
        # for removing the finished entry from `self._uid_to_futures`.
        raise NotImplementedError

    def _gather_error(self):
        q_err = self._q_err
        futures = self._uid_to_futures
        while not q_err.empty():
            uid, err = q_err.get_nowait()
            fut = futures.pop(uid, None)
            if fut is None:  # timed-out in `call` or `async_call`
                logger.info(
                    'got error for an already-cancelled task: %r', err)
                continue
            try:
                # `err` is a RemoteException object.
                fut['fut'].set_exception(err)
            except asyncio.InvalidStateError as e:
                if fut['fut'].cancelled():
                    logger.warning('Future object is already cancelled')
                else:
                    logger.exception(e)
                    raise
            # No sleep. Get results out of the queue as quickly as possible.

    async def _async_call_enqueue(self, x, *, qs, t0, enqueue_timeout):
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        uid = self._enqueue_future(x, fut)

        t1 = t0 + enqueue_timeout
        for iq, q in enumerate(qs):
            while True:
                if not self.started:
                    return
                try:
                    q.put_nowait((uid, x))
                    break
                except queue.Full:
                    timenow = time.perf_counter()
                    if timenow < t1:
                        await asyncio.sleep(self.SLEEP_ENQUEUE)
                    else:
                        fut.cancel()
                        del self._uid_to_futures[uid]
                        raise EnqueueTimeout(
                            f'waited {timenow - t0} seconds; timeout at queue {iq}')
        return uid, fut

    async def _async_call_wait_for_result(self, *, uid, fut, t0, total_timeout):
        t1 = t0 + total_timeout
        while not fut.done():
            if time.perf_counter() > t1:
                fut.cancel()
                self._uid_to_futures.pop(uid, None)
                # `uid` could have been deleted by
                # `gather_results` during very subtle
                # timing coincidence.
                raise TotalTimeout(
                    f'waited {time.perf_counter() - t0} seconds')
            await asyncio.sleep(self.SLEEP_DEQUEUE)
        # await asyncio.wait_for(fut, timeout=t0 + total_timeout - time.perf_counter())
        # NOTE: `asyncio.wait_for` seems to be blocking for the
        # `timeout` even after result is available.
        return fut.result()
        # This could raise RemoteException.

    def _call_enqueue(self, x, *, qs, t0, enqueue_timeout):
        fut = concurrent.futures.Future()
        uid = self._enqueue_future(x, fut)

        t1 = t0 + enqueue_timeout
        for iq, q in enumerate(qs):
            while True:
                if not self.started:
                    return
                try:
                    q.put_nowait((uid, x))
                    break
                except queue.Full:
                    timenow = time.perf_counter()
                    if timenow < t1:
                        time.sleep(self.SLEEP_ENQUEUE)
                    else:
                        fut.cancel()
                        del self._uid_to_futures[uid]
                        raise EnqueueTimeout(
                            f'waited {timenow - t0} seconds; timeout at queue {iq}')
        return uid, fut

    def _call_wait_for_result(self, *, uid, fut, t0, total_timeout):
        t1 = t0 + total_timeout
        while not fut.done():
            if time.perf_counter() > t1:
                fut.cancel()
                self._uid_to_futures.pop(uid, None)
                # `uid` could have been deleted by
                # `gather_results` during very subtle
                # timing coincidence.
                raise TotalTimeout(
                    f'waited {time.perf_counter() - t0} seconds')
            time.sleep(self.SLEEP_DEQUEUE)
        return fut.result()


class SequentialServer(MPServer):
    '''
    A sequence of operations performed in order,
    the previous op's result becoming the subsequent
    op's input.
    '''

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._q_in_out: List[mp.Queue] = [
            self.MP_CLASS.Queue(self.max_queue_size)]

    def add_servlet(self, servlet: Type[Servlet], **kwargs):
        q_in = self._q_in_out[-1]
        q_out: mp.Queue = self.MP_CLASS.Queue(self.max_queue_size)
        self._q_in_out.append(q_out)

        self._add_servlet(
            servlet,
            q_in=q_in,
            q_out=q_out,
            **kwargs,
        )

    def _gather_output(self):
        q_out = self._q_in_out[-1]
        futures = self._uid_to_futures

        while not q_out.empty():
            uid, y = q_out.get_nowait()
            fut = futures.pop(uid, None)
            if fut is None:
                # timed-out in `async_call` or `call`.
                continue
            try:
                fut['fut'].set_result(y)
            except asyncio.InvalidStateError as e:
                if fut.cancelled():
                    logger.warning('Future object is already cancelled')
                else:
                    logger.exception(e)
                    raise
            # No sleep. Get results out of the queue as quickly as possible.

    def _init_future_struct(self, x, fut):
        return {'fut': fut}

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
        self._q_in: List[mp.Queue] = []
        self._q_out: List[mp.Queue] = []

    def add_servlet(self, servlet: Type[Servlet], **kwargs):
        q_in: mp.Queue = self.MP_CLASS.Queue(self.max_queue_size)
        q_out: mp.Queue = self.MP_CLASS.Queue(self.max_queue_size)
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

    def _gather_output(self):
        futures = self._uid_to_futures
        n_results_needed = len(self._q_out)

        for idx, q_out in enumerate(self._q_out):
            while not q_out.empty():
                uid, y = q_out.get_nowait()
                fut = futures.get(uid)
                if fut is None:
                    # timed-out in `async_call` or `call`.
                    continue

                fut['res'][idx] = y
                fut['n'] += 1
                if fut['n'] == n_results_needed:
                    del futures[uid]
                    try:
                        z = self.ensemble(fut['x'], fut['res'])
                        fut['fut'].set_result(z)
                    except asyncio.InvalidStateError as e:
                        if fut['fut'].cancelled():
                            logger.warning(
                                'Future object is already cancelled')
                        else:
                            logger.exception(e)
                            raise
                    except Exception as e:
                        fut['fut'].set_exception(e)
                # No sleep. Get results out of the queue as quickly as possible.

    def _init_future_struct(self, x, fut):
        return {
            'x': x,
            'fut': fut,
            'res': [None] * len(self._q_out),
            'n': 0
        }

    def _input_queues(self):
        return self._q_in


class SimpleServer(SequentialServer):
    '''
    One worker process per CPU core, each worker running
    the specified function.
    '''

    def __init__(self,
                 func: Callable,
                 *,
                 max_queue_size: int = None,
                 cpus: list = None,
                 workers: int = None,
                 batch_size: int = 0,
                 **kwargs
                 ):
        '''
        `func`: a function that takes an input value,
        which will be the value provided in calls to the server,
        plus `kwargs`.
        '''
        super().__init__(max_queue_size=max_queue_size)

        class SimpleServlet(Servlet):
            def __init__(self, *, batch_size: int = None, **kwargs):
                super().__init__(batch_size=batch_size)
                self._kwargs = kwargs

            def call(self, x):
                return func(x, **self._kwargs)

        self.add_servlet(SimpleServlet,
                         cpus=cpus,
                         workers=workers,
                         batch_size=batch_size,
                         **kwargs)
