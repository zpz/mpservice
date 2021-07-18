
import asyncio
import logging
import multiprocessing as mp
import queue
import time
from abc import ABCMeta, abstractmethod
from multiprocessing import synchronize
from typing import List, Type, Tuple, Sequence, Dict, Union

import psutil  # type: ignore

from ._mperror import MPError

logger = logging.getLogger(__name__)


class TimeoutError(Exception):
    pass


class EnqueueTimeout(TimeoutError):
    pass


class TotalTimeout(TimeoutError):
    pass


class Servlet(metaclass=ABCMeta):
    # Typically a subclass needs to enhance
    # `__init__` and implement `process`.

    @classmethod
    def run(cls, *,
            q_in: mp.Queue,
            q_out: mp.Queue,
            q_err: mp.Queue,
            q_in_lock: synchronize.Lock,
            cpus: Sequence[int] = None,
            **init_kwargs):
        if cpus:
            psutil.Process().cpu_affinity(cpus=cpus)
        obj = cls(**init_kwargs)
        obj.start(q_in=q_in,
                  q_out=q_out,
                  q_err=q_err,
                  q_in_lock=q_in_lock)

    def __init__(self, *,
                 batch_size: int = None,
                 batch_wait_time: float = None,
                 silent_errors: Tuple[Type[Exception], ...] = None):
        # `batch_wait_time`: seconds, may be 0.
        self.batch_size = batch_size or 0
        self.batch_wait_time = batch_wait_time or 0
        self.name = f'{self.__class__.__name__}--{mp.current_process().name}'
        self.silent_errors = silent_errors

    def _start_single(self, *, q_in, q_out, q_err):
        batch_size = self.batch_size
        silent_errors = self.silent_errors
        while True:
            uid, x = q_in.get()
            try:
                if batch_size:
                    y = self([x])[0]
                else:
                    y = self(x)
                q_out.put((uid, y))

            except Exception as e:
                if not silent_errors or not isinstance(e, silent_errors):
                    logger.info(e)
                # There are opportunities to print traceback
                # and details later using the `MPError`
                # object. Be brief on the logging here.
                err = MPError(e)
                q_err.put((uid, err))

    def _start_batch(self, *, q_in, q_out, q_err, q_in_lock):
        batch_size = self.batch_size
        batch_wait_time = self.batch_wait_time
        silent_errors = self.silent_errors
        perf_counter = time.perf_counter

        batch_size_max = -1
        batch_size_min = 1000000
        batch_size_total = 0
        n_batches = 0

        while True:
            batch = []
            uids = []
            n = 0
            with q_in_lock:
                uid, x = q_in.get()
                batch.append(x)
                uids.append(uid)
                n += 1

                wait_until = perf_counter() + batch_wait_time
                while n < batch_size:
                    time_left = wait_until - perf_counter()
                    try:
                        uid, x = q_in.get(timeout=time_left)
                    except queue.Empty:
                        break

                    batch.append(x)
                    uids.append(uid)
                    n += 1

            batch_size_max = max(batch_size_max, n)
            batch_size_min = min(batch_size_min, n)
            batch_size_total += n
            n_batches += 1
            if n_batches % 1000 == 0:
                batch_size_mean = batch_size_total / n_batches
                logger.info('batch size stats (count, max, min, mean): %d, %d, %d, %.1f',
                            n_batches, batch_size_max, batch_size_min, batch_size_mean)

            try:
                results = self(batch)
            except Exception as e:
                if not silent_errors or not isinstance(e, silent_errors):
                    logger.info(e)
                err = MPError(e)
                for uid in uids:
                    q_err.put((uid, err))
            else:
                for uid, y in zip(uids, results):
                    q_out.put((uid, y))

    def start(self, *, q_in, q_out, q_err, q_in_lock):
        q_err.put('ready')
        logger.info('%s started', self.name)
        if self.batch_size > 1:
            self._start_batch(q_in=q_in, q_out=q_out, q_err=q_err,
                              q_in_lock=q_in_lock)
        else:
            self._start_single(q_in=q_in, q_out=q_out, q_err=q_err)

    @abstractmethod
    def __call__(self, x):
        # `x`: a single element if `self.batch_size == 0`;
        # else, a list of elements.
        # When `batch_size == 0`, hence `x` is a single element,
        # return corresponding result.
        # When `batch_size > 0`, return list of results
        # corresponding to elements in `x`.
        raise NotImplementedError


class Server:
    MP_CLASS = mp
    # This class attribute is provided because in some cases
    # one may want to use `torch.multiprocessing`, which is
    # a drop-in replacement for the standard `multiprocessing`
    # with some enhancements related to data sharing between
    # processes.

    def __init__(self,
                 max_queue_size: int = None,
                 cpus: Sequence[int] = None):
        self.max_queue_size = max_queue_size or 1024
        self._q_in_out: List[mp.Queue] = [
            self.MP_CLASS.Queue(self.max_queue_size)]
        self._q_in_lock: List[synchronize.Lock] = []
        self._q_err: mp.Queue = self.MP_CLASS.Queue(self.max_queue_size)

        self._uid_to_futures: Dict[int, asyncio.Future] = {}
        self._t_gather_results: asyncio.Task = None  # type: ignore
        self._servlets: List[mp.Process] = []

        self.loop = asyncio.get_running_loop()

        if cpus:
            psutil.Process().cpu_affinity(cpus=cpus)

        self._started = False

    def add_servlet(self,
                    servlet: Type[Servlet],
                    *,
                    cpus=None,
                    workers: int = None,
                    **init_kwargs):
        # `servlet` is the class object, not instance.
        assert not self._started
        q_in = self._q_in_out[-1]
        q_in_lock = self.MP_CLASS.Lock()
        self._q_in_lock.append(q_in_lock)

        q_out: mp.Queue = self.MP_CLASS.Queue(self.max_queue_size)
        self._q_in_out.append(q_out)

        n_cpus = psutil.cpu_count(logical=True)

        if workers:
            # Number of workers is specified.
            # `cpus` specifies the cores for each worker;
            # can be `None` or `List[int]`.
            assert workers > 0
            cpus = [cpus for _ in range(workers)]
        else:
            if cpus is None:
                # Create one worker, not pinned to any core.
                cpus = [None]
            else:
                assert isinstance(cpus, list)
                # Create as many processes as the length of `cpus`.
                # Each element of `cpus` specifies cpu pinning for
                # one process. `cpus` could contain repeat numbers,
                # meaning multiple processes can be pinned to the same
                # cpu.
                # This provides the ultimate flexibility, e.g.
                #    [[0, 1, 2], [0], [2, 3], [4, 5, 6], None]

        for cpu in cpus:
            if cpu is None:
                logger.info('adding servlet %s', servlet.__name__)
            else:
                if isinstance(cpu, int):
                    cpu = [cpu]
                assert all(0 <= c < n_cpus for c in cpu)
                logger.info('adding servlet %s at CPU %s',
                            servlet.__name__, cpu)

            self._servlets.append(
                self.MP_CLASS.Process(
                    target=servlet.run,
                    name=f'servlet-{cpu}',
                    kwargs={
                        'q_in': q_in,
                        'q_out': q_out,
                        'q_err': self._q_err,
                        'cpus': cpu,
                        'q_in_lock': q_in_lock,
                        **init_kwargs,
                    },
                )
            )

    def start(self):
        assert self._servlets
        assert not self._started
        n = 0
        for m in self._servlets:
            m.start()
            n += 1
        k = 0
        while k < n:
            z = self._q_err.get()
            assert z == 'ready'
            k += 1
            print(f"servlet processes ready: {k}/{n}")

        self._t_gather_results = asyncio.create_task(self._gather_results())
        self._started = True

    def stop(self):
        if not self._started:
            return
        if self._t_gather_results is not None and not self._t_gather_results.done():
            self._t_gather_results.cancel()
            # self._t_gather_results = None
        for m in self._servlets:
            # if m.is_alive():
            m.terminate()
            m.join()
        self._started = False

        # Reset CPU affinity.
        psutil.Process().cpu_affinity(cpus=[])

    def __del__(self):
        self.stop()

    async def _gather_results(self):
        q_out = self._q_in_out[-1]
        q_err = self._q_err
        futures = self._uid_to_futures
        while True:
            while not q_out.empty():
                uid, y = q_out.get_nowait()
                fut = futures.pop(uid, None)
                if fut is None:  # timed-out in `a_predic`.
                    continue
                try:
                    fut.set_result(y)
                except asyncio.InvalidStateError:
                    if fut.cancelled():
                        logger.warning('Future object is already cancelled')
                # No sleep. Get results out of the queue as quickly as possible.

            while not q_err.empty():
                uid, err = q_err.get_nowait()
                fut = futures.pop(uid, None)
                if fut is None:
                    logger.error(
                        'got error for an already-cancelled task: %r', err)
                    continue  # timed-out in `__call__`.
                try:
                    fut.set_exception(err)
                except asyncio.InvalidStateError:
                    if fut.cancelled():
                        logger.error(
                            'got error for an already-cancelled task: %r', err)
                # No sleep. Get results out of the queue as quickly as possible.

            await asyncio.sleep(0.0013)

    async def __call__(self,
                       x,
                       *,
                       enqueue_timeout: Union[int, float] = None,
                       total_timeout: Union[int, float] = None,
                       ):
        if enqueue_timeout is None:
            enqueue_timeout = 10
        elif enqueue_timeout < 0:
            enqueue_timeout = 0
        if total_timeout is None:
            total_timeout = max(100, enqueue_timeout * 10)
        else:
            assert total_timeout > 0, "total_timeout must be > 0"
        if enqueue_timeout > total_timeout:
            enqueue_timeout = total_timeout

        loop = self.loop
        fut = loop.create_future()
        uid = id(fut)
        self._uid_to_futures[uid] = fut
        q_in = self._q_in_out[0]

        time0 = loop.time()
        time1 = time0 + enqueue_timeout
        time2 = time0 + total_timeout

        while True:
            try:
                q_in.put_nowait((uid, x))
            except queue.Full:
                timenow = loop.time()
                if timenow < time1:
                    await asyncio.sleep(0.00089)
                else:
                    fut.cancel()
                    del self._uid_to_futures[uid]
                    raise EnqueueTimeout(f'waited {timenow - time0} seconds')
            else:
                break

        try:
            await asyncio.wait_for(fut, timeout=time2 - loop.time())
        except asyncio.TimeoutError:
            # `fut` is now cancelled.
            if uid in self._uid_to_futures:
                # `uid` could have been deleted by
                # `_gather_results` during very subtle
                # timing coincidence.
                del self._uid_to_futures[uid]
            raise TotalTimeout(f'waited {loop.time() - time0} seconds')
        else:
            return fut.result()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.stop()
