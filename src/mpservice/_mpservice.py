
'''
Although named after machine learning use cases,
this service utility is generic.
'''

import asyncio
import logging
import multiprocessing as mp
import queue
import time
from abc import ABCMeta, abstractmethod
from multiprocessing import synchronize
from typing import List, Type, Tuple, Sequence, Dict

import psutil  # type: ignore

from ._mperror import MpError

logger = logging.getLogger(__name__)


class Modelet(metaclass=ABCMeta):
    # Typically a subclass needs to enhance
    # `__init__` and implement `predict`.

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
        modelet = cls(**init_kwargs)
        modelet.start(q_in=q_in,
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
                    y = self.predict([x])[0]
                else:
                    y = self.predict(x)
                q_out.put((uid, y))

            except Exception as e:
                if not silent_errors or not isinstance(e, silent_errors):
                    logger.info(e)
                # There are opportunities to print traceback
                # and details later using the `MpError`
                # object. Be brief on the logging here.
                err = MpError(e)
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

                time0 = perf_counter()
                while n < batch_size:
                    try:
                        if batch_wait_time > 0:
                            time_left = batch_wait_time - \
                                (perf_counter() - time0)
                            if time_left > 0:
                                uid, x = q_in.get(timeout=time_left)
                            else:
                                uid, x = q_in.get_nowait()
                        else:
                            uid, x = q_in.get_nowait()
                        batch.append(x)
                        uids.append(x)
                        n += 1
                    except queue.Empty:
                        break

            batch_size_max = max(batch_size_max, n)
            batch_size_min = min(batch_size_min, n)
            batch_size_total += n
            n_batches += 1
            if n_batches % 1000 == 0:
                batch_size_mean = batch_size_total / n_batches
                logger.info('batch size stats (count, max, min, mean): %d, %d, %d, %.1f',
                            n_batches, batch_size_max, batch_size_min, batch_size_mean)

            try:
                results = self.predict(batch)
            except Exception as e:
                if not silent_errors or not isinstance(e, silent_errors):
                    logger.info(e)
                err = MpError(e)
                for uid in uids:
                    q_err.put((uid, err))
            else:
                for uid, y in zip(uids, results):
                    q_out.put((uid, y))

    def start(self, *, q_in, q_out, q_err, q_in_lock):
        logger.info('%s started', self.name)
        if self.batch_size > 1:
            self._start_batch(q_in=q_in, q_out=q_out, q_err=q_err,
                              q_in_lock=q_in_lock)
        else:
            self._start_single(q_in=q_in, q_out=q_out, q_err=q_err)

    @abstractmethod
    def predict(self, x):
        # `x`: a single element if `self.batch_size == 0`;
        # else, a list of elements.
        # When `batch_size == 0`, hence `x` is a single element,
        # return corresponding result.
        # When `batch_size > 0`, return list of results
        # corresponding to elements in `x`.
        raise NotImplementedError


class ModelService:
    MPCLASS = mp
    # This class attribute is provided because in some cases
    # on may want to use `torch.multiprocessing`, which is
    # a drop-in replacement for the standard `multiprocessing`
    # with some enhancements related to data sharing between
    # processes.

    def __init__(self,
                 max_queue_size: int = None,
                 cpus: Sequence[int] = None):
        self.max_queue_size = max_queue_size or 1024
        self._q_in_out: List[mp.Queue] = [
            self.MPCLASS.Queue(self.max_queue_size)]
        self._q_err: mp.Queue = self.MPCLASS.Queue(self.max_queue_size)
        self._q_in_lock: List[synchronize.Lock] = []

        self._uid_to_futures: Dict[int, asyncio.Future] = {}
        self._t_gather_results: asyncio.Task = None  # type: ignore
        self._modelets: List[mp.Process] = []

        self.loop = asyncio.get_running_loop()

        if cpus:
            psutil.Process().cpu_affinity(cpus=cpus)

        self._started = False

    def add_modelet(self,
                    modelet: Type[Modelet],
                    *,
                    cpus=None,
                    workers: int = None,
                    **init_kwargs):
        # `modelet` is the class object, not instance.
        assert not self._started
        q_in = self._q_in_out[-1]
        q_out: mp.Queue = self.MPCLASS.Queue(self.max_queue_size)
        self._q_in_out.append(q_out)
        q_in_lock = self.MPCLASS.Lock()
        self._q_in_lock.append(q_in_lock)

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

        n_cpus = psutil.cpu_count(logical=True)

        for cpu in cpus:
            if cpu is None:
                logger.info('adding modelet %s', modelet.__name__)
            else:
                if isinstance(cpu, int):
                    cpu = [cpu]
                assert all(0 <= c < n_cpus for c in cpu)
                logger.info('adding modelet %s at CPU %s',
                            modelet.__name__, cpu)

            self._modelets.append(
                self.MPCLASS.Process(
                    target=modelet.run,
                    name=f'modelet-{cpu}',
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
        assert self._modelets
        assert not self._started
        for m in self._modelets:
            m.start()
        self._t_gather_results = asyncio.create_task(self._gather_results())
        self._started = True

    def stop(self):
        if not self._started:
            return
        if self._t_gather_results is not None and not self._t_gather_results.done():
            self._t_gather_results.cancel()
            self._t_gather_results = None
        for m in self._modelets:
            if m.is_alive():
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
                uid, y = q_out.get()
                fut = futures.pop(uid)
                try:
                    fut.set_result(y)
                except asyncio.InvalidStateError:
                    if fut.cancelled():
                        logger.warning('Future object is already cancelled')
                # No sleep. Get results out of the queue as quickly as possible.

            while not q_err.empty():
                uid, err = q_err.get()
                fut = futures.pop(uid)
                try:
                    fut.set_exception(err)
                except asyncio.InvalidStateError:
                    if fut.cancelled():
                        logger.warning('Future object is already cancelled')
                # No sleep. Get results out of the queue as quickly as possible.

            await asyncio.sleep(0.0013)

    async def a_predict(self, x):
        fut = self.loop.create_future()
        uid = id(fut)
        self._uid_to_futures[uid] = fut
        q_in = self._q_in_out[0]

        # Exception handling is slow,
        # hence proactively check until
        # it's more likely to be un-empty.
        while True:
            while q_in.full():
                await asyncio.sleep(0.0012)
            try:
                q_in.put_nowait((uid, x))
                break
            except queue.Full:
                await asyncio.sleep(0.0013)
        return await fut

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *args_ignore, **kwargs_ignore):
        self.stop()
