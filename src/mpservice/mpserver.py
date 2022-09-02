'''Service using multiprocessing to perform CPU-bound operations
making use of multiple CPUs (i.e. cores) on the machine.

There are 4 levels of constructs.

- On the lowest level is `Worker`. This defines operation on a single input item
  or a batch of items in usual sync code. This is supposed to run in a separate process
  and uses that single process only. (If the code futher creates other processes,
  well, no one can forbid that, but that is not the intended usage.)

  There are two worker classes: `ProcessWorker` and `ThreadWorker`.

- A `Servlet` manages a `Worker`, specifing how many processes are to be created,
  each executing the worker code independently. It may control exactly
  which CPU(s) each worker process uses. The servlet code runs in the "main process",
  whereas workers run in other processes. Each input item is processed by one
  and only one worker.

  There are two servelet classess: `ProcessServlet` and `ThreadServlet`.

  If `W1` is a subclass of `ProcessWorker`, then the simplest servlet
  is like this:

    s = ProcessServlet(W1)

  This creates one worker process that is not pinned to a particular CPU.

- Servlets can be composed in `Sequential`s or `Ensemble`s. In a `Sequential`,
  the first servlet's output becomes the second servlet's input, and so on.
  In an `Ensemble`, each input item is processed by all the servlets, and their
  results are collected in a list.

  Great power comes from the fact that both `Sequential` and `Ensemble`
  also follow the `Servlet` API, hence both can be constituents of other compositions.
  In principle, you can freely compose and nest them.
  For example, suppose `W1`, `W2`,..., are `Worker` subclasses,
  then you may design such a workflow,

    s = Sequential(
            ProcessServlet(W1),
            Ensemble(
                ThreadServlet(W2),
                Sequential(ProcessServlet(W3), ThreadServlet(W4)),
                ),
            Ensemble(
                Sequetial(ProcessServlet(W5), ProcessServlet(W6)),
                Sequetial(ProcessServlet(W7), ThreadServlet(W8), ProcessServlet(W9)),
                ),
        )

  In sum, `ProcessServlet`, `ThreadServlet`, `Sequential`, `Ensemble` are collectively
  referred to and used as `Servlet`.

- On the top level is `Server`. Pass a `Servlet`, or `Sequential` or `Ensemble`
  into a `Server`, which handles scheduling as well as interfacing with the outside
  world:

    server = Server(s)
    with server:
        y = server.call(38)
        z = await server.async_call('abc')

        for x, y in server.stream(data, return_x=True):
            print(x, y)

The "interface" and "scheduling" code of `Server` runs in the "main process".
Two usage patterns are supported, namely making (concurrent) individual
calls to the service to get individual results, or flowing
a potentially unlimited stream of data through the service
to get a stream of results. The first usage supports a sync API and an async API.

The user's main work is implementing the operations in the "workers".
Another task (of some trial and error) by the user is experimenting with
CPU allocations among workers to achieve best performance.

Reference: [Service Batching from Scratch, Again](https://zpz.github.io/blog/batched-service-redesign/).
This article describes roughly version 0.7.2.
'''
from __future__ import annotations
import asyncio
import concurrent.futures
import logging
import multiprocessing
import multiprocessing.queues
import queue
import threading
from abc import ABCMeta, abstractmethod
from datetime import datetime
from queue import Empty
from time import perf_counter, sleep
from typing import Sequence, Union, Callable, Type, Any

import psutil

from .remote_exception import RemoteException, exit_err_msg
from .util import is_exception, Thread, TimeoutError, ProcessLogger
from ._queues import SingleLane


# Set level for logs produced by the standard `multiprocessing` module.
multiprocessing.log_to_stderr(logging.WARNING)

logger = logging.getLogger(__name__)

NOMOREDATA = b'c7160a52-f8ed-40e4-8a38-ec6b84c2cd87'


class FastQueue(multiprocessing.queues.SimpleQueue):
    # Check out os.read, os.write, os.close with file-descriptor args.
    def __init__(self, *, ctx):
        super().__init__(ctx=ctx)
        # Replace Lock by RLock to facilitate batching via greedy `get_many`.
        self._rlock = ctx.RLock()


class SimpleQueue(queue.SimpleQueue):
    def __init__(self):
        super().__init__()
        self._rlock = threading.RLock()


class Worker(metaclass=ABCMeta):
    # Typically a subclass needs to enhance
    # `__init__` and implement `call`.

    @classmethod
    def run(cls, *,
            q_in: Union[FastQueue, SimpleQueue],
            q_out: Union[FastQueue, SimpleQueue],
            **init_kwargs):
        # For a `ProcessWorker`, `q_in` and `q_out` should be `FastQueue`.
        # For a `ThreadWorker`, `q_in` and `q_out` are either `FastQueue` or `SimpleQueue`.
        obj = cls(**init_kwargs)
        q_out.put(obj.name)
        # This sends a signal to caller indicating completion of init.
        obj.start(q_in=q_in, q_out=q_out)

    def __init__(self, *,
                 batch_size: int = None,
                 batch_wait_time: float = None,
                 batch_size_log_cadence: int = 1_000_000):
        '''
        `batch_size`: max batch size; see `call`.

        `batch_wait_time`: seconds, may be 0; the total duration
            to wait for one batch after the first item has arrived.

        `batch_size_log_cadence`: log batch size statistics every this
            many batches. If `None`, do not log.

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
        self.batch_size_log_cadence = batch_size_log_cadence
        self.batch_wait_time = batch_wait_time
        self.name = multiprocessing.current_process().name

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
        # the uer in `Servlet` to specify a zero or nonzero `batch_size`
        # for this worker.
        #
        # In case of exceptions, unless the user has specific things to do,
        # do not handle them; just let them happen. They will be handled
        # in `_start_batch` and `_start_single`.
        raise NotImplementedError

    def start(self, *, q_in, q_out):
        try:
            if self.batch_size > 1:
                self._start_batch(q_in=q_in, q_out=q_out)
            else:
                self._start_single(q_in=q_in, q_out=q_out)
        except KeyboardInterrupt:
            print(self.name, 'stopped by KeyboardInterrupt')
            # The process will exit. Don't print the usual
            # exception stuff as that's not need when user
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
            if is_exception(x):
                q_out.put((uid, x))
                continue

            try:
                if batch_size:
                    y = self.call([x])[0]
                else:
                    y = self.call(x)
            except Exception:
                # There are opportunities to print traceback
                # and details later using the `RemoteException`
                # object. Be brief on the logging here.
                y = RemoteException()

            q_out.put((uid, y))

    def _start_batch(self, *, q_in, q_out):
        def print_batching_info():
            logger.info('%d batches with sizes %d--%d, mean %.1f',
                        n_batches, batch_size_min, batch_size_max, batch_size_mean)

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

                uids = [v[0] for v in batch]
                batch = [v[1] for v in batch]
                n = len(batch)

                try:
                    results = self.call(batch)
                except Exception:
                    err = RemoteException()
                    for uid in uids:
                        q_out.put((uid, err))
                else:
                    for z in zip(uids, results):
                        q_out.put(z)

                if batch_size_log_cadence:
                    n_batches += 1
                    batch_size_max = max(batch_size_max, n)
                    batch_size_min = min(batch_size_min, n)
                    batch_size_mean = (batch_size_mean * (n_batches - 1) + n) / n_batches
                    if n_batches >= batch_size_log_cadence:
                        print_batching_info()
                        n_batches = 0
        finally:
            if batch_size_log_cadence and n_batches:
                print_batching_info()
            collector_thread.join()

    def _build_input_batches(self, q_in, q_out):
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
                            return
                        # Now `z` is a tuple like (uid, x).
                        if is_exception(z[1]):
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
        extra_timeout = self.batch_wait_time
        batchsize = self.batch_size
        buffer = self._batch_buffer
        out = buffer.get()
        if out == NOMOREDATA:
            return out
        out = [out]
        n = 1

        deadline = perf_counter() + extra_timeout
        # Timeout starts after the first item is secured.

        while n < batchsize:
            t = deadline - perf_counter()
            try:
                z = buffer.get(timeout=max(0, t))
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


class ProcessWorker(Worker):
    @classmethod
    def run(cls, *,
            log_passer: ProcessLogger,
            cpus: Sequence[int] = None,
            **kwargs):
        '''
        This classmethod runs in the worker process to construct
        the worker object and start its processing loop.

        This function is the parameter `target` to `Process`.
        As such, elements in `init_kwargs` go through pickling,
        hence they should consist
        mainly of small, native types such as string, number,small dict.
        Be careful about passing custom class objects in `init_kwargs`.
        '''
        try:
            log_passer.start()
            if cpus:
                psutil.Process().cpu_affinity(cpus=cpus)
            super().run(**kwargs)
        finally:
            log_passer.stop()


class ThreadWorker(Worker):
    '''
    Use this class if the operation is I/O bound (e.g. calling an external service
    to get some info), and computation is very light compared to the I/O part.
    Another use-case of this class is to perform some very simple and quick
    pre-processing or post-processing.
    '''
    pass


def make_threadworker(func: Callable[[Any], Any]):
    # Define a simple ThreadWorker subclass for quick, "on-the-fly" use.
    # This can be useful when we want to introduce simple servlets
    # for pre-processing and post-processing.
    class MyThreadWorker(ThreadWorker):
        def call(self, x):
            return func(x)

    MyThreadWorker.__name__ = f"ThreadWorker-{func.__name__}"
    return MyThreadWorker


PassThrough = make_threadworker(lambda x: x)
# Example use of this class:
#
#    def combine(x):
#       '''
#       Combine the ensemble elements depending on the results
#       as well as the original input.
#       '''
#       x, *y = x
#       assert len(y) == 3
#       if x < 100:
#           return sum(y) / len(y)
#       else:
#           return max(y)
#
#   s = Ensemble(
#           ThreadServlet(PassThrough),
#           ProcessServlet(W1),
#           ProcessServlet(W2)
#           ProcessServlet(W3),
#       )
#   ss = Sequential(s, ThreadServlet(make_threadworker(combine)))


class ProcessServlet:
    def __init__(self,
                 worker_cls: Type[ProcessWorker],
                 *,
                 cpus: list = None,
                 name: str = None,
                 **kwargs):
        '''
        `cpus`: specifies how many processes to create and how they are pinned
                to specific CPUs. The default is a single unpinned process.
        '''
        self._worker_cls = worker_cls
        self._name = name or worker_cls.__name__
        self._cpus = cpus
        self._init_kwargs = kwargs
        self._workers = []
        self._started = False

    def start(self, q_in, q_out, *, log_passer, ctx):
        '''
        `ctx` is a multiprocessing context passed in ultimately from `Server`.
        '''
        assert not self._started

        # CPU allocation can look as flexible as this:
        #   [[0, 1, 2], [0], [2, 3], [4, 5, 6], 4, None]
        cpus = self._cpus or [None]
        for i, c in enumerate(cpus):
            if c is not None:
                if isinstance(c, int):
                    cpus[i] = [c]
                else:
                    assert all(isinstance(v, int) for v in c)

        for cpu in cpus:
            # Create as many processes as the length of `cpus`.
            # Each process is pinned to the specified cpu core.
            if cpu is None:
                sname = self._name
            else:
                sname = f"{self._name}-{','.join(map(str, cpu))}"
            logger.info('adding worker <%s> at CPU %s ...', sname, cpu)
            w = ctx.Process(
                target=self._worker_cls.run,
                name=sname,
                kwargs={
                    'q_in': q_in,
                    'q_out': q_out,
                    'cpus': cpu,
                    'log_passer': log_passer,
                    **self._init_kwargs,
                },
            )
            self._workers.append(w)
            w.start()
            name = q_out.get()
            logger.debug(f"   ... worker <{name}> is ready")

        logger.info(f"servlet {self._name} is ready")
        self._started = True

    def stop(self):
        assert self._started
        for w in self._workers:
            w.join()
        self._workers = []
        self._started = False


class ThreadServlet:
    def __init__(self,
                 worker_cls: Type[ThreadWorker],
                 *,
                 num_threads: int = None,
                 name: str = None,
                 **kwargs):
        self._worker_cls = worker_cls
        self._name = name or worker_cls.__name__
        self._num_threads = num_threads or 1
        self._init_kwargs = kwargs
        self._workers = []
        self._started = False

    def start(self, q_in, q_out):
        assert not self._started
        for ithread in range(self._num_threads):
            sname = f"{self._name}-{ithread}"
            logger.info('adding worker <%s> in thread ...', sname)
            w = Thread(
                target=self._worker_cls.run,
                name=sname,
                kwargs={
                    'q_in': q_in,
                    'q_out': q_out,
                    **self._init_kwargs,
                },
            )
            self._workers.append(w)
            w.start()
            name = q_out.get()
            logger.debug(f"   ... worker <{name}> is ready")

        logger.info(f"servlet {self._name} is ready")
        self._started = True

    def stop(self):
        assert self._started
        for w in self._workers:
            w.join()
        self._workers = []
        self._started = False


class Sequential:
    '''
    A sequence of operations performed in order,
    the previous op's output becoming the subsequent
    op's input.
    '''
    def __init__(self, *servlets: Servlet):
        assert len(servlets) > 0
        self._servlets = servlets
        self._qs = []
        self._started = False

    def start(self, q_in, q_out, *, log_passer, ctx):
        assert not self._started
        nn = len(self._servlets)
        q1 = q_in
        for i, s in enumerate(self._servlets):
            if i + 1 < nn:
                if isinstance(s, ThreadServlet) and isinstance(self._servlets[i + 1], ThreadServlet):
                    q2 = SimpleQueue()
                else:
                    q2 = FastQueue(ctx=ctx)
                self._qs.append(q2)
            else:
                q2 = q_out
            if isinstance(s, ThreadServlet):
                s.start(q1, q2)
            else:
                s.start(q1, q2, log_passer=log_passer, ctx=ctx)
            q1 = q2
        self._started = True

    def stop(self):
        assert self._started
        for s in self._servlets:
            s.stop()
        self._qs = []
        self._started = False

    @property
    def _workers(self):
        w = []
        for s in self._servlets:
            w.extend(s._workers)
        return w


class Ensemble:
    '''
    A number of operations performed on the same input
    in parallel, the list of results are gathered and combined
    to form a final result.
    '''
    def __init__(self, *servlets: Servlet):
        assert len(servlets) > 1
        self._servlets = servlets
        self._started = False

    def _reset(self):
        self._qin = None
        self._qout = None
        self._qins = []
        self._qouts = []
        self._uid_to_results = {}
        self._threads = []

    def start(self, q_in, q_out, *, log_passer, ctx):
        assert not self._started
        self._reset()
        self._qin = q_in
        self._qout = q_out
        for s in self._servlets:
            if isinstance(s, ThreadServlet):
                q1 = SimpleQueue()
                q2 = SimpleQueue()
                s.start(q1, q2)
            else:
                q1 = FastQueue(ctx=ctx)
                q2 = FastQueue(ctx=ctx)
                s.start(q1, q2, log_passer=log_passer, ctx=ctx)
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
            if v == NOMOREDATA:
                for q in qins:
                    q.put(v)
                break

            uid, x = v
            if is_exception(x):
                qout.put((uid, x))
                continue
            z = {'y': [None] * nn, 'n': 0}
            catalog[uid] = z
            for q in qins:
                q.put((uid, x))

    def _dequeue(self):
        threading.current_thread().name = f"{self.__class__.__name__}._dequeue"
        qout = self._qout
        qouts = self._qouts
        catalog = self._uid_to_results
        nn = len(qouts)
        while True:
            for idx, q in enumerate(qouts):
                while not q.empty():
                    # Get all available results out of this queue.
                    # They are for different requests.
                    v = q.get()
                    if v == NOMOREDATA:
                        qout.put(v)
                        return
                    uid, y = v
                    z = catalog[uid]
                    z['y'][idx] = y
                    z['n'] += 1
                    if z['n'] == nn:
                        # All results for this request have been collected.
                        catalog.pop(uid)
                        y = z['y']
                        qout.put((uid, y))

    def stop(self):
        assert self._started
        for s in self._servlets:
            s.stop()
        for t in self._threads:
            t.join()
        self._reset()
        self._started = False

    @property
    def _workers(self):
        w = []
        for s in self._servlets:
            w.extend(s._workers)
        return w


Servlet = Union[ProcessServlet, ThreadServlet, Sequential, Ensemble]


class Server:
    def __init__(self,
                 servlet: Servlet,
                 *,
                 main_cpu: int = 0,
                 backlog: int = 1024,
                 ctx=None,
                 sys_info_log_cadence: int = 1_000_000,
                 ):
        '''
        `main_cpu`: specifies the cpu for the "main process",
        i.e. the process in which this server objects resides.

        `backlog`: max number of requests concurrently in progress within this server,
            all pipes/servlets/stages combined.

        `ctx`: a multiprocessing context. Leave this at default unless you know what you're doing.

        `sys_info_log_cadence`: log worker process system info (cpu/memory utilization)
            every this many requests; if `None`, do not log.
        '''
        self._servlet = servlet

        assert backlog > 0
        self._backlog = backlog

        if ctx is None:
            self._ctx = multiprocessing.get_context('spawn')
        else:
            self._ctx = ctx

        if main_cpu is not None:
            # Pin this coordinating thread to the specified CPUs.
            if isinstance(main_cpu, int):
                cpus = [main_cpu]
            else:
                assert isinstance(main_cpu, list)
                cpus = main_cpu
            psutil.Process().cpu_affinity(cpus=cpus)

        self._sys_info_log_cadence = sys_info_log_cadence
        self._started = False

    def __enter__(self):
        # After adding servlets, all other methods of this object
        # should be used with context manager `__enter__`/`__exit__`
        assert not self._started

        self._worker_logger = ProcessLogger(ctx=self.get_mpcontext())
        self._worker_logger.start()

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

        self._q_in = None
        self._q_out = None
        if isinstance(self._servlet, (Ensemble, ThreadServlet)):
            self._q_in = queue.Queue()
            self._q_out = queue.Queue()
        else:
            self._q_in = FastQueue(ctx=self.get_mpcontext())
            self._q_out = FastQueue(ctx=self.get_mpcontext())
        if isinstance(self._servlet, ThreadServlet):
            self._servlet.start(self._q_in, self._q_out)
        else:
            self._servlet.start(
                self._q_in, self._q_out,
                log_passer=self._worker_logger, ctx=self.get_mpcontext())

        t = Thread(target=self._gather_output)
        t.start()
        self._threads.append(t)

        t = Thread(target=self._onboard_input)
        t.start()
        self._threads.append(t)

        self._started = True
        return self

    def __exit__(self, exc_type=None, exc_value=None, exc_traceback=None):
        assert self._started
        msg = exit_err_msg(self, exc_type, exc_value, exc_traceback)
        if msg:
            logger.error(msg)

        self._input_buffer.put(NOMOREDATA)

        self._servlet.stop()

        for t in self._threads:
            t.join()

        self._worker_logger.stop()

        # Reset CPU affinity.
        psutil.Process().cpu_affinity(cpus=[])
        self._started = False

    def get_mpcontext(self):
        # If subclasses need to use additional Queues, Locks, Conditions, etc,
        # they should create them out of this context.
        return self._ctx

        # TODO: how to track helper processes created by subclasses, so that
        # the sys info log in `_gather_output` can include them?

    async def async_call(self, x, /, *, timeout: Union[int, float] = 60):
        # When this is called, it's usually backing a (http or other) service.
        # Concurrent async calls to this may happen.
        # At the same time, `call` and `stream` are not used.
        fut = await self._async_enqueue(x, timeout)
        return await self._async_wait_for_result(fut)

    def call(self, x, /, *, timeout: Union[int, float] = 60):
        # This is called in "embedded" mode for sporadic uses.
        # It is not designed to serve high load from multi-thread concurrent
        # calls. To process large amounts in embedded model, use `stream`.
        fut = self._enqueue(x, timeout)
        return self._wait_for_result(fut)

    def stream(self, data_stream, /, *,
               return_x: bool = False,
               return_exceptions: bool = False,
               timeout=60,
               ):
        '''
        The order of elements in the stream is preserved, i.e.,
        elements in the output stream corresponds to elements
        in the input stream in the same order.
        '''

        # For streaming, "timeout" is usually not a concern.
        # The concern is overall throughput.

        def _enqueue(tasks, return_exceptions):
            threading.current_thread().name = f"{self.__class__.__name__}.stream._enqueue"
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
                # Uncaptured exceptions will propagate and cause the thread to exit in
                # exception state. This exception is not covered by `return_exceptions`;
                # it will be detected in the main thread.
            finally:
                tasks.put(NOMOREDATA)

        tasks = queue.SimpleQueue()
        executor = concurrent.futures.ThreadPoolExecutor(1)
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
                    # May raise RemoteException or TimeoutError.
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

    async def _async_enqueue(self, x, timeout):
        t0 = perf_counter()
        deadline = t0 + timeout

        while len(self._uid_to_futures) >= self._backlog:
            if (t := perf_counter()) > deadline:
                raise TimeoutError(f"{t - t0:.3f} seconds enqueue")
            await asyncio.sleep(min(0.01, deadline - t))
            # It's OK if this sleep is a little long,
            # because the pipe is full and busy.

        # fut = asyncio.Future()
        fut = concurrent.futures.Future()
        fut.data = {'t0': t0, 'timeout': timeout}
        uid = id(fut)
        self._uid_to_futures[uid] = fut
        self._input_buffer.put((uid, x))
        return fut

    async def _async_wait_for_result(self, fut):
        t0 = fut.data['t0']
        t2 = t0 + fut.data['timeout']
        while not fut.done():
            timenow = perf_counter()
            if timenow > t2:
                fut.cancel()
                raise TimeoutError(f"{timenow - t0:.3f} seconds total")
            await asyncio.sleep(min(0.001, t2 - timenow))
        return fut.result()
        # This could raise RemoteException.

        # TODO: I don't understand why the following (along with
        # corresponding change in `_async_enqueue`) seems to work but
        # is very, very slow.

        # t0 = fut.data['t0']
        # t2 = t0 + fut.data['timeout']
        # try:
        #     return await asyncio.wait_for(fut, timeout=max(0, t2 - perf_counter()))
        #     # this may raise RemoteException
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
            sleep(min(0.01, deadline - t))
            # It's OK if this sleep is a little long,
            # because the pipe is full and busy.

        fut = concurrent.futures.Future()
        fut.data = {'t0': t0, 'timeout': timeout}
        uid = id(fut)
        self._uid_to_futures[uid] = fut
        self._input_buffer.put((uid, x))
        return fut

    def _wait_for_result(self, fut):
        t0 = fut.data['t0']
        t2 = t0 + fut.data['timeout']
        try:
            return fut.result(timeout=max(0, t2 - perf_counter()))
            # this may raise RemoteException
        except concurrent.futures.TimeoutError:
            fut.cancel()
            raise TimeoutError(f"{perf_counter() - t0:.3f} seconds total")

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

        log_cadence = self._sys_info_log_cadence
        if log_cadence:
            ts0 = datetime.utcnow()
            logcounter = 0

            def _log_sys_info():
                pp = [(w.name, psutil.Process(w.pid))
                      for w in self._servlet._workers
                      if not isinstance(w, Thread)]
                msg = [f"  time from           {ts0}",
                       f"  time to             {datetime.utcnow()}",
                       f"  items served        {logcounter:_}",
                       ]
                attrs = ['memory_full_info',
                         'cpu_affinity', 'cpu_percent', 'cpu_times',
                         'num_threads', 'num_fds', 'io_counters',
                         'status',
                         ]
                for pname, pobj in pp:
                    msg.append(f'  process {pname}')
                    info = pobj.as_dict(attrs)
                    for k in attrs:
                        v = info[k]
                        if k == 'memory_full_info':
                            msg.append(f"        {'memory_uss':<15} {v.uss / 1_000_000:.2f} MB")
                        else:
                            msg.append(f"        {k:<15} {v}")
                logger.info('worker process stats:\n%s', '\n'.join(msg))

        try:
            while True:
                z = q_out.get()
                if z == NOMOREDATA:
                    break
                uid, y = z
                fut = futures.pop(uid)
                try:
                    if is_exception(y):
                        fut.set_exception(y)
                    else:
                        fut.set_result(y)
                    fut.data['t1'] = perf_counter()
                except (concurrent.futures.InvalidStateError, asyncio.InvalidStateError) as e:
                    if fut.cancelled():
                        # Could have been cancelled due to TimeoutError.
                        # logger.debug('Future object is already cancelled')
                        pass
                    else:
                        # Unexpected situation; to be investigated.
                        logger.exception(e)
                        raise

                if log_cadence:
                    logcounter += 1
                    if (logcounter >= log_cadence and q_out.empty()) or logcounter >= log_cadence * 10:
                        _log_sys_info()
                        logcounter = 0
                        ts0 = datetime.utcnow()
        finally:
            if log_cadence and logcounter:
                _log_sys_info()
