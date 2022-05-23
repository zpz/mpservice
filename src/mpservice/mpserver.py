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
from abc import ABCMeta, abstractmethod
from concurrent.futures import Future
from multiprocessing import synchronize, queues
from queue import Empty, Full
from time import monotonic, sleep
from typing import Sequence, Union, Callable, Type, Protocol, Any

import psutil

from .remote_exception import RemoteException, exit_err_msg
from .util import is_exception, Thread, Process, ProcessLogger
from .streamer import put_in_queue
from . import _queues


# Set level for logs produced by the standard `multiprocessing` module.
multiprocessing.log_to_stderr(logging.WARNING)

logger = logging.getLogger(__name__)

NOMOREDATA = b'c7160a52-f8ed-40e4-8a38-ec6b84c2cd87'


class TimeoutError(Exception):
    pass


class BatchQueue:
    '''
    Eagerly collect data in a background thread to facilitate batching.
    '''
    def __init__(self, batch_size: int, queue_in, queue_err, *, rlock, buffer_size: int = None):
        assert 1 <= batch_size
        if buffer_size is None:
            buffer_size = batch_size
        else:
            assert batch_size <= buffer_size

        self._buffer_size = buffer_size
        self._batch_size = batch_size
        self._buffer = _queues.SingleLane(buffer_size)

        self._qin = queue_in
        self._lock = rlock
        self._qerr = queue_err

        self._get_called = threading.Event()
        self._thread = Thread(target=self._read, daemon=True)
        self._thread.start()

    def _read(self):
        threading.current_thread().name = f"{self.__class__.__name__}._read"
        buffer = self._buffer
        lock = self._lock
        getcalled = self._get_called
        closed = self._buffer._closed
        batchsize = self._batch_size
        qin = self._qin
        qerr = self._qerr

        while True:
            if buffer.full():
                with buffer._not_full:
                    buffer._not_full.wait()
            try:
                with lock:  # read lock
                    # In order to facilitate batching,
                    # we hold the lock and keep getting
                    # data off the pipe for this reader's buffer,
                    # even though there may be other readers waiting.
                    while True:
                        n = max(1, batchsize - buffer.qsize())
                        try:
                            # `faster_fifo.Queue.get_many` will wait
                            # up to the specified timeout if there's nothing;
                            # once there's something to read, it will
                            # read up to the max count that's already there
                            # to be read---importantly, it will not spread
                            # out the wait in order to collect more elements.
                            z = qin.get_many(
                                block=True,
                                timeout=float(60),
                                max_messages_to_get=int(n))
                        except Empty:
                            pass
                        else:
                            for x in z:
                                if x == NOMOREDATA:
                                    buffer.put(x)
                                    qin.put(x)
                                    return

                                if is_exception(x[1]):
                                    qerr.put(x)
                                else:
                                    buffer.put(x)

                        if closed:
                            return
                        if getcalled.is_set():
                            # Once `get` or `get_many` has been called,
                            # we release the lock so that other readers
                            # get a chance for the lock.
                            getcalled.clear()
                            break
                        if buffer.qsize() >= batchsize:
                            break

            except Exception:
                if closed:
                    break
                raise

    def close(self):
        self._buffer.close()

    def __del__(self):
        self.close()

    def empty(self):
        return self._buffer.empty()

    def qsize(self):
        return self._buffer.qsize()

    def get_many(self, *, first_timeout=None, extra_timeout=None):
        buffer = self._buffer
        out = buffer.get(timeout=first_timeout)
        if out == NOMOREDATA:
            return out

        out = [out]
        max_n = self._batch_size
        n = 1
        if max_n > 1:
            if extra_timeout is None:
                extra_timeout = 60
            deadline = monotonic() + extra_timeout
            while n < max_n:
                t = deadline - monotonic()
                try:
                    z = buffer.get(timeout=t)
                except Empty:
                    break
                if z == NOMOREDATA:
                    # Return the batch so far.
                    # Put this indicator back in the queue.
                    # Next call to this method will get
                    # the indicator.
                    buffer.put(z)
                    break
                out.append(z)
                n += 1
        self._get_called.set()
        return out


class Worker(metaclass=ABCMeta):
    # Typically a subclass needs to enhance
    # `__init__` and implement `call`.

    @classmethod
    def run(cls, *,
            q_in: _queues.Unique,
            q_out: _queues.Unique,
            q_indicator: queues.Queue,
            **init_kwargs):
        try:
            obj = cls(**init_kwargs)
            q_indicator.put(obj.name)
            batch_size = obj.batch_size
            if batch_size > 1:
                q_in = BatchQueue(batch_size, q_in, q_out, rlock=q_in._rlock, buffer_size=batch_size + 10)
            obj.start(q_in=q_in, q_out=q_out)
        finally:
            q_in.close()
            if isinstance(q_in, BatchQueue):
                q_in._qin.close()
            q_out.close()

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

    def start(self, *, q_in, q_out):
        try:
            if self.batch_size > 1:
                self._start_batch(q_in=q_in, q_out=q_out)
            else:
                self._start_single(q_in=q_in, q_out=q_out)
        except KeyboardInterrupt:
            print(self.name, 'stopped by KeyboardInterrupt')

    def _start_single(self, *, q_in, q_out):
        batch_size = self.batch_size
        while True:
            try:
                z = q_in.get()
            except Empty:
                continue
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
        batch_wait_time = self.batch_wait_time

        def print_batching_info():
            logger.info('%d batches with sizes %d--%d, mean %.1f',
                        n_batches, batch_size_min, batch_size_max, batch_size_mean)

        n_batches = 0
        try:
            while True:
                if n_batches == 0:
                    batch_size_max = -1
                    batch_size_min = 1000000
                    batch_size_mean = 0.0

                try:
                    batch = q_in.get_many(first_timeout=0.5, extra_timeout=batch_wait_time)
                except Empty:
                    continue
                if batch == NOMOREDATA:
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
                    while True:
                        try:
                            q_out.put_many(list(zip(uids, results)))
                            break
                        except Full:
                            continue

                n_batches += 1
                batch_size_max = max(batch_size_max, n)
                batch_size_min = min(batch_size_min, n)
                batch_size_mean = (batch_size_mean * (n_batches - 1) + n) / n_batches
                if n_batches % 1000 == 0:
                    print_batching_info()
                    n_batches = 0
        finally:
            if n_batches:
                print_batching_info()

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
            # TODO: does the log message carry process info?
            if cpus:
                psutil.Process().cpu_affinity(cpus=cpus)
            super().run(**kwargs)
        finally:
            log_passer.stop()


class ServletProtocol(Protocol):
    def start(self, q_in, q_out, *, log_passer, should_stop, ctx):
        pass

    def stop(self):
        pass


class Servlet:
    def __init__(self,
                 worker_cls: Type[ProcessWorker],
                 *,
                 cpus: list = None,
                 workers: int = None,
                 name: str = None,
                 **kwargs):
        self._worker_cls = worker_cls
        self._name = name or worker_cls.__name__
        self._cpus = self._resolve_cpus(cpus=cpus, workers=workers)
        self._init_kwargs = kwargs
        self._workers = []
        self._started = False

    def start(self, q_in, q_out, *, log_passer, should_stop, ctx):
        assert not self._started
        q_indicator = ctx.Queue()
        for cpu in self._cpus:
            # Pinned to the specified cpu core.
            sname = f"{self._name}-{','.join(map(str, cpu))}"
            logger.info('adding worker <%s> at CPU %s ...', sname, cpu)
            w = Process(
                ctx=ctx,
                target=self._worker_cls.run,
                name=sname,
                kwargs={
                    'q_in': q_in,
                    'q_out': q_out,
                    'cpus': cpu,
                    'log_passer': log_passer,
                    'q_indicator': q_indicator,
                    **self._init_kwargs,
                },
            )
            self._workers.append(w)
            w.start()
            name = q_indicator.get()
            logger.debug(f"   ... worker <{name}> is ready")
        logger.info(f"servlet {self._name} is ready")
        q_indicator.close()
        self._started = True

    def stop(self):
        assert self._started
        for w in self._workers:
            w.join()
        self._workers = []
        self._started = False

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
            # Create as many processes as there are cores minus core '0',
            # one process pinned to one core.
            cpus = list(range(1, n_cpus))
            # List[int]

        for i, cpu in enumerate(cpus):
            if isinstance(cpu, int):
                cpu = [cpu]
                cpus[i] = cpu
            assert all(0 <= c < n_cpus for c in cpu)

        return cpus


class Sequential:
    '''
    A sequence of operations performed in order,
    the previous op's result becoming the subsequent
    op's input.
    '''
    def __init__(self, *servlets: ServletProtocol):
        assert len(servlets) > 0
        self._servlets = servlets
        self._qs = []
        self._started = False

    def start(self, q_in, q_out, *, log_passer, should_stop, ctx):
        assert not self._started
        nn = len(self._servlets)
        q1 = q_in
        self._qs.append(q1)
        for i, s in enumerate(self._servlets):
            if i + 1 < nn:
                q2 = _queues.Unique(ctx=ctx)
            else:
                q2 = q_out
            s.start(q1, q2, log_passer=log_passer, should_stop=should_stop, ctx=ctx)
            self._qs.append(q2)
            q1 = q2
        self._started = True

    def stop(self):
        assert self._started
        for s in self._servlets:
            s.stop()
        for q in self._qs[1:-1]:
            q.close()
        self._qs = []
        self._started = False


class Ensemble:
    '''
    A number of operations performed on the same input
    in parallel, the list of results gathered and combined
    to form a final result.
    '''
    def __init__(self, *servlets: ServletProtocol,
                 post_func: Callable[[Any, list], Any] = None):
        assert len(servlets) > 1
        self._servlets = servlets
        self._func = post_func
        self._started = False

    def _reset(self):
        self._qin = None
        self._qout = None
        self._qins = []
        self._qouts = []
        self._uid_to_results = {}
        self._threads = []

    def start(self, q_in, q_out, *, log_passer, should_stop, ctx):
        assert not self._started
        self._reset()
        self._qin = q_in
        self._qout = q_out
        for s in self._servlets:
            q1 = _queues.Unique(ctx=ctx)
            q2 = _queues.Unique(ctx=ctx)
            s.start(q1, q2, log_passer=log_passer, should_stop=should_stop, ctx=ctx)
            self._qins.append(q1)
            self._qouts.append(q2)
        t = Thread(target=self._dequeue, args=(should_stop,))
        t.start()
        self._threads.append(t)
        t = Thread(target=self._enqueue, args=(should_stop,))
        t.start()
        self._threads.append(t)
        self._started = True

    def _enqueue(self, should_stop):
        threading.current_thread().name = f"{self.__class__.__name__}._enqueue"
        qin = self._qin
        qout = self._qout
        qins = self._qins
        catalog = self._uid_to_results
        hasfunc = self._func is not None
        nn = len(qins)
        while not should_stop.is_set():
            try:
                v = qin.get(timeout=0.1)
            except Empty:
                continue
            if v == NOMOREDATA:
                for q in qins:
                    q.put(v)
                break

            uid, x = v
            if is_exception(x):
                qout.put((uid, x))
                continue
            z = {'y': [None] * nn, 'n': 0}
            if hasfunc:
                z['x'] = x
            catalog[uid] = z
            for q in qins:
                q.put((uid, x))

    def _dequeue(self, should_stop):
        threading.current_thread().name = f"{self.__class__.__name__}._dequeue"
        func = self._func
        qout = self._qout
        qouts = self._qouts
        catalog = self._uid_to_results
        nn = len(qouts)
        while not should_stop.is_set():
            for idx, q in enumerate(qouts):
                while not q.empty():
                    # Get all available results out of this queue.
                    # They are for different requests.
                    v = q.get(timeout=0)
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
                        if func is not None:
                            y = func(z['x'], y)
                        qout.put((uid, y))

    def stop(self):
        assert self._started
        for s in self._servlets:
            s.stop()
        for q in self._qins:
            q.close()
        for q in self._qouts:
            q.close()
        for t in self._threads:
            t.join()
        self._reset()
        self._started = False


class Server:
    def __init__(self, servlet: ServletProtocol, *, main_cpu: int = 0, backlog: int = 1024):
        '''
        `main_cpu`: specifies the cpu for the "main process",
        i.e. the process in which this server objects resides.

        `backlog`: max number of requests concurrently in progress within this server,
            all pipes/servlets/stages combined.
        '''
        self._servlet = servlet

        assert backlog > 0
        self._backlog = backlog

        self._should_stop = self.get_mpcontext().Event()

        if main_cpu is not None:
            # Pin this coordinating thread to the specified CPUs.
            if isinstance(main_cpu, int):
                cpus = [main_cpu]
            else:
                assert isinstance(main_cpu, list)
                cpus = main_cpu
            psutil.Process().cpu_affinity(cpus=cpus)

        self._started = False

    def __enter__(self):
        # After adding servlets, all other methods of this object
        # should be used with context manager `__enter__`/`__exit__`
        assert not self._started
        assert not self._should_stop.is_set()

        self._worker_logger = ProcessLogger(ctx=self.get_mpcontext())
        self._worker_logger.start()

        self._threads = []

        self._uid_to_futures = {}
        # Size of this dict is capped at `self._backlog`.
        # A few places need to enforce this size limit.

        # # Use thse to enforce `backlog` in sync mode.
        # mutex = threading.Lock()
        # self._notfull = threading.Condition(lock=mutex)
        # self._notempty = threading.Condition(lock=mutex)

        # # Use thse to enforce `backlog` in async mode.
        # amutex = asyncio.Lock()
        # self._a_notfull = asyncio.Condition(lock=amutex)
        # self._a_notempty = asyncio.Condition(lock=amutex)

        self._input_buffer = queue.SimpleQueue()
        # This has unlimited size; `put` never blocks (as long as
        # memory is not blown up!). Input requests respect size limit
        # of `_uid_to_futures`, but is not blocked when putting
        # into this queue. A background thread takes data out of this
        # queue and puts them into `_q_in`, which could block.

        self._q_in = None
        self._q_out = None
        if isinstance(self._servlet, Ensemble):
            self._q_in = queue.Queue()
            self._q_out = queue.Queue()
        else:
            self._q_in = _queues.Unique(ctx=self.get_mpcontext())
            self._q_out = _queues.Unique(ctx=self.get_mpcontext())
        self._servlet.start(
            self._q_in,
            self._q_out,
            log_passer=self._worker_logger,
            should_stop=self._should_stop,
            ctx=self.get_mpcontext())

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
        self._should_stop.set()

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
        # This method does not create a new object.
        # It returns the same object.
        return multiprocessing.get_context('spawn')

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
            should_stop = self._should_stop
            try:
                for x in data_stream:
                    try:
                        fut = _enq(x, timeout)
                        timedout = False
                    except Exception as e:
                        if return_exceptions:
                            timedout = True
                            fut = Future()
                            fut.set_exception(e)
                        else:
                            logger.error("exception '%r' happened for input '%s'", e, x)
                            raise
                    tasks.put((x, fut, timedout))
                    if should_stop.is_set():
                        break
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
        t0 = monotonic()
        deadline = t0 + timeout

        while len(self._uid_to_futures) >= self._backlog:
            if (t := monotonic()) > deadline:
                raise TimeoutError(f"{t - t0} seconds enqueue")
            await asyncio.sleep(min(0.001, deadline - t))

        fut = Future()
        fut.data = {'t0': t0, 'timeout': timeout}
        uid = id(fut)
        self._uid_to_futures[uid] = fut
        self._input_buffer.put((uid, x))
        return fut

    async def _async_wait_for_result(self, fut):
        t0 = fut.data['t0']
        t2 = t0 + fut.data['timeout']
        while not fut.done():
            timenow = monotonic()
            if timenow > t2:
                fut.cancel()
                raise TimeoutError(f"{timenow - t0} seconds total")
            await asyncio.sleep(min(0.001, t2 - timenow))
        return fut.result()
        # This could raise RemoteException.

    def _enqueue(self, x, timeout):
        # This method is called by `call` or `stream`.
        # There are no concurrent calls to this method.
        t0 = monotonic()
        deadline = t0 + timeout

        while len(self._uid_to_futures) >= self._backlog:
            if (t := monotonic()) >= deadline:
                raise TimeoutError(f"{t - t0} seconds enqueue")
            sleep(min(0.01, deadline - t))

        fut = Future()
        fut.data = {'t0': t0, 'timeout': timeout}
        uid = id(fut)
        self._uid_to_futures[uid] = fut
        self._input_buffer.put((uid, x))
        return fut

    def _wait_for_result(self, fut):
        t0 = fut.data['t0']
        t2 = t0 + fut.data['timeout']
        try:
            return fut.result(timeout=max(0, t2 - monotonic()))
            # this may raise RemoteException
        except concurrent.futures.TimeoutError:
            fut.cancel()
            raise TimeoutError(f"{monotonic() - t0} seconds total")

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

        while not self._should_stop.is_set():
            try:
                z = q_out.get(timeout=1)
            except Empty:
                continue
            if z == NOMOREDATA:
                break
            uid, y = z
            fut = futures.pop(uid)
            try:
                if is_exception(y):
                    fut.set_exception(y)
                else:
                    fut.set_result(y)
                fut.data['t1'] = monotonic()
            except asyncio.InvalidStateError as e:
                if fut.cancelled():
                    # Could have been cancelled due to TimeoutError.
                    # logger.debug('Future object is already cancelled')
                    pass
                else:
                    # Unexpected situation; to be investigated.
                    logger.exception(e)
                    raise
