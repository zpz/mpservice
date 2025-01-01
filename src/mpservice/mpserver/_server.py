import asyncio
import concurrent.futures
import logging
import multiprocessing
import multiprocessing.queues
import queue
import threading
from collections.abc import AsyncIterable, AsyncIterator, Iterable, Iterator
from datetime import datetime, timezone
from time import perf_counter
from typing import Callable, final

from mpservice._common import TimeoutError
from mpservice.multiprocessing import MP_SPAWN_CTX
from mpservice.multiprocessing.remote_exception import RemoteException
from mpservice.streamer import async_fifo_stream, fifo_stream
from mpservice.threading import Thread

from ._servlet import Servlet
from ._worker import _SimpleProcessQueue, _SimpleThreadQueue

logger = logging.getLogger(__name__)


class ServerBacklogFull(RuntimeError):
    def __init__(self, n, x=None):
        super().__init__(n, x)

    def __str__(self):
        n, x = self.args
        if x is None:
            return f'Server is at capacity with {n} items in proces; new request is rejected immediately due to back-pressure'
        return f'Server is at capacity with {n} items in proces; new request is rejected after waiting for {x:.3f} seconds'


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
