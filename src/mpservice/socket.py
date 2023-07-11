"""The module ``mpservice.socket`` provides tools to use sockets to communicate between
two Python processes on the same machine.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import functools
import inspect
import logging
import os
import queue
import stat
import subprocess
import threading
import time
from collections.abc import Iterable
from pickle import dumps as pickle_dumps
from pickle import loads as pickle_loads
from time import perf_counter
from typing import Any, Awaitable, Callable

from ._queues import SingleLane
from .concurrent.futures import ThreadPoolExecutor
from .multiprocessing import RemoteException
from .threading import MAX_THREADS

__all__ = [
    'SocketApplication',
    'SocketServer',
    'SocketClient',
    'make_server',
]

logger = logging.getLogger(__name__)


def is_async(func):
    while isinstance(func, functools.partial):
        func = func.func
    return inspect.iscoroutinefunction(func) or (
        not inspect.isfunction(func)
        and hasattr(func, "__call__")
        and inspect.iscoroutinefunction(func.__call__)
    )


def get_docker_host_ip():
    """
    From within a Docker container, this function finds the IP address
    of the host machine.
    """
    # INTERNAL_HOST_IP=$(ip route show default | awk '/default/ {print $3})')
    # another idea:
    # ip -4 route list match 0/0 | cut -d' ' -f3
    #
    # Usually the result is '172.17.0.1'
    #
    # The command `ip` is provided by the Linux package `iproute2`.

    z = subprocess.check_output(
        ["ip", "-4", "route", "list", "match", "0/0"]  # noqa: S603, S607
    )
    z = z.decode()[len("default via ") :]
    return z[: z.find(" ")]


def put_in_queue(q, x, stop_event, timeout=0.1):
    """
    ``q`` is either a ``threading.Queue`` or a ``multiprocessing.queues.Queue``,
    but not an ``asyncio.Queue``, because the latter does not take
    the ``timeout`` argument.

    This is used in a blocking mode to put ``x`` in the queue
    till success. It checks for any request of early-stop indicated by
    ``stop_event``, which is either ``threading.Event`` or ``multiprocessing.synchronize.Event``.
    Usually there is not need to customize the value of ``timeout``,
    because in this use case it's OK to try a little long before checking
    early-stop.

    Return ``True`` if successfully put in queue; ``False`` if early-stop is detected.
    """
    while True:
        try:
            q.put(x, timeout=timeout)
            return True
        except queue.Full:
            if stop_event.is_set():
                return False


# References about Docker networking:
#
#  Docker Containers: IPC using Sockets
#  https://medium.com/technanic/docker-containers-ipc-using-sockets-part-1-2ee90885602c
#  https://medium.com/technanic/docker-containers-ipc-using-sockets-part-2-834e8ea00768
#
#  Connection refused? Docker networking and how it impacts your image
#  https://pythonspeed.com/articles/docker-connection-refused/

# Unix domain sockets
#   https://pymotw.com/2/socket/uds.html

# Python socket programming
#  https://realpython.com/python-sockets/
#  https://docs.python.org/3/howto/sockets.html


def encode(data, encoder):
    if encoder == "pickle":
        return pickle_dumps(data)
    if encoder == "utf8":
        return data.encode("utf8")
    if encoder != "none":
        raise ValueError(f"expecting 'none' but got: {encoder}")
    return data  # `data` must be bytes


def decode(data, encoder):
    if encoder == "pickle":
        return pickle_loads(data)
    if encoder == "utf8":
        return data.decode("utf")
    assert encoder == "none"
    return data  # bytes unchanged


# Our design of a record is laid out this way:
#
#    b'adf23d 24 pickle\naskadfka23kdkda'
#
# The 'header' part contains
#    - request id (whatever string the client decides to use, no space)
#    - lengths of payload in bytes,
#    - encoder
# ending with '\n'; after that comes the said number
# of bytes, which should be decoded by `decode` according to the 'encoder'.


async def write_record(writer, request_id, data, *, encoder: str = "pickle"):
    data_bytes = encode(data, encoder)
    writer.write(f"{request_id} {len(data_bytes)} {encoder}\n".encode())
    writer.write(data_bytes)
    await writer.drain()
    # TODO: add timeout?


async def read_record(reader, *, timeout=None):
    # `timeout` should be `None` or `> 0`.
    # This may raise `asyncio.TimeoutError` (nothing to read at the moment)
    # or `asyncio.IncompleteReadError` (connection has been closed by
    # the other side).
    data = await asyncio.wait_for(reader.readuntil(b"\n"), timeout)
    request_id, num_bytes, encoder = data[:-1].decode().split()
    data = await reader.readexactly(int(num_bytes))
    return request_id, decode(data, encoder)
    # If this function is called on the server side, it should include
    # `request_id` as is in the response.
    # If this function is called on the client side, after getting
    # `request_id` in the server response, it may need to transform on it
    # as per its design, e.g. if the ID is an int, now it's a string in
    # the response, hence client needs to convert it to an int.


async def run_tcp_server(conn_handler: Callable, host: str, port: int):
    server = await asyncio.start_server(conn_handler, host, port)
    async with server:
        addrs = ", ".join(str(sock.getsockname()) for sock in server.sockets)
        logger.info("serving on %s", addrs)
        await server.serve_forever()


async def run_unix_server(conn_handler: Callable, path: str):
    """
    Parameters
    ----------
    path
        A file path accessible by both the server and the client,
        which run on the same machine. This is not a regular file,
        and it (including parent directories) does not need to exist.
        In fact, if the file exists, it will be removed first.
    """
    if os.path.exists(path):
        assert stat.S_ISSOCK(
            os.stat(path).st_mode
        ), f"file '{path}' exists but is not a socket file"
    else:
        os.makedirs(os.path.dirname(path), exist_ok=True)
    server = await asyncio.start_unix_server(conn_handler, path)
    async with server:
        addrs = ", ".join(str(sock.getsockname()) for sock in server.sockets)
        logger.info("serving on %s", addrs)
        await server.serve_forever()


async def open_tcp_connection(host, port, *, timeout=None):
    t0 = perf_counter()
    while True:
        try:
            reader, writer = await asyncio.open_connection(host, port)
            return reader, writer
        except ConnectionRefusedError:
            if timeout is not None:
                if perf_counter() - t0 > timeout:
                    raise
            await asyncio.sleep(0.1)


async def open_unix_connection(path: str, *, timeout=None):
    """
    Parameters
    ----------
    path
        The same string that has been used by ``run_unix_server``.
    """
    t0 = perf_counter()
    while True:
        try:
            reader, writer = await asyncio.open_unix_connection(path)
            return reader, writer
        except (ConnectionRefusedError, FileNotFoundError):
            if timeout is not None:
                if perf_counter() - t0 > timeout:
                    raise
            await asyncio.sleep(0.1)


class SocketApplication:
    """
    ``SocketApplication`` is designed to to used similar to the "application" in a
    HTTP framework. The main API is to register "endpoint" functions by the method
    :meth:`add_route`. This allows to back the socket service by multiple functions
    for different purposes. Usually there is only one main function, which involves
    transmitting substantial amount of data between the server and the client.
    For simplicity, one may use ``'/'`` for the ``path`` of this route.
    The other routes are usually supportive, for example, getting server info
    or setting options. For example::

        app.add_route('/', make_prediction)
        app.add_route('/server-info', get_server_info)
        app.add_route('/set-option', set_server_option)

    This class is the intended interface between a socket server and a particular
    application (functions). Usually, user should not customize the class
    ``SocketServer``.
    """

    def __init__(
        self,
    ):
        self._routes = {}

    def add_route(
        self,
        path: str,
        route: Callable[[Any], Awaitable[Any]] | Callable[[], Awaitable[Any]],
    ):
        """
        ``route`` is an *async* function that takes a single positional arg,
        and returns a response (which could be ``None`` if so desired).
        The response should be serializable by the encoder.
        To be safe, return a object of Python native types.
        If exception is raised in this method, appropriate :class:`RemoteException`
        object will be sent in the response.
        The method could also proactively return a :class:`RemoteException` object.

        ``path`` is any string. The route is identified by this string. For familiarity,
        it may be a good idea to start the string with ``'/'``, although this is in no
        way necessary.

        There is no GET/POST distinction like in the case of HTTP.
        """
        # TODO: support sync "endpoint" functions, if they are I/O bound.
        self._routes[path] = route

    async def handle_request(self, path: str, data: Any = None):
        """
        Dispatch the request to a registered route function.
        """
        if data is None:
            return await self._routes[path]()
        return await self._routes[path](data)


class SocketServer:
    def __init__(
        self,
        app: SocketApplication,
        *,
        path: str | None = None,
        host: str | None = None,
        port: int | None = None,
        backlog: int | None = None,
        shutdown_path: str = "/shutdown",
    ):
        """
        ``backlog`` is the max concurrent in-progress requests per connection.
        (Note, the client may open many connections.)
        This "concurrency" is in terms of concurrent calls to
        :meth:`SocketApplication.handle_request`.

        The type of the service, between 'tcp' and 'unix', is determined
        by the parameters ``path``, ``host``, and ``port``. See code for details.
        """
        self.app = app
        if path:
            assert not host
            assert not port
            self._path = os.path.abspath(path)
            self._host = None
            self._port = None
        else:
            # If the server runs within a Docker container,
            # `host` should be '0.0.0.0'. Outside of Docker,
            # it should be '127.0.0.1' (I think but did not verify).
            assert port
            if not host:
                host = "0.0.0.0"  # in Docker
            self._path = None
            self._host = host
            self._port = int(port)
        self._backlog = backlog or 256
        self._encoder = "pickle"  # encoder when sending responses
        self._n_connections = 0
        self._shutdown_path = shutdown_path
        self.to_shutdown = False

    def __repr__(self):
        if self._path:
            return f"{self.__class__.__name__}('{self._path}')"
        return f"{self.__class__.__name__}('{self._host}:{self._port}')"

    def __str__(self):
        return self.__repr__()

    async def serve(self):
        """
        Start the server and let it stay up until shutdown conditions are met.
        """
        if self._path:
            server_task = asyncio.create_task(
                run_unix_server(self._handle_connection, self._path)
            )
        else:
            server_task = asyncio.create_task(
                run_tcp_server(self._handle_connection, self._host, self._port)
            )
        logger.info("server %s is ready", self)
        try:
            while True:
                if self.to_shutdown and not self._n_connections:
                    raise Exception("shutdown requested")
                await asyncio.sleep(0.1)
        except BaseException as e:
            # This should take care of keyboard interrupt and such.
            # To be verified.
            server_task.cancel()
            if self._path:
                os.unlink(self._path)
            logger.info("server %s is stopped", self)
            if str(e) != "shutdown requested":
                raise

    async def _handle_connection(self, reader, writer):
        """
        This is called upon a new connection that is openned
        at the request from a client to the server.
        This method handles requests in that connection.
        """
        if self._path:
            addr = writer.get_extra_info("sockname")
        else:
            addr = writer.get_extra_info("peername")
        self._n_connections += 1
        logger.info(
            "connection %d is openned from client %r", self._n_connections, addr
        )
        reqs = asyncio.Queue(self._backlog)

        async def _keep_receiving():
            # Infinite loop to handle requests on this connection
            # until the connection is closed by the client.
            loop = asyncio.get_running_loop()
            while True:
                try:
                    req_id, data = await read_record(reader, timeout=0.1)
                except asyncio.TimeoutError:
                    if self.to_shutdown:
                        return
                    continue
                # If `asyncio.IncompleteReadError` is raised, let it propage.
                path = data[0]
                data = data[1]
                if path == self._shutdown_path:
                    t = loop.create_future()
                    await reqs.put((req_id, t))
                    self.to_shutdown = True
                    t.set_result(None)
                    break

                f = self.app.handle_request(path, data)
                t = asyncio.create_task(f)
                await reqs.put((req_id, t))
                # The queue size will restrict how many concurrent calls
                # to `handle_request` can be in progress.

        # `write_record` needs to be called sequentially because it's not atomic;
        # that's why we don't use `add_done_callback` on the Futures to do
        # response writing.

        async def _keep_responding():
            while True:
                try:
                    req_id, t = await asyncio.wait_for(reqs.get(), 0.1)
                except asyncio.TimeoutError:
                    if self.to_shutdown:
                        return
                    continue
                try:
                    z = await t
                except Exception as e:
                    z = RemoteException(e)
                await write_record(writer, req_id, z, encoder=self._encoder)
                # TODO: what if client has closed the connection?

        trec = asyncio.create_task(_keep_receiving())
        tres = asyncio.create_task(_keep_responding())
        try:
            await trec
        except asyncio.IncompleteReadError:
            tres.cancel()
        else:
            await tres

        writer.close()
        await writer.wait_closed()
        logger.info("connection %d from client %r is closed", self._n_connections, addr)
        self._n_connections -= 1


def make_server(app: SocketApplication, **kwargs):
    return SocketServer(app, **kwargs)


class SocketClient:
    def __init__(
        self,
        *,
        path: None | str = None,
        host: None | str = None,
        port: None | int = None,
        num_connections: None | int = None,
        connection_timeout: int = 60,
        backlog: int = 2048,
    ):
        """
        Parameters
        ----------
        path, host, port
            Either ``path`` is given (for Unix socket),
            or ``port`` (plus optionally ``host``) is given (for Tcp socket).
            These values should, of course, be consistent with the corresponding
            server.
        num_connections
            This is expected to have a direct impact on the performance,
            hence needs experimentation.
        connection_timeout
            How many seconds to wait while connecting to the server.
            This is meant for waiting for server to be ready, rather than for
            the action of "connecting" itself (which should be fast).
        backlog
            Size of the queue for in-progress requests.
        """
        # Experiments showed `max_connections` can be up to 200.
        # This needs to be improved.
        if path:
            # `path` must be consistent with that passed to `run_unix_server`.
            assert not host
            assert not port
            self._path = path
            self._host = None
            self._port = None
        else:
            # If both client and server run on the same machine
            # in separate Docker containers, `host` should be
            # `mpservice._util.get_docker_host_ip()`.
            assert port
            if not host:
                host = get_docker_host_ip()  # in Docker
            self._path = None
            self._host = host
            self._port = int(port)

        self._num_connections = num_connections or MAX_THREADS
        self._connection_timeout = connection_timeout
        self._backlog = backlog
        self._encoder = "pickle"  # encoder when sending requests.
        self._prepare_shutdown = threading.Event()
        self._to_shutdown = threading.Event()
        self._executor = ThreadPoolExecutor()
        self._tasks = []
        self._pending_requests: queue.Queue | None = None
        self._active_requests = {}
        self._shutdown_timeout = 60

    def __repr__(self):
        if self._path:
            return f"{self.__class__.__name__}('{self._path}')"
        return f"{self.__class__.__name__}('{self._host}:{self._port}')"

    def __str__(self):
        return self.__repr__()

    def __enter__(self):
        self._pending_requests = SingleLane(self._backlog)
        q = queue.Queue()
        self._tasks.append(self._executor.submit(self._open_connections, q))
        n = 0
        while n < self._num_connections:
            z = q.get()
            if z == "OK":
                n += 1
            if isinstance(z, BaseException):
                self._to_shutdown.set()
                concurrent.futures.wait(self._tasks)
                self._executor.shutdown()
                raise z
        logger.info("client %s is ready", self)
        return self

    def __exit__(self, *args, **kwargs):
        self._prepare_shutdown.set()
        t0 = perf_counter()
        while not self._pending_requests.empty():
            if perf_counter() - t0 > self._shutdown_timeout:
                break
            time.sleep(0.1)
        while self._active_requests:
            if perf_counter() - t0 > self._shutdown_timeout:
                break
            time.sleep(0.1)
        self._to_shutdown.set()
        concurrent.futures.wait(self._tasks)
        self._executor.shutdown()
        self._pending_requests = None

    def _open_connections(self, q: queue.Queue):
        async def _keep_sending(writer):
            pending = self._pending_requests
            # This queue is populated by `_enqueue`.
            active = self._active_requests
            encoder = self._encoder
            to_shutdown = self._to_shutdown
            while True:
                try:
                    x, fut = pending.get_nowait()
                except queue.Empty:
                    if to_shutdown.is_set():
                        return
                    await asyncio.sleep(0.0012)
                    # This sleep should be short, but the length
                    # is not critical, because this happens only when
                    # the queue is empty. In a busy application,
                    # the queue should be rarely empty.
                    continue
                req_id = id(fut)
                await write_record(writer, req_id, x, encoder=encoder)
                active[req_id] = fut

        async def _keep_receiving(reader):
            active = self._active_requests
            to_shutdown = self._to_shutdown
            while True:
                try:
                    req_id, data = await read_record(reader, timeout=0.1)
                    req_id = int(req_id)
                except asyncio.TimeoutError:
                    if to_shutdown.is_set():
                        return
                    continue
                # Do not capture `asyncio.IncompleteReadError`;
                # let it stop this function.
                fut = active.pop(req_id)
                if isinstance(data, BaseException):
                    fut.set_exception(data)
                else:
                    fut.set_result(data)

        async def _open_connection(k):
            try:
                if self._path:
                    reader, writer = await open_unix_connection(
                        self._path, timeout=self._connection_timeout
                    )
                else:
                    reader, writer = await open_tcp_connection(
                        self._host, self._port, timeout=self._connection_timeout
                    )
            except Exception as e:
                q.put(e)
                return
            q.put("OK")
            addr = writer.get_extra_info("peername")
            logger.info("connection %d to server %r is openned", k + 1, addr)
            tw = asyncio.create_task(_keep_sending(writer))
            tr = asyncio.create_task(_keep_receiving(reader))
            try:
                await tr
            except asyncio.IncompleteReadError:
                # Propagated from `tr` indicating server has closed the connection.
                tw.cancel()
            else:
                await tw
            writer.close()
            await writer.wait_closed()
            logger.info("connection %d to server %r is closed", k + 1, addr)

        async def _main():
            tasks = [_open_connection(i) for i in range(self._num_connections)]
            await asyncio.gather(*tasks)
            # If any of the tasks raises exception, it will be propagated here.

        asyncio.run(_main())
        # If `_main` raises exception, it will be propagated here,
        # hence stopping this thread.

    def _enqueue(self, path: str, data, *, timeout=None):
        if not self._pending_requests:
            raise Exception(
                "Client is not yet started. Please use it in a context manager"
            )
        if self._prepare_shutdown.is_set() or self._to_shutdown.is_set():
            raise Exception("Client is closed")
        fut = concurrent.futures.Future()
        # `data` is the payload to send.
        # `fut` will hold the corresponding response to be received.
        # Every request will get a response from the server.
        self._pending_requests.put(((path, data), fut), timeout=timeout)
        return fut

    def request(
        self, path: str, data=None, *, enqueue_timeout=None, response_timeout=None
    ):
        """
        This could raise ``concurrent.futures.TimeoutError``.
        That means result is not available in the specified time,
        but the request may have well been sent to the server.
        However the user handles the exception, it will not affect
        the server's response to the request. The user will not be
        able to resume the wait for the result.

        If caller does not need the response, use ``response_timeout=0``.

        In some cases, the request does not need to send data, e.g. if the request
        if for certain info query. In such situations, the corresponding function on
        the server side takes no argument, and in this call to ``request``, ``data`` should be ``None``.

        Example::

            request('/shutdown', response_timeout=0)
        """
        fut = self._enqueue(path, data, timeout=enqueue_timeout)
        if response_timeout is not None and response_timeout <= 0:
            return None
        return fut.result(timeout=response_timeout)

    def stream(
        self,
        path: str,
        data: Iterable,
        *,
        return_x: bool = False,
        return_exceptions: bool = False,
        enqueue_timeout=60,
        response_timeout=60,
    ):
        """
        If ``return_x`` is ``True``, return a stream of ``(x, y)`` tuples,
        where ``x`` is the input data, and ``y`` is a dict with element 'data'.
        If ``return_x`` is ``False``, return a stream of ``y``.
        If ``return_exceptions`` is ``True``, ``y`` could be an Exception object.
        """
        # Refer to `mpserver.Server.stream` for in-code documentation.
        tasks = SingleLane(self._backlog)
        nomore = object()

        def _enqueue():
            en = self._enqueue
            et = enqueue_timeout
            tt = tasks
            Future = concurrent.futures.Future
            to_shutdown = self._to_shutdown
            for x in data:
                try:
                    fut = en(path, x, timeout=et)
                except Exception as e:
                    if return_exceptions:
                        fut = Future()
                        fut.set_exception(e)
                    else:
                        # logger.error("exception '%r' happened for input '%s'", e, x)
                        logger.error(repr(e))
                        raise
                t0 = perf_counter()
                if not put_in_queue(tt, (x, fut, t0), to_shutdown):
                    return
            put_in_queue(tt, nomore, to_shutdown)

        t = self._executor.submit(_enqueue)
        self._tasks.append(t)

        while True:
            try:
                z = tasks.get(timeout=0.1)
            except queue.Empty:
                if t.done():
                    if t.exception():
                        raise t.exception()
                    if not self._to_shutdown.is_set():
                        raise ValueError(
                            f"expecting `self._to_shutdown.is_set()` to be True but got: {self._to_shutdown.is_set()}"
                        )
                    break
                if self._to_shutdown.is_set():
                    break
                continue

            if z is nomore:
                break
            x, fut, t0 = z
            try:
                y = fut.result(timeout=response_timeout - (perf_counter() - t0))
            except Exception as e:
                if return_exceptions:
                    if return_x:
                        yield x, e
                    else:
                        yield e
                else:
                    # logger.error("exception '%r' happened for input '%s'", e, x)
                    logger.error(repr(e))
                    raise
            else:
                if return_x:
                    yield x, y
                else:
                    yield y
