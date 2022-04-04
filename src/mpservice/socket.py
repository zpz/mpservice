import asyncio
import concurrent.futures
import logging
import os
import queue
import threading
import time
from pickle import dumps as pickle_dumps, loads as pickle_loads
from time import perf_counter
from typing import Iterable, Union, Sequence, Callable, Awaitable, Any

from orjson import loads as orjson_loads, dumps as orjson_dumps  # pylint: disable=no-name-in-module
from overrides import EnforceOverrides

from .util import get_docker_host_ip, is_exception, is_async, MAX_THREADS
from .remote_exception import RemoteException, exit_err_msg
from ._streamer import put_in_queue, get_from_queue

logger = logging.getLogger(__name__)


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
    if encoder == 'orjson':
        return orjson_dumps(data)
    if encoder == 'pickle':
        return pickle_dumps(data)
    if encoder == 'utf8':
        return data.encode('utf8')
    assert encoder == 'none'
    return data   # `data` must be bytes


def decode(data, encoder):
    if encoder == 'orjson':
        return orjson_loads(data)
    if encoder == 'pickle':
        return pickle_loads(data)
    if encoder == 'utf8':
        return data.decode('utf')
    assert encoder == 'none'
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


async def write_record(writer, request_id, data, *, encoder: str = 'orjson'):
    data_bytes = encode(data, encoder)
    writer.write(f'{request_id} {len(data_bytes)} {encoder}\n'.encode())
    writer.write(data_bytes)
    await writer.drain()
    # TODO: add timeout?


async def read_record(reader, *, timeout=None):
    # `timeout` should be `None` or `> 0`.
    # This may raise `asyncio.TimeoutError` (nothing to read at the moment)
    # or `asyncio.IncompleteReadError` (connection has been closed by
    # the other side).
    data = await asyncio.wait_for(reader.readuntil(b'\n'), timeout)
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
        addrs = ', '.join(str(sock.getsockname()) for sock in server.sockets)
        logger.info('serving on %s', addrs)
        await server.serve_forever()


async def run_unix_server(conn_handler: Callable, path: str):
    try:
        os.unlink(path)
    except FileNotFoundError:
        os.makedirs(os.path.dirname(path), exist_ok=True)
    except PermissionError:
        raise
    except OSError:
        if os.path.exists(path):
            raise
    server = await asyncio.start_unix_server(conn_handler, path)
    async with server:
        addrs = ', '.join(str(sock.getsockname()) for sock in server.sockets)
        logger.info('serving on %s', addrs)
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


async def open_unix_connection(path, *, timeout=None):
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


class SocketApplication(EnforceOverrides):
    def __init__(self, *,
                 on_startup: Sequence[Callable] = None,
                 on_shutdown: Sequence[Callable] = None,
                 ):
        self.on_startup = on_startup or []
        self.on_shutdown = on_shutdown or []
        self._routes = {}

    def add_route(self, path: str,
                  route: Union[Callable[[Any], Awaitable[Any]], Callable[[], Awaitable[Any]]]):
        '''
        `route` is an async function that takes a single positional arg,
        and returns a response (which could be `None` if so desired).
        The response should be serializable by the encoder.
        To be safe, return a object of Python native types.
        If exception is raised in this method, appropriate `RemoteException`
        object will be sent in the response.
        The method could also proactively return a `RemoteException` object.
        '''
        self._routes[path] = route

    async def handle_request(self, path: str, data: Any = None):
        if data is None:
            return await self._routes[path]()
        return await self._routes[path](data)


# Usually user should not customize this class.
# Rather, they should design their worker class and "attach" it to
# a SocketApplication instance.
class SocketServer(EnforceOverrides):
    def __init__(self,
                 app: SocketApplication,
                 *,
                 path: str = None,
                 host: str = None,
                 port: int = None,
                 backlog: int = None,
                 shutdown_path: str = '/shutdown',
                 ):
        '''
        `backlog`: max concurrent in-progress requests per connection.
            (Note, the client may open many connections.)
            This "concurrency" is in terms of concurrent calls to
            `handle_request`.
        '''
        self.app = app
        if path:
            assert not host
            assert not port
            self._path = path
            self._host = None
            self._port = None
        else:
            # If the server runs within a Docker container,
            # `host` should be '0.0.0.0'. Outside of Docker,
            # it should be '127.0.0.1' (I think but did not verify).
            assert port
            if not host:
                host = '0.0.0.0'  # in Docker
            self._path = None
            self._host = host
            self._port = int(port)
        self._backlog = backlog or 64
        self._encoder = 'orjson'  # encoder when sending responses
        self._n_connections = 0
        self._shutdown_path = shutdown_path
        self.to_shutdown = False

    def __repr__(self):
        if self._path:
            return f"{self.__class__.__name__}('{self._path}')"
        return f"{self.__class__.__name__}('{self._host}:{self._port}')"

    def __str__(self):
        return self.__repr__()

    async def run(self):
        if self._path:
            server_task = asyncio.create_task(
                run_unix_server(self._handle_connection, self._path))
        else:
            server_task = asyncio.create_task(
                run_tcp_server(self._handle_connection, self._host, self._port))
        for f in self.app.on_startup:
            if is_async(f):
                await f()
            else:
                f()
        logger.info('server %s is ready', self)
        try:
            while True:
                if self.to_shutdown and not self._n_connections:
                    raise Exception('shutdown requested')
                await asyncio.sleep(0.1)
        except BaseException as e:
            # This should take care of keyboard interrupt and such.
            # To be verified.
            for f in self.app.on_shutdown:
                if is_async(f):
                    await f()
                else:
                    f()
            server_task.cancel()
            if self._path:
                os.unlink(self._path)
            logger.info('server %s is stopped', self)
            if str(e) != 'shutdown requested':
                raise

    async def _handle_connection(self, reader, writer):
        # This is called upon a new connection that is openned
        # at the request from a client to the server.
        # This method handles requests in that connection.
        if self._path:
            addr = writer.get_extra_info('sockname')
        else:
            addr = writer.get_extra_info('peername')
        self._n_connections += 1
        logger.info('connection %d is openned from client %r', self._n_connections, addr)
        reqs = asyncio.Queue(self._backlog)

        async def _keep_receiving():
            # Infinite loop to handle requests on this connection
            # until the connection is closed by the client.
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
                    t = asyncio.Future()
                    await reqs.put((req_id, t))
                    self.to_shutdown = True
                    t.set_result(None)
                    break
                else:
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
                    req_id, t = reqs.get_nowait()
                except asyncio.QueueEmpty:
                    if self.to_shutdown:
                        return
                    await asyncio.sleep(0.0023)
                    continue
                try:
                    z = await t
                except Exception as e:
                    if isinstance(e, RemoteException):
                        z = e
                    else:
                        z = RemoteException(e)
                enc = 'pickle' if isinstance(z, RemoteException) else self._encoder
                await write_record(writer, req_id, z, encoder=enc)
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
        logger.info('connection %d from client %r is closed', self._n_connections, addr)
        self._n_connections -= 1


def make_server(app: SocketApplication, **kwargs):
    return SocketServer(app, **kwargs)


def run_app(app, **kwargs):
    server = make_server(app, **kwargs)
    asyncio.run(server.run())


class SocketClient(EnforceOverrides):
    def __init__(self, *,
                 path: str = None,
                 host: str = None,
                 port: int = None,
                 num_connections: int = None,
                 connection_timeout: int = 60,
                 backlog: int = 1024,
                 ):
        '''
        `path`, `host`, `port`: either `path` is given (for Unix socket),
            or `port` (plus optionally `host`) is given (for Tcp socket).
        `connection_timeout`: how many seconds to wait while connecting to the server.
            This is meant for waiting for server to be ready, rather than for
            the action of "connecting" itself (which should be fast).
        `backlog`: size of queue for in-progress requests.
        '''
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
        self._encoder = 'orjson'  # encoder when sending requests.
        self._prepare_shutdown = threading.Event()
        self._to_shutdown = threading.Event()
        self._executor = concurrent.futures.ThreadPoolExecutor()
        self._tasks = []
        self._pending_requests: queue.Queue = None
        self._active_requests = {}
        self._shutdown_timeout = 60

    def __repr__(self):
        if self._path:
            return f"{self.__class__.__name__}('{self._path}')"
        return f"{self.__class__.__name__}('{self._host}:{self._port}')"

    def __str__(self):
        return self.__repr__()

    def __enter__(self):
        self._pending_requests = queue.Queue(self._backlog)
        q = queue.Queue()
        self._tasks.append(self._executor.submit(self._open_connections, q))
        n = 0
        while n < self._num_connections:
            z = q.get()
            if z == 'OK':
                n += 1
            if is_exception(z):
                self._to_shutdown.set()
                concurrent.futures.wait(self._tasks)
                self._executor.shutdown()
                raise z
        logger.info('client %s is ready', self)
        return self

    def __exit__(self, exc_type=None, exc_value=None, exc_traceback=None):
        msg = exit_err_msg(self, exc_type, exc_value, exc_traceback)
        if msg:
            logger.error(msg)

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
                    continue
                req_id = id(fut)
                await write_record(writer, req_id, x, encoder=encoder)
                active[req_id] = fut

        async def _keep_receiving(reader):
            active = self._active_requests
            to_shutdown = self._to_shutdown
            while True:
                try:
                    req_id, data = await read_record(reader, timeout=0.0015)
                    req_id = int(req_id)
                except asyncio.TimeoutError:
                    if to_shutdown.is_set():
                        return
                    continue
                # Do not capture `asyncio.IncompleteReadError`;
                # let it stop this function.
                fut = active.pop(req_id)
                if is_exception(data):
                    fut.set_exception(data)
                else:
                    fut.set_result(data)

        async def _open_connection(k):
            try:
                if self._path:
                    reader, writer = await open_unix_connection(
                        self._path, timeout=self._connection_timeout)
                else:
                    reader, writer = await open_tcp_connection(
                        self._host, self._port, timeout=self._connection_timeout)
            except Exception as e:
                q.put(e)
                return
            q.put('OK')
            addr = writer.get_extra_info('peername')
            logger.info('connection %d to server %r is openned', k + 1, addr)
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
            logger.info('connection %d to server %r is closed', k + 1, addr)

        async def _main():
            tasks = [_open_connection(i) for i in range(self._num_connections)]
            await asyncio.gather(*tasks)
            # If any of the tasks raises exception, it will be propagated here.

        asyncio.run(_main())
        # If `_main` raises exception, it will be propagated here,
        # hence stopping this thread.

    def _enqueue(self, path: str, data, *, timeout=None):
        if not self._pending_requests:
            raise Exception("Client is not yet started. Please use it in a context manager")
        if self._prepare_shutdown.is_set() or self._to_shutdown.is_set():
            raise Exception("Client is closed")
        fut = concurrent.futures.Future()
        # `data` is the payload to send.
        # `fut` will hold the corresponding response to be received.
        # Every request will get a response from the server.
        self._pending_requests.put(((path, data), fut), timeout=timeout)
        return fut

    def request(self, path: str, data=None, *, enqueue_timeout=None, response_timeout=None):
        '''
        This could raise `concurrent.futures.TimeoutError`.
        That means result is not available in the specified time,
        but the request may have well been sent to the server.
        However the user handles the exception, it will not affect
        the server's response to the request. The user will not be
        able to resume the wait for the result.

        If caller does not need the response, use `timeout=0`.

        In some cases, the request does not need to send data, e.g. if the request
        if for certain info query. In such situations, the corresponding function on
        the server side takes no argument, and in this call to `request`, `data` should be `None`.

        Example:

            request('/shutdown', response_timeout=0)
        '''
        fut = self._enqueue(path, data, timeout=enqueue_timeout)
        if response_timeout is not None and response_timeout <= 0:
            return
        return fut.result(timeout=response_timeout)

    def stream(self, path: str, data: Iterable, *,
               return_x: bool = False,
               return_exceptions: bool = False,
               enqueue_timeout=60,
               response_timeout=60,
               ):
        '''
        If `return_x` is `True`, return a stream of `(x, y)` tuples,
        where `x` is the input data, and `y` is a dict with element 'data'.
        If `return_x` is `False`, return a stream of `y`.
        If `return_exceptions` is `True`, `y` could be an Exception object.
        '''
        tasks = queue.Queue(self._backlog)
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
                        logger.error("exception '%r' happened for input '%s'", e, x)
                        raise
                t0 = perf_counter()
                if not put_in_queue(tt, (x, fut, t0), to_shutdown):
                    return
            put_in_queue(tt, nomore, to_shutdown)

        t = self._executor.submit(_enqueue)
        self._tasks.append(t)

        while True:
            z = get_from_queue(tasks, self._to_shutdown, t)
            if z is nomore:
                break
            if z is None:
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
                    logger.error("exception '%r' happened for input '%s'", e, x)
                    raise
            else:
                if return_x:
                    yield x, y
                else:
                    yield y
