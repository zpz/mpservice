import asyncio
import concurrent.futures
import logging
import os
import queue
import threading
import time
import traceback
from pickle import dumps as pickle_dumps, loads as pickle_loads
from time import perf_counter
from typing import Callable, Iterable, Union, Optional

from orjson import loads as orjson_loads, dumps as orjson_dumps  # pylint: disable=no-name-in-module
from overrides import EnforceOverrides

from .util import get_docker_host_ip, FutureIterQueue, MAX_THREADS
from .remote_exception import RemoteException

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


class SocketServer(EnforceOverrides):
    def __init__(self, *,
                 path: str = None,
                 host: str = None,
                 port: int = None,
                 backlog: int = None,
                 ):
        '''
        `backlog`: max concurrent in-progress requests per connection.
            (Note, the client may open many connections.)
        '''
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
        self.to_shutdown = False

    def __repr__(self):
        if self._path:
            return f"{self.__class__.__name__}('{self._path}')"
        return f"{self.__class__.__name__}('{self._host}:{self._port}')"

    def __str__(self):
        return self.__repr__()

    async def before_startup(self):
        pass

    async def after_startup(self):
        pass

    async def before_shutdown(self):
        pass

    async def after_shutdown(self):
        pass

    async def run(self):
        await self.before_startup()
        if self._path:
            server_task = asyncio.create_task(
                run_unix_server(self._handle_connection, self._path))
        else:
            server_task = asyncio.create_task(
                run_tcp_server(self._handle_connection, self._host, self._port))
        await self.after_startup()
        logger.info('server %s is ready', self)
        while True:
            if self.to_shutdown and not self._n_connections:
                await self.before_shutdown()
                server_task.cancel()
                await self.after_shutdown()
                if self._path:
                    os.unlink(self._path)
                logger.info('server %s is stopped', self)
                break
            await asyncio.sleep(1)

        # Subclasses of `SocketServer` and `SocketClient` for a particular
        # application should design a message convention for the client
        # to tell the server to shut down, and for the server to act
        # accordingly.

    async def handle_request(self, data):
        # Subclass should override this method to do whatever they need to do
        # about the request data `data` and return the response.
        # The response should be serializable by the encoder.
        # To be safe, return a object of Python native types.
        # If `None` is returned, the response will be "OK".
        # If exception is raised in this method, appropriate `RemoteException`
        # object will be sent in the response.
        # The method could also proactively return a `RemoteException` object.

        # Simple echo.
        await asyncio.sleep(0.5)
        return data
        # Subclass reimplementation: don't forget `encoder` in this call.

    async def _handle_connection(self, reader, writer):
        # This is called upon a new connection that is openned
        # at the request from a client to the server.
        # This method handles requests in that connection.
        if self._path:
            addr = writer.get_extra_info('sockname')
        else:
            addr = writer.get_extra_info('peername')
        self._n_connections += 1
        logger.info('connection %d openned from client %r', self._n_connections, addr)
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
                t = asyncio.create_task(self.handle_request(data))
                await reqs.put((req_id, t))
                # The queue size will restrict how many concurrent calls
                # to `handle_request` can be in progress.

        async def _keep_responding():
            while True:
                try:
                    req_id, t = reqs.get_nowait()
                except asyncio.QueueEmpty:
                    if self.to_shutdown:
                        return
                    await asyncio.sleep(0.005)
                    continue
                try:
                    z = await t
                    if z is None:
                        z = 'OK'
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
        logger.debug('connection %r closed from client', addr)
        self._n_connections -= 1


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
        `backlog`: queue size for in-progress requests.
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
        self._tasks.append(self._executor.submit(self._open_connections))
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        if exc_type:
            msg = "Exiting {} with exception: {}\n{}".format(
                self.__class__.__name__, exc_type, exc_value,
            )
            if isinstance(exc_value, RemoteException):
                msg = "{}\n\n{}\n\n{}".format(
                    msg, exc_value.format(),
                    "The above exception was the direct cause of the following exception:")
            msg = f"{msg}\n\n{''.join(traceback.format_tb(exc_traceback))}"
            logger.error(msg)

        print('client exit 1')
        self._prepare_shutdown.set()
        t0 = perf_counter()
        print('client exit 2')
        while not self._pending_requests.empty():
            if perf_counter() - t0 > self._shutdown_timeout:
                break
            time.sleep(0.1)
        print('client exit 3')
        while self._active_requests:
            if perf_counter() - t0 > self._shutdown_timeout:
                break
            time.sleep(0.1)
        print('client exit 4')
        self._to_shutdown.set()
        concurrent.futures.wait(self._tasks)
        print('client exit 5')
        self._executor.shutdown()
        self._pending_requests = None
        print('client exit 6')

    def _open_connections(self):
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
                active[req_id] = (x, fut)

        async def _keep_receiving(reader):
            active = self._active_requests
            to_shutdown = self._to_shutdown
            while True:
                try:
                    req_id, data = await read_record(reader, timeout=0.005)
                    req_id = int(req_id)
                except asyncio.TimeoutError:
                    if to_shutdown.is_set():
                        return
                    continue
                # Do not capture `asyncio.IncompleteReadError`;
                # let it stop this function.
                x, fut = active[req_id]
                fut.set_result((x, data))

        async def _open_connection(k):
            t0 = perf_counter()
            while True:
                try:
                    if self._path:
                        reader, writer = await asyncio.open_unix_connection(self._path)
                    else:
                        reader, writer = await asyncio.open_connection(self._host, self._port)
                    addr = writer.get_extra_info('peername')
                    logger.info('connection %d openned to server %r', k+1, addr)
                    break
                except ConnectionRefusedError:
                    if perf_counter() - t0 > self._connection_timeout:
                        raise
                    await asyncio.sleep(0.1)
            tw = asyncio.create_task(_keep_sending(writer))
            tr = asyncio.create_task(_keep_receiving(reader))
            try:
                await asyncio.gather(tr, tw)
            except asyncio.IncompleteReadError:
                # Propagated from `tr` indicating server has closed the connection.
                tw.cancel()

        async def _main():
            tasks = [_open_connection(i) for i in range(self._num_connections)]
            await asyncio.gather(*tasks)
            # If any of the tasks raises exception, it will be propagated here.

        asyncio.run(_main())
        # If `_main` raises exception, it will be propagated here,
        # hence stopping this thread.

    def _enqueue(self, x):
        if not self._pending_requests:
            raise Exception("Client is not yet started. Please use it in a context manager")
        if self._prepare_shutdown.is_set() or self._to_shutdown.is_set():
            raise Exception("Client is closed")
        fut = concurrent.futures.Future()
        # `x` is the payload to send.
        # `fut` will hold the corresponding response to be received.
        # Every request will get a response from the server.
        self._pending_requests.put((x, fut))
        return fut

    def request(self, data, *, timeout: Optional[Union[int, float]] = None) -> Optional[dict]:
        '''
        Return the dict that is returned at the end
        of `recv_record_inc`. Same for the method `stream`.

        This could raise `concurrent.futures.TimeoutError`.
        That means result is not available in the specified time,
        but the request may have well been sent to the server.
        However the user handles the exception, it will not affect
        the server's response to the request. The user will not be
        able to resume the wait for the result.
        '''
        fut = self._enqueue(data)
        if timeout is not None and timeout <= 0:
            return
        return fut.result(timeout)[1]
        # `[0]` is the input `data`. In this method, there is no need
        # to return the input, because the caller has it in hand.

    def stream(self, data: Iterable, *,
               return_x: bool = False,
               return_exceptions: bool = False,
               ):
        results = FutureIterQueue(maxsize=self._backlog,
                                  return_x=return_x,
                                  return_exceptions=return_exceptions)

        def enqueue():
            en = self._enqueue
            data_in = data
            fut_out = results
            for x in data_in:
                fut = en(x)
                fut_out.put(fut)
            fut_out.close()

        self._tasks.append(self._executor.submit(enqueue))
        return results
