import asyncio
import concurrent.futures
import logging
import os
import queue
import random
import selectors
import socket
import time
import traceback
from pickle import dumps as pickle_dumps, loads as pickle_loads
from time import perf_counter
from types import SimpleNamespace
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
    return data  # remain bytes


# Our design of a record is laid out this way:
#
#    b'24 pickle\naskadfka23kdkda'
#
# The 'header' part contains lengths (bytes) and
# encoder, ending with '\n'; after that comes the said number
# of bytes, which should be decoded according to the 'encoder'.


async def write_record(writer, request_id, data, *, encoder: str = 'orjson'):
    data_bytes = encode(data, encoder)
    writer.write(f'{request_id} {len(data_bytes)} {encoder}\n'.encode())
    writer.write(data_bytes)
    await writer.drain()


async def read_record(reader):
    # This may raise `asyncio.IncompleteReadError`.
    data = await reader.readuntil(b'\n')
    rid, n, encoder = data[:-1].decode().split()
    n = int(n)
    data = await reader.readexactly(n)
    return rid, decode(data, encoder)


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
                 port: int = None):
        if path:
            # Make sure the socket does not already exist.
            assert not host
            assert not port
            self._socket_type = 'unix'
            self._socket_path = path
        else:
            # If the server runs within a Docker container,
            # `host` should be '0.0.0.0'. Outside of Docker,
            # it should be '127.0.0.1' (I think but did not verify).
            assert port
            if not host:
                host = '0.0.0.0'  # in Docker
            self._socket_type = 'tcp'
            self._host = host
            self._port = int(port)
        self._encoder = 'orjson'  # encoder when sending responses
        self._n_connections = 0
        self._to_shutdown = False
        self.to_shutdown = False

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
        if self._socket_type == 'tcp':
            server_task = asyncio.create_task(
                run_tcp_server(self._handle_connection, self._host, self._port))
        else:
            server_task = asyncio.create_task(
                run_unix_server(self._handle_connection, self._socket_path))
        await self.after_startup()
        logger.info('server %s is ready', self)
        while True:
            if self.to_shutdown:
                await self.before_shutdown()
                server_task.cancel()
                await self.after_shutdown()
                os.unlink(self._socket_path)
                logger.info('server %s is stopped', self)
                break
            await asyncio.sleep(1)

    async def handle_request(self, data):
        # In one connection, "one round of interactions"
        # if for server to receive a request and send back a response.
        # This method handles one such round.
        # Subclass will override this with their interpretation of
        # the request `data` and their response.

        # Simple echo.
        await asyncio.sleep(0.5)
        return data
        # Subclass reimplementation: don't forget `encoder` in this call.

    async def _handle_connection(self, reader, writer):
        # This is called upon a new connection that is openned
        # at the request from a client to the server.
        # This method handles requests in that connection.
        if self._socket_type == 'tcp':
            addr = writer.get_extra_info('peername')
        else:
            addr = writer.get_extra_info('sockname')
        logger.debug('connection %r openned from client', addr)
        self._n_connections += 1
        sem = asyncio.Semaphore(32)
        tasks = set()

        async def _handle_request(request_id, data):
            try:
                z = await self.handle_request(data)
            except Exception as e:
                if not isinstance(e, RemoteException):
                    e = RemoteException(e)
                await write_record(writer, request_id, e, encoder='pickle')
            else:
                await write_record(writer, request_id, z, encoder=self._encoder)

        while True:
            # Infinite loop to handle requests on this connection
            # until the connection is closed by the client.
            try:
                rid, data = await read_record(reader)
            except asyncio.IncompleteReadError:
                # Client has closed the connection.
                break
            async with sem:
                t = asyncio.create_task(_handle_request(rid, data))
                t.add_done_callback(lambda fut: tasks.remove(fut))

        writer.close()
        logger.debug('connection %r closed from client', addr)
        self._n_connections -= 1
        if self._n_connections == 0 and self._to_shutdown:
            # If any one connection has requested server shutdown,
            # then stop server once all connections are closed.
            self.to_shutdown = True


class SocketClient(EnforceOverrides):
    def __init__(self, *,
                 path: str = None,
                 host: str = None,
                 port: int = None,
                 max_connections: int = None,
                 connection_timeout: int = 10,
                 backlog: int = 1024,
                 ):
        '''
        `connection_timeout`: how many seconds to wait for connecting to the server.
        '''
        # Experiments showed `max_connections` can be up to 200.
        # This needs to be improved.
        if path:
            # `path` must be consistent with that passed to `run_unix_server`.
            assert not host
            assert not port
            self._socket_type = 'unix'
            self._socket_path = path
        else:
            # If both client and server run on the same machine
            # in separate Docker containers, `host` should be
            # `mpservice._util.get_docker_host_ip()`.
            assert port
            if not host:
                host = get_docker_host_ip()  # in Docker
            self._socket_type = 'tcp'
            self._host = host
            self._port = int(port)

        self._max_connections = max_connections or MAX_THREADS
        self._connection_timeout = connection_timeout
        self._encoder = 'orjson'  # encoder when sending requests.
        self._to_shutdown = asyncio.Event()
        self._executor = concurrent.futures.ThreadPoolExecutor()
        self._tasks = []
        self._pending_requests = queue.Queue(backlog)
        self._active_requests = {}

    def __enter__(self):
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
            # print(msg)

        self._to_shutdown.set()
        concurrent.futures.wait(self._tasks)
        self._executor.shutdown()

    def _open_connections(self):
        async def _keep_sending(writer):
            inqueue = self._pending_requests
            while True:
                x, fut = inqueue.get()


        async def _open_connection():
            try:
                if self._socket_type == 'tcp':
                    reader, writer = await asyncio.wait_for(
                        asyncio.open_connection(self._host, self._port),
                        self._connection_timeout)
                else:
                    reader, writer = await asyncio.wait_for(
                        asyncio.open_unix_connection(self._socket_path),
                        self._connection_timeout)
            except asyncio.TimeoutError as e:
                print(e)
                raise
            # logger.debug('connection %r openned to server', sock)
            while True:
                in_queue = self._in_queue
                x, fut = in_queue.get()
                await write_record(writer, x)
                resp = await read_record(reader)
                fut.set_result((x, resp))

        async def _main():
            tasks = [_open_connection() for _ in range(self._max_connections)]
            await asyncio.gather(*tasks)

        asyncio.run(_main())

    def _enqueue(self, x):
        fut = concurrent.futures.Future()
        # `x` is the payload to send.
        # `fut` will hold the corresponding response to be received.
        # Every request will get a response from the server.
        self._pending_requests.put((x, fut))
        return fut

    def request(self, data, *, timeout: Optional[Union[int, float]] = None) -> Optional[dict]:
        # If caller does not want result, pass in `timeout=0`.
        fut = self._enqueue(data)
        if timeout is not None and timeout <= 0:
            return
        return fut.result(timeout)[1]
        # `[0]` is the input `data`. In this method, there is no need
        # to return the input, because the caller has it in hand.
        # The return is the dict that is returned at the end
        # of `recv_record_inc`. Same for the method `stream`.
        # This could raise Timeout error. The user would not be able
        # to get the result. Do not cancel the future.

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
