import asyncio
import concurrent.futures
import logging
import os
import queue
import random
import selectors
import socket
import time
from pickle import dumps as pickle_dumps, loads as pickle_loads
from types import SimpleNamespace
from typing import Callable, Iterable

from orjson import loads as orjson_loads, dumps as orjson_dumps  # pylint: disable=no-name-in-module
from overrides import EnforceOverrides

from .util import get_docker_host_ip, FutureIterQueue, MAX_THREADS


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


async def write_record(writer, data, *, encoder: str = 'orjson'):
    data_bytes = encode(data, encoder)
    writer.write(f'{len(data_bytes)} {encoder}\n'.encode())
    writer.write(data_bytes)
    await writer.drain()


async def read_record(reader):
    # This may raise `asyncio.IncompleteReadError`.
    data = await reader.readuntil(b'\n')
    n, encoder = data[:-1].decode().split()
    n = int(n)
    data = await reader.readexactly(n)
    return decode(data, encoder)


def send_record(sock, data, *, encoder: str = 'orjson'):
    data_bytes = encode(data, encoder)
    sock.sendall(f'{len(data_bytes)} {encoder}\n'.encode())
    sock.sendall(data_bytes)


def recv_record(sock):
    # Returning `b''` indicates connection has been closed.
    size = b''
    while True:
        x = sock.recv(1)
        if x == b'':
            return b''
        if x == b'\n':
            break
        size += x
    size, encoder = size.decode().split()
    size = int(size)
    data = b''
    while True:
        x = sock.recv(size)
        if x == b'':
            return b''
        data += x
        size -= len(x)
        if size == 0:
            break
    return decode(data, encoder)


def recv_record_inc(sock, sock_data):
    # Incrementally read one record in multiple calls to this function.
    # When `selectors.select` says a socket is ready for read, it must
    # have something. But after the first read, there is no guarantee
    # there is more available at the moment. When this happens, it's not
    # that `socket.recv()` will get more after some waiting---things
    # tend to lock up (I don't know whether it *always* locks up).
    # That's why we need to do only one read, upon detection by
    # `selectors.select`.

    # Returning `b''` indicates connection has been closed.
    if sock_data.n is None:
        # Still reading the header part.

        # TODO: if we read up to 2 bytes, there may be slight
        # performance gain. But it gets a little tricky if
        # the length of a record can be 0.
        x = sock.recv(1)

        if x == b'':
            return x
        if x == b'\n':
            # Header part is complete.
            size, encoder = sock_data.d.decode().split()
            sock_data.n = int(size)   # length of body in bytes
            sock_data.e = encoder
            sock_data.d = b''
            return

        # More bytes for header; not finished yet.
        sock_data.d += x
        return

    k = sock_data.n - len(sock_data.d)
    x = sock.recv(k)
    if x == b'':
        return x
    sock_data.d += x
    if len(x) < k:
        # Added some more bytes to body; not done yet.
        return

    # Body part is finished. This record is complete.
    z = decode(sock_data.d, sock_data.e)
    sock_data.d = b''
    sock_data.n = None
    sock_data.e = None
    return z  # This is the Python value of this record.


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

    async def handle_request(self, data, writer):
        # In one connection, "one round of interactions"
        # if for server to receive a request and send back a response.
        # This method handles one such round.
        # Subclass will override this with their interpretation of
        # the request `data` and their response.

        # Simple echo.
        await asyncio.sleep(0.5)
        await write_record(writer, data, encoder=self._encoder)
        # Subclass reimplementation: don't forget `encoder` in this call.

        # In case of exception, if one wants to pass the exception object
        # to the client, send `RemoteException(e)` with `encoder='pickle'`.

    async def set_server_option(self, name: str, value) -> None:
        if name == 'encoder':
            self._encoder = value
            return
        raise ValueError(f"unknown option '{name}'")

    async def get_server_info(self, name: str, writer) -> None:
        # Subclass should override and implement server info entries as needed.
        await write_record(writer, f'unknown info item `{name}`')

    async def run_server_command(self, name: str, *args, **kwargs) -> None:
        if name == 'shutdown':
            assert not args
            assert not kwargs
            self._to_shutdown = True
            return
        raise ValueError(f"unknown command '{name}'")

    async def _handle_connection(self, reader, writer):
        # This is called upon a new connection that is openned
        # at the request from a client to the server.
        if self._socket_type == 'tcp':
            addr = writer.get_extra_info('peername')
        else:
            addr = writer.get_extra_info('sockname')
        logger.debug('connection %r openned from client', addr)
        self._n_connections += 1
        while True:
            # Infinite loop to handle requests on this connection
            # until the connection is closed by the client.
            try:
                data = await read_record(reader)
            except asyncio.IncompleteReadError:
                # Client has closed the connection.
                break

            if isinstance(data, dict):
                if 'run_server_command' in data:
                    await self.run_server_command(
                        data['run_server_command'],
                        *data['args'], **data['kwargs'])
                    continue
                elif 'set_server_option' in data:
                    await self.set_server_option(
                        data['set_server_option'], data['value'])
                    continue
                elif 'get_server_info' in data:
                    assert len(data) == 1
                    await self.get_server_info(data['get_server_info'], writer)
                    continue

            await self.handle_request(data, writer)

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

        self._backlog = backlog
        self._max_connections = max_connections or MAX_THREADS
        self._connection_timeout = connection_timeout
        self._encoder = 'orjson'  # encoder when sending requests.
        self._to_shutdown = False
        self._in_queue = None
        self._sel = None
        self._socks = set()
        self._executor = None
        self._tasks = []

    def __enter__(self):
        self._in_queue = queue.Queue(self._backlog)
        self._sel = selectors.DefaultSelector()
        self._executor = concurrent.futures.ThreadPoolExecutor()
        for _ in range(self._max_connections):
            self._open_connection()
        self._tasks.append(self._executor.submit(self._send_recv))
        return self

    def __exit__(self, *args, **kwargs):
        if self._socks:
            self._to_shutdown = True
            concurrent.futures.wait(self._tasks)
            self._executor.shutdown()
            for sock in self._socks:
                self._sel.unregister(sock)
                sock.close()
                logger.debug('connection %r closed at client', sock)
            self._sel.close()
            self._socks = set()

    def _open_connection(self):
        if self._socket_type == 'tcp':
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setblocking(False)
            t0 = time.perf_counter()
            while True:
                status = sock.connect_ex((self._host, self._port))
                if status == 0:
                    break
                if time.perf_counter() - t0 > self._connection_timeout:
                    raise ConnectionError('failed to connect to server')
                time.sleep(random.uniform(0.2, 2))
        else:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.setblocking(False)
            t0 = time.perf_counter()
            while True:
                status = sock.connect_ex(self._socket_path)
                if status == 0:
                    break
                if time.perf_counter() - t0 > self._connection_timeout:
                    raise ConnectionError('failed to connect to server')
                time.sleep(random.uniform(0.2, 2))
        # To set buffer size, use `socket.setsockopt`.
        events = selectors.EVENT_READ | selectors.EVENT_WRITE
        self._sel.register(
            sock, events,
            data=SimpleNamespace(
                q=queue.Queue(512),  # input-data and future
                d=b'',               # bytes read so far for the next record
                n=None,              # length of the next record
                e=None,              # encoder of the next send_record
                # Once `l` and `e` have been resolved, `d` will be reset
                # to exclude the bytes for `l` and `e`.
            )
        )
        self._socks.add(sock)
        logger.debug('connection %r openned to server', sock)

    def _enqueue(self, x, *, expect_response: bool = True):
        if expect_response:
            fut = concurrent.futures.Future()
        else:
            fut = None
        self._in_queue.put((x, fut))
        return fut

    def _send_recv(self):
        # TODO: consider doing reading and writing in separate threads.
        data_in = self._in_queue
        sel = self._sel
        socks = self._socks
        while True:
            for key, mask in sel.select(timeout=None):
                sock = key.fileobj
                if mask & selectors.EVENT_READ:
                    z = recv_record_inc(sock, key.data)
                    if z == b'':
                        sel.unregister(sock)
                        sock.close()
                        socks.remove(sock)
                        continue
                    if z is None:
                        continue
                    x, fut = key.data.q.get()
                    fut.set_result((x, z))
                    continue
                if mask & selectors.EVENT_WRITE:
                    try:
                        x, fut = data_in.get_nowait()
                    except queue.Empty:
                        pass
                    else:
                        send_record(sock, x, encoder=self._encoder)
                        if fut is not None:
                            key.data.q.put((x, fut))
            if self._to_shutdown:
                break

    def request(self, data, *, wait_for_response: bool = True):
        fut = self._enqueue(data, expect_response=wait_for_response)
        if wait_for_response:
            return fut.result()[1]

    def set_server_option(self, name: str, value):
        self.request({'set_server_option': name, 'value': value},
                     wait_for_response=False)

    def get_server_info(self, name: str):
        return self.request({'get_server_info': name})

    def run_server_command(self, name: str, *args, **kwargs):
        self.request({'run_server_command': name, 'args': args, 'kwargs': kwargs},
                     wait_for_response=False)

    def shutdown_server(self):
        self.run_server_command('shutdown')

    def stream(self, data: Iterable, *,
               return_x: bool = False,
               return_exceptions: bool = False):
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
