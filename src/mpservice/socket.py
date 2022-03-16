import asyncio
import logging
import os
import queue
import selectors
import socket
import time
from typing import Callable

from orjson import loads, dumps

from .mpserver import MPServer
from ._streamer import MAX_THREADS


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


async def write_record(writer, data):
    data_bytes = dumps(data)
    writer.write(str(len(data_bytes)).encode() + b'\n')
    writer.write(data_bytes)
    await writer.drain()


async def read_record(reader):
    # This may raise `asyncio.IncompleteReadError`.
    data = await reader.readuntil(b'\n')
    n = int(data[:-1].decode())
    data = await reader.readexactly(n)
    return loads(data)


def send_record(sock, data):
    data_bytes = dumps(data)
    sock.sendall(str(len(data_bytes)).encode() + b'\n')
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
    size = int(size.decode())
    data = b''
    while True:
        x = sock.recv(size)
        if x == b'':
            return b''
        data += x
        size -= len(x)
        if size == 0:
            break
    return loads(data)


async def run_tcp_server(conn_handler: Callable, host: str, port: int):
    server = await asyncio.start_server(conn_handler, host, port)
    async with server:
        addrs = ', '.join(str(sock.getsockname()) for sock in server.sockets)
        logger.info('serving on %s', addrs)
        await server.serve_forever()


async def run_unix_server(conn_handler: Callable, path: str):
    try:
        os.unlink(path)
    except OSError:
        if os.path.exists(path):
            raise

    server = await asyncio.start_unix_server(conn_handler, path)
    async with server:
        addrs = ', '.join(str(sock.getsockname()) for sock in server.sockets)
        logger.info('serving on %s', addrs)
        await server.serve_forever()


class SocketServer:
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
            self._port = port
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
        # print('received', data)
        await asyncio.sleep(0.8)
        await write_record(writer, data)

    async def _handle_connection(self, reader, writer):
        # This is called upon a new connection that is openned
        # at the request from a client to the server.
        addr = writer.get_extra_info('peername' if self._socket_type == 'tcp' else 'sockname')
        logger.debug('connection %r openned on server', addr)
        self._n_connections += 1
        while True:
            try:
                data = await read_record(reader)
            except asyncio.IncompleteReadError:
                # Client has closed the connection.
                break
            if isinstance(self, dict) and 'run_server_command' in data:
                # Client submitted `{'run_server_command': 'shutdown'}`.
                assert len(data) == 1
                if data['run_server_command'] == 'shutdown':
                    self._to_shutdown = True
                    break
                # Subclass may define and handle other server commands
                # in `handle_request`.
            await self.handle_request(data, writer)

        writer.close()
        logger.debug('connection %r closed on server', addr)
        self._n_connections -= 1
        if self._n_connections == 0 and self._to_shutdown:
            # If any one connection has requested server shutdown,
            # then stop server once all connections are closed.
            self.to_shutdown = True


class SocketClient:
    def __init__(self, *,
                 path: str = None,
                 host: str = None,
                 port: int = None,
                 max_connections: int = None,
                 ):
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
            self._port = port

        self._max_connections = max_connections or MAX_THREADS
        self._sel = selectors.DefaultSelector()
        self._socks = []

    def _open_connection(self):
        if self._socket_type == 'tcp':
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setblocking(False)
            sock.connect_ex((self._host, self._port))
        else:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.setblocking(False)
            sock.connect_ex(self._socket_path)
        events = selectors.EVENT_READ | selectors.EVENT_WRITE
        self._sel.register(sock, events, data=None)
        self._socks.append(sock)
        logger.debug('connection %r openned on client', sock)
        return sock

    def _make_request(self, data, sock):
        send_record(sock, data)
        z = recv_record(sock)
        # print('received', z)
        if z == b'':
            self._sel.unregister(sock)
            sock.close()
        return z

    def request(self, data):
        n = len(data)
        k = 0
        kk = 0
        while True:
            events = self._sel.select(timeout=None)
            for key, mask in events:
                if k < n and (mask & selectors.EVENT_WRITE):
                    # print('sending', data[k])
                    send_record(key.fileobj, data[k])
                    k += 1
                elif kk < n and (mask & selectors.EVENT_READ):
                    z = recv_record(key.fileobj)
                    # print('received', z)
                    kk += 1
            if k == n and kk == n:
                break

        # if not self._socks:
        #     self._open_connection()
        # if len(self._socks) < self._max_workers:
        #     events = self._sel.select(timeout=0)
        #     sock = None
        #     # for key, mask in events:
        #     #     if mask & selectors.EVENT_WRITE:
        #     #         sock = key.fileobj
        #     #         break
        #     if sock is None:
        #         sock = self._open_connection()
        # else:
        #     while True:
        #         events = self._sel.select(timeout=None)
        #         sock = None
        #         for key, mask in events:
        #             if mask & selectors.EVENT_WRITE:
        #                 sock = key.fileobj
        #                 break
        #         if sock is not None:
        #             break
        #         time.sleep(0.01)
        # return self._make_request(data, sock)


    def __enter__(self):
        for _ in range(self._max_connections):
            self._open_connection()

    def __exit__(self, *args, **kwargs):
        for sock in self._socks:
            self._sel.unregister(sock)
            sock.close()
            logger.debug('connection %r closed on client', sock)


class MPSocketServer(SocketServer):
    def __init__(self, server: MPServer, **kwargs):
        super().__init__(**kwargs)
        self._server = server
        self._enqueue_timeout, self._total_timeout = server._resolve_timeout()

    def __repr__(self):
        return f'{self.__class__.__name__}({repr(self._server)})'

    def __str__(self):
        return self.__repr__()

    async def on_startup(self):
        self._server.__enter__()

    async def on_shutdown(self):
        self._server.__exit__(None, None, None)

    async def handle_request(self, data, writer):
        if isinstance(data, dict) and 'set_server_option' in data:
            assert len(data) == 1
            opts = data['set_server_option']
            # Currently the only options are timeouts.
            if 'enqueue_timeout' in opts or 'total_timeout' in opts:
                tt = self._server._resolve_timeout(
                    enqueue_timeout=opts.get('enqueue_timeout'),
                    total_timeout=opts.get('total_timeout'))
                self._enqueue_timeout, self._total_timeout = tt
                return
        y = await self._server.async_call(
            data, enqueue_timeout=self._enqueue_timeout,
            total_timeout=self._total_timeout)
        await write_record(writer, y)


if __name__ == '__main__':
    import sys
    from zpz.logging import config_logger
    from mpservice._util import get_docker_host_ip
    config_logger(level='info')
    cmd = sys.argv[1]
    if cmd == 'server':
        server = SocketServer(path='./abc')
        # server = SocketServer(port=9898)
        asyncio.run(server.run())
    else:
        client = SocketClient(path='./abc', max_workers=100)
        # client = SocketClient(port=9898, max_workers=10)
        import string
        x = string.ascii_letters * 100
        data = [x for _ in range(1000)]
        with client:
            t0 = time.perf_counter()
            # for i in range(10000):
            #    client.request(x)
            client.request(data)
            t1 = time.perf_counter()
            print(t1 - t0)


