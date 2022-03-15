import asyncio
import logging
import os
from typing import Callable

from orjson import loads, dumps

from .mpserver import MPServer


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


async def make_request(reader, writer):
    data = ['first', 'second', 'third', 'fourth', 'fifth']
    for msg in data:
        await write_record(writer, msg)

    for _ in range(len(data)):
        data = await read_record(reader)
        print(data)

    writer.close()
    await writer.wait_closed()


async def take_request(reader, writer):
    for _ in range(5):
        data = await read_record(reader)
        print('server received', data)
        await write_record(writer, f'{data} {len(data)}')

    writer.close()


async def run_tcp_client(host: str, port: int):
    # If both client and server run on the same machine
    # in separate Docker containers, `host` should be
    # `mpservice._util.get_docker_host_ip()`.
    reader, writer = await asyncio.open_connection(host, port)
    await make_request(reader, writer)


async def run_tcp_server(conn_handler: Callable, host: str, port: int):
    # If the server runs within a Docker container,
    # `host` should be '0.0.0.0'. Outside of Docker,
    # it should be '127.0.0.1' (I think but did not verify).
    server = await asyncio.start_server(conn_handler, host, port)
    async with server:
        await server.serve_forever()


async def run_unix_client(path: str):
    # `path` must be consistent with that passed to `run_unix_server`.
    reader, writer = await asyncio.open_unix_connection(path)
    await make_request(reader, writer)


async def run_unix_server(conn_handler: Callable, path: str):
    # Make sure the socket does not already exist.
    try:
        os.unlink(path)
    except OSError:
        if os.path.exists(path):
            raise

    server = await asyncio.start_unix_server(conn_handler, path)
    async with server:
        await server.serve_forever()


class MPSocketServer:
    def __init__(self,
                 server: MPServer,
                 *,
                 socket_type: str,
                 host: str = None,
                 port: int = None,
                 path: str = None):
        assert socket_type in ('tcp', 'unix')
        self._socket_type = socket_type
        if socket_type == 'tcp':
            if not host:
                host = '0.0.0.0'  # in Docker
            assert port
            self._host = host
            self._port = port
        else:
            assert path
            self._path = path
        self._server = server
        self._server_task = None
        self._enqueue_timeout, self._total_timeout = server._resolve_timeout()
        self._n_connections = 0
        self._to_shutdown = False

    def __repr__(self):
        return f'{self.__class__.__name__}({repr(self._server)})'

    def __str__(self):
        return self.__repr__()

    def run(self):
        self._server.__enter__()
        # TODO: need to start loop.
        if self._socket_type == 'tcp':
            self._server_task = asyncio.create_task(
                run_tcp_server(self._handle_connection, self._host, self._port))
        else:
            self._server_task = asyncio.create_task(
                run_unix_server(self._handle_connection, self._path))
        logger.info('server %s is ready', self)

    def stop(self):
        self._server.__exit__(None, None, None)
        self._server_task.cancel()
        logger.info('server %s is stopped')

    async def _handle_connection(self, reader, writer):
        self._n_connections += 1
        while True:
            try:
                data = await read_record(reader)
            except asyncio.IncompleteReadError:
                # Client has closed the connection.
                break
            if isinstance(data, dict) and 'set_server_option' in data:
                assert len(data) == 1
                opts = data['set_server_option']
                # Currently the only options are timeouts.
                if 'enqueue_timeout' in opts or 'total_timeout' in opts:
                    tt = self._server._resolve_timeout(
                        enqueue_timeout=opts.get('enqueue_timeout'),
                        total_timeout=opts.get('total_timeout'))
                    self._enqueue_timeout, self._total_timeout = tt
                continue
            if isinstance(self, dict) and 'run_server_command' in data:
                assert len(data) == 1
                assert data['run_server_command'] == 'shutdown'
                self._to_shutdown = True
                break
            y = await self._server.async_call(
                data, enqueue_timeout=self._enqueue_timeout,
                total_timeout=self._total_timeout)
            await write_record(writer, y)

        writer.close()
        self._n_connections -= 1
        if self._n_connections == 0:
            self.stop()


if __name__ == '__main__':
    import sys
    from mpservice._util import get_docker_host_ip
    cmd = sys.argv[1]
    print('command:', cmd)
    if cmd == 'server':
        # asyncio.run(run_tcp_server('0.0.0.0', 9898))
        asyncio.run(run_unix_server('./abc'))
    else:
        # asyncio.run(run_tcp_client(get_docker_host_ip(), 9898))
        asyncio.run(run_unix_client('./abc'))


