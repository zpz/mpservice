import asyncio
import os

from orjson import loads, dumps

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


async def run_tcp_server(host: str, port: int):
    # If the server runs within a Docker container,
    # `host` should be '0.0.0.0'. Outside of Docker,
    # it should be '127.0.0.1' (I think but did not verify).
    server = await asyncio.start_server(
        take_request, host, port)
    async with server:
        await server.serve_forever()


async def run_unix_client(path: str):
    # `path` must be consistent with that passed to `run_unix_server`.
    reader, writer = await asyncio.open_unix_connection(path)
    await make_request(reader, writer)


async def run_unix_server(path: str):
    # Make sure the socket does not already exist.
    try:
        os.unlink(path)
    except OSError:
        if os.path.exists(path):
            raise

    server = await asyncio.start_unix_server(take_request, path)
    async with server:
        await server.serve_forever()


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


