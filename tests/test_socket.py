import asyncio
import multiprocessing
import time
from mpservice.socket import SocketServer, SocketClient, MPSocketServer, write_record
from mpservice.mpserver import SimpleServer

from zpz.logging import config_logger


class MySocketServer(SocketServer):
    async def handle_request(self, data, writer):
        await asyncio.sleep(0.01)
        await write_record(writer, data * 2, encoder=self._encoder)


def run_my_server():
    server = MySocketServer(path='/tmp/sock_abc')
    asyncio.run(server.run())


def test_simple():
    config_logger(level='info')   # this is for the server running in another process
    mp = multiprocessing.get_context('spawn')
    server = mp.Process(target=run_my_server)
    server.start()
    with SocketClient(path='/tmp/sock_abc') as client:
        assert client.request(23) == 46
        assert client.request('abc') == 'abcabc'
        client.shutdown_server()

        data = range(10)
        for x, y in zip(data, client.stream(data)):
            assert y == x * 2
        for x, y in zip(data, client.stream(data, return_x=True)):
            assert y == (x, x * 2)

    server.join()


def double(x):
    time.sleep(0.01)
    return x * 2


def run_mp_server():
    server = MPSocketServer(SimpleServer(double), path='/tmp/sock_abc')
    asyncio.run(server.run())


def test_mp():
    config_logger(level='info')   # this is for the server running in another process
    mp = multiprocessing.get_context('spawn')
    server = mp.Process(target=run_mp_server)
    server.start()
    with SocketClient(path='/tmp/sock_abc') as client:
        assert client.request(23) == 46
        assert client.request('abc') == 'abcabc'

        data = range(10)
        for x, y in zip(data, client.stream(data)):
            assert y == x * 2
        for x, y in zip(data, client.stream(data, return_x=True)):
            assert y == (x, x * 2)

        client.shutdown_server()

    server.join()

