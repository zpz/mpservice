import asyncio
import multiprocessing
import time

from mpservice.socket import SocketServer, SocketClient
from mpservice.mpserver import MPServer, SimpleServer
from overrides import overrides
from zpz.logging import config_logger


class MySocketServer(SocketServer):
    @overrides
    async def handle_request(self, data):
        data = data['data']
        if data == 'shutdown_server':
            self.to_shutdown = True
            return
        await asyncio.sleep(0.01)
        return data * 2


class MySocketClient(SocketClient):
    def shutdown_server(self):
        self.request('shutdown_server')


def run_my_server():
    config_logger(level='info')   # this is for the server running in another process
    server = MySocketServer(path='/tmp/sock_abc')
    asyncio.run(server.run())


def test_simple():
    mp = multiprocessing.get_context('spawn')
    server = mp.Process(target=run_my_server)
    server.start()
    with MySocketClient(path='/tmp/sock_abc') as client:
        assert client.request(23)['data'] == 46
        assert client.request('abc')['data'] == 'abcabc'
        data = range(10)
        for x, y in zip(data, client.stream(data)):
            assert y['data'] == x * 2
        for x, (yx, yy) in zip(data, client.stream(data, return_x=True)):
            assert (yx, yy['data']) == (x, x * 2)
        client.shutdown_server()
    server.join()


# This is a demo implementation.
# Since this is so simple, user may choose to create their own
# implementation from scratch.
class MPSocketServer(SocketServer):
    def __init__(self, server: MPServer, **kwargs):
        super().__init__(**kwargs)
        self._server = server
        self._enqueue_timeout, self._total_timeout = server._resolve_timeout()

    def __repr__(self):
        return f'{self.__class__.__name__}({repr(self._server)})'

    def __str__(self):
        return self.__repr__()

    @overrides
    async def handle_request(self, data):
        data = data['data']
        if isinstance(data, dict):
            if 'set_server_option' in data:
                if data['set_server_option'] == 'timeout':
                    assert len(data) == 2
                    t1, t2 = data['value']
                    self._enqueue_timeout, self._total_timeout = self._server._resolve_timeout(
                        enqueue_timeout=t1, total_timeout=t2)
                    return
        if data == 'shutdown_server':
            self.to_shutdown = True
            return

        return await self._server.async_call(
            data, enqueue_timeout=self._enqueue_timeout,
            total_timeout=self._total_timeout)

    @overrides
    async def before_startup(self):
        self._server.__enter__()

    @overrides
    async def after_shutdown(self):
        self._server.__exit__(None, None, None)


def double(x):
    time.sleep(0.01)
    return x * 2


def run_mp_server():
    config_logger(level='info')   # this is for the server running in another process
    server = MPSocketServer(SimpleServer(double), path='/tmp/sock_abc')
    asyncio.run(server.run())


def test_mpserver():
    mp = multiprocessing.get_context('spawn')
    server = mp.Process(target=run_mp_server)
    server.start()
    with MySocketClient(path='/tmp/sock_abc') as client:
        assert client.request(23)['data'] == 46
        assert client.request('abc')['data'] == 'abcabc'

        data = range(10)
        for x, y in zip(data, client.stream(data)):
            assert y['data'] == x * 2

        client.request(
            {'set_server_option': 'timeout', 'value': (0.1, 1)})

        for x, y in zip(data, client.stream(data, return_x=True)):
            assert (y[0], y[1]['data']) == (x, x * 2)

        client.shutdown_server()
    server.join()
