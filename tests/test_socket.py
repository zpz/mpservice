import asyncio
import multiprocessing
import time

from mpservice.mpserver import ProcessServlet, Server, Worker
from mpservice.socket import SocketApplication, SocketClient, make_server
from zpz.logging import config_logger

mp = multiprocessing.get_context('spawn')


def run_my_server():
    async def double(data):
        await asyncio.sleep(0.01)
        return data * 2

    app = SocketApplication()
    app.add_route('/', double)

    server = make_server(app, path='/tmp/sock_abc')
    asyncio.run(server.serve())


def test_simple():
    server = mp.Process(target=run_my_server)
    server.start()
    with SocketClient(path='/tmp/sock_abc') as client:
        assert client.request('/', 23) == 46
        assert client.request('/', 'abc') == 'abcabc'
        data = range(10)
        for x, y in zip(data, client.stream('/', data)):
            assert y == x * 2
        for x, y in zip(data, client.stream('/', data, return_x=True)):
            assert y == (x, x * 2)
        for x, y in zip(data, client.stream('/', data, return_x=True)):
            assert y == (x, x * 2)
            if x > 5:
                break
        client.request('/shutdown')
    server.join()


class Double(Worker):
    def call(self, x):
        time.sleep(0.01)
        return x * 2


def run_mp_server():

    model = Server(ProcessServlet(Double))
    app = SocketApplication(on_startup=[model.__enter__], on_shutdown=[model.__exit__])
    app.add_route('/', model.async_call)

    config_logger(level='info')  # this is for the server running in another process
    server = make_server(app, path='/tmp/sock_abc')
    asyncio.run(server.serve())


def test_mpserver():
    mp = multiprocessing.get_context('spawn')
    server = mp.Process(target=run_mp_server)
    server.start()
    with SocketClient(path='/tmp/sock_abc') as client:
        assert client.request('/', 23) == 46
        assert client.request('/', 'abc') == 'abcabc'

        data = range(10)
        for x, y in zip(data, client.stream('/', data)):
            assert y == x * 2

        for x, y in zip(data, client.stream('/', data, return_x=True)):
            assert y == (x, x * 2)

        client.request('/shutdown', response_timeout=0)
    server.join()
