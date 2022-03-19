import asyncio
import multiprocessing
from mpservice.socket import SocketServer, SocketClient, write_record

from overrides import overrides
from zpz.logging import config_logger


class MySocketServer(SocketServer):
    @overrides
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

        client.set_server_option('encoder', 'pickle')

        data = range(10)
        for x, y in zip(data, client.stream(data)):
            assert y == x * 2
        for x, y in zip(data, client.stream(data, return_x=True)):
            assert y == (x, x * 2)

        client.shutdown_server()

    server.join()

