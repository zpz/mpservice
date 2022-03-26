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
    print(1)
    mp = multiprocessing.get_context('spawn')
    print(2)
    server = mp.Process(target=run_my_server)
    print(3)
    server.start()
    print(4)
    with SocketClient(path='/tmp/sock_abc') as client:
        print(5)
        #assert client.request(23)['data'] == 46
        print(6)
        #assert client.request('abc')['data'] == 'abcabc'
        print(7)

        #client.set_server_option('encoder', 'pickle')
        print(8)

        data = range(10)
        #for x, y in zip(data, client.stream(data)):
        #    print(9)
        #    assert y['data'] == x * 2
        #for x, y in zip(data, client.stream(data, return_x=True)):
        #    print(10)
        #    assert (y[0], y[1]['data']) == (x, x * 2)

        print(11)

        client.shutdown_server()
        print(12)

    print(13)

    server.join()
    print(14)

