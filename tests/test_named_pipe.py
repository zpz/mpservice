import concurrent.futures
import os
from mpservice.named_pipe import Server, Client


def _server(path):
    s = Server(path)
    s.send('hello')
    assert s.recv() == 'got it'
    assert s.recv() == {'message': 'shut up'}
    return 'server done'


def _client(path):
    c = Client(path)
    assert c.recv() == 'hello'
    c.send('got it')
    c.send({'message': 'shut up'})
    return 'client done'


def test_basic():
    path = '/tmp/test/namedpipe/abc'
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass
    executor = concurrent.futures.ProcessPoolExecutor()
    p1 = executor.submit(_client, path)
    p2 = executor.submit(_server, path)
    assert p1.result() == 'client done'
    assert p2.result() == 'server done'

