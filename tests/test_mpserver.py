import asyncio
import concurrent.futures
import os
import pickle
import time

import pytest
from overrides import overrides

from mpservice.remote_exception import RemoteException
from mpservice.mpserver import (
    Servlet, SequentialServer, EnsembleServer, SimpleServer,
    EnqueueTimeout, TotalTimeout
)
from mpservice import _mpserver
from mpservice.remote_exception import RemoteException
from mpservice.streamer import Streamer


class Double(Servlet):
    def call(self, x):
        return x * 2


class Shift(Servlet):
    def __init__(self, stepsize=3, **kwargs):
        super().__init__(**kwargs)
        self._stepsize = stepsize

    def call(self, x):
        if self.batch_size == 0:
            return x + self._stepsize
        return [_ + self._stepsize for _ in x]


class Square(Servlet):
    def __init__(self):
        super().__init__(batch_size=4)

    def call(self, x):
        return [v*v for v in x]


class Delay(Servlet):
    def call(self, x):
        time.sleep(x)
        return x


@pytest.mark.parametrize('use_faster_fifo', [True, False])
@pytest.mark.asyncio
async def test_sequential_server_async(use_faster_fifo):
    _mpserver.USE_FASTER_FIFO = use_faster_fifo

    service = SequentialServer(cpus=[0])
    service.add_servlet(Double, cpus=[1, 2])
    service.add_servlet(Shift, cpus=[3])
    with service:
        z = await service.async_call(3)
        assert z == 3 * 2 + 3

        x = list(range(10))
        tasks = [service.async_call(v) for v in x]
        y = await asyncio.gather(*tasks)
        assert y == [v * 2 + 3 for v in x]


@pytest.mark.parametrize('use_faster_fifo', [True, False])
def test_sequential_server(use_faster_fifo):
    _mpserver.USE_FASTER_FIFO = use_faster_fifo

    service = SequentialServer(cpus=[0])
    service.add_servlet(Double, cpus=[1, 2])
    service.add_servlet(Shift, cpus=[3])
    with service:
        z = service.call(3)
        assert z == 3 * 2 + 3

        x = list(range(10))
        y = [service.call(v) for v in x]
        assert y == [v * 2 + 3 for v in x]


@pytest.mark.parametrize('use_faster_fifo', [True, False])
def test_sequential_batch(use_faster_fifo):
    _mpserver.USE_FASTER_FIFO = use_faster_fifo

    service = SequentialServer(cpus=[0])
    service.add_servlet(Shift, cpus=[1, 2, 3], batch_size=10, stepsize=4)
    with service:
        z = service.call(3)
        assert z == 3 + 4

        x = list(range(111))
        y = [service.call(v) for v in x]
        assert y == [v + 4 for v in x]


@pytest.mark.parametrize('use_faster_fifo', [True, False])
def test_sequential_error(use_faster_fifo):
    _mpserver.USE_FASTER_FIFO = use_faster_fifo

    service = SequentialServer(cpus=[0])
    service.add_servlet(Double, cpus=[1, 2])
    service.add_servlet(Shift, cpus=[3], stepsize=4)
    with service:
        z = service.call(3)
        assert z == 3 * 2 + 4

        with pytest.raises(RemoteException):
            z = service.call('a')


@pytest.mark.skip(reason='no longer works; needs a new design')
@pytest.mark.parametrize('use_faster_fifo', [True, False])
@pytest.mark.asyncio
async def test_sequential_timeout_async(use_faster_fifo):
    _mpserver.USE_FASTER_FIFO = use_faster_fifo

    queue_size = 4
    service = SequentialServer(cpus=[0])

    service.add_servlet(Delay)
    with service:
        z = await service.async_call(3.1)
        assert z == 3.1

        tasks = [asyncio.create_task(service.async_call(2.1, enqueue_timeout=6))
                 for _ in range(queue_size*2)]
        await asyncio.sleep(0.5)

        with pytest.raises(EnqueueTimeout):
            z = await service.async_call(1.5, enqueue_timeout=0)
        with pytest.raises(EnqueueTimeout):
            z = await service.async_call(1.5, enqueue_timeout=0.1)

        await asyncio.wait(tasks)

        with pytest.raises(TotalTimeout):
            z = await service.async_call(8.1, enqueue_timeout=0, total_timeout=2)


@pytest.mark.skip(reason='no longer works; needs a new design')
@pytest.mark.parametrize('use_faster_fifo', [True, False])
def test_sequential_timeout(use_faster_fifo):
    _mpserver.USE_FASTER_FIFO = use_faster_fifo

    queue_size = 4
    service = SequentialServer(cpus=[0])

    service.add_servlet(Delay)
    with service, concurrent.futures.ThreadPoolExecutor(10) as pool:
        z = service.call(3.1)
        assert z == 3.1

        tasks = [pool.submit(service.call, 2.1, enqueue_timeout=6)
                 for _ in range(queue_size*2)]
        time.sleep(0.5)

        with pytest.raises(EnqueueTimeout):
            z = service.call(1.5, enqueue_timeout=0)
        with pytest.raises(EnqueueTimeout):
            z = service.call(1.5, enqueue_timeout=0.1)

        concurrent.futures.wait(tasks)

        with pytest.raises(TotalTimeout):
            z = service.call(8.2, enqueue_timeout=0, total_timeout=2)


@pytest.mark.parametrize('use_faster_fifo', [True, False])
def test_sequential_stream(use_faster_fifo):
    _mpserver.USE_FASTER_FIFO = use_faster_fifo

    service = SequentialServer(cpus=[0])
    service.add_servlet(Square, cpus=[1, 2, 3])
    with service:
        data = range(100)
        ss = service.stream(data)
        assert list(ss) == [v*v for v in data]

        with Streamer(data) as s:
            s.transform(service.call, concurrency=10)
            assert s.collect() == [v*v for v in data]


class GetHead(Servlet):
    def call(self, x):
        return x[0]


class GetTail(Servlet):
    def call(self, x):
        return x[-1]


class GetLen(Servlet):
    def call(self, x):
        return len(x)


class MyWideServer(EnsembleServer):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.add_servlet(GetHead, cpus=[1, 2])
        self.add_servlet(GetTail, cpus=[3])
        self.add_servlet(GetLen, cpus=[2])

    @overrides
    def ensemble(self, x, results):
        return (results[0] + results[1]) * results[2]


@pytest.mark.parametrize('use_faster_fifo', [True, False])
@pytest.mark.asyncio
async def test_ensemble_server_async(use_faster_fifo):
    _mpserver.USE_FASTER_FIFO = use_faster_fifo

    service = MyWideServer(cpus=[0])

    with service:
        z = await service.async_call('abcde')
        assert z == 'aeaeaeaeae'

        x = ['xyz', 'abc', 'jk', 'opqs']
        tasks = [service.async_call(v) for v in x]
        y = await asyncio.gather(*tasks)
        assert y == ['xzxzxz', 'acacac', 'jkjk', 'osososos']


@pytest.mark.parametrize('use_faster_fifo', [True, False])
def test_ensemble_server(use_faster_fifo):
    _mpserver.USE_FASTER_FIFO = use_faster_fifo

    service = MyWideServer(cpus=[0])

    with service:
        z = service.call('abcde')
        assert z == 'aeaeaeaeae'

        x = ['xyz', 'abc', 'jk', 'opqs']
        y = [service.call(v) for v in x]
        assert y == ['xzxzxz', 'acacac', 'jkjk', 'osososos']


class AddThree(Servlet):
    def call(self, x):
        return x + 3


class YourWideServer(EnsembleServer):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.add_servlet(AddThree, cpus=[1, 2])
        self.add_servlet(Delay, cpus=[3])

    @overrides
    def ensemble(self, x, results):
        return results[0] + results[1] + x


@pytest.mark.skip(reason='no longer works; needs a new design')
@pytest.mark.parametrize('use_faster_fifo', [True, False])
@pytest.mark.asyncio
async def test_ensemble_timeout_async(use_faster_fifo):
    _mpserver.USE_FASTER_FIFO = use_faster_fifo

    queue_size = 2
    service = YourWideServer(cpus=[0])

    with service:
        z = await service.async_call(2.1)
        assert z == 2.1 + 3 + 2.1 + 2.1

        tasks = [asyncio.create_task(service.async_call(2.2, enqueue_timeout=6))
                 for _ in range(queue_size*2)]
        await asyncio.sleep(0.5)

        with pytest.raises(EnqueueTimeout):
            z = await service.async_call(1.5, enqueue_timeout=0)
        with pytest.raises(EnqueueTimeout):
            z = await service.async_call(1.5, enqueue_timeout=0.1)

        await asyncio.wait(tasks)

        with pytest.raises(TotalTimeout):
            z = await service.async_call(8.1, enqueue_timeout=0, total_timeout=2)


@pytest.mark.skip(reason='no longer works; needs a new design')
@pytest.mark.parametrize('use_faster_fifo', [True, False])
def test_ensemble_timeout(use_faster_fifo):
    _mpserver.USE_FASTER_FIFO = use_faster_fifo

    queue_size = 2
    service = YourWideServer(cpus=[0])

    with service, concurrent.futures.ThreadPoolExecutor(10) as pool:
        z = service.call(2.1)
        assert z == 2.1 + 3 + 2.1 + 2.1

        tasks = [pool.submit(service.call, 3.1, enqueue_timeout=6)
                 for _ in range(queue_size * 2)]
        time.sleep(0.5)

        with pytest.raises(EnqueueTimeout):
            z = service.call(1.5, enqueue_timeout=0)
        with pytest.raises(EnqueueTimeout):
            z = service.call(1.5, enqueue_timeout=0.1)

        concurrent.futures.wait(tasks)

        with pytest.raises(TotalTimeout):
            z = service.call(8.2, enqueue_timeout=0, total_timeout=2)


class HisWideServer(EnsembleServer):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.add_servlet(Shift, stepsize=1, cpus=[1], batch_size=0)
        self.add_servlet(Shift, stepsize=3, cpus=[1, 2], batch_size=1)
        self.add_servlet(Shift, stepsize=5, cpus=[0, 3], batch_size=4)
        self.add_servlet(Shift, stepsize=7, cpus=[2])

    @overrides
    def ensemble(self, x, results: list):
        return [min(results), max(results)]


@pytest.mark.parametrize('use_faster_fifo', [True, False])
def test_ensemble_stream(use_faster_fifo):
    _mpserver.USE_FASTER_FIFO = use_faster_fifo

    service = HisWideServer(cpus=[0])
    with service:
        data = range(100)
        ss = service.stream(data)
        assert list(ss) == [[v + 1, v + 7] for v in data]

        with Streamer(data) as s:
            s.transform(service.call, concurrency=10)
            assert s.collect() == [[v + 1, v + 7] for v in data]


def func1(x, shift):
    return x + shift


def func2(x, shift):
    return [_ + shift for _ in x]


@pytest.mark.parametrize('use_faster_fifo', [True, False])
def test_simple_server(use_faster_fifo):
    _mpserver.USE_FASTER_FIFO = use_faster_fifo

    server = SimpleServer(func1, shift=3)
    with server:
        data = range(1000)
        ss = server.stream(data, return_x=True)
        assert list(ss) == [(x, x + 3) for x in range(1000)]

    server = SimpleServer(func2, batch_size=99, shift=5)
    with server:
        data = range(1000)
        ss = server.stream(data)
        assert list(ss) == [x + 5 for x in range(1000)]
