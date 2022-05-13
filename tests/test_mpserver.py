import asyncio
import time

import pytest
from overrides import overrides

from mpservice.remote_exception import RemoteException
from mpservice.mpserver import (
    Servlet, SequentialServer, EnsembleServer, SimpleServer,
    TimeoutError
)
from mpservice.remote_exception import RemoteException
from mpservice.streamer import Streamer


@pytest.fixture(params=['FastQueue', 'Unique'])
def qtype(request):
    yield request.param


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


@pytest.mark.asyncio
async def test_sequential_server_async(qtype):
    service = SequentialServer(queue_type=qtype)
    service.add_servlet(Double, cpus=[1, 2])
    service.add_servlet(Shift, cpus=[3])
    with service:
        z = await service.async_call(3)
        assert z == 3 * 2 + 3

        x = list(range(10))
        tasks = [service.async_call(v) for v in x]
        y = await asyncio.gather(*tasks)
        assert y == [v * 2 + 3 for v in x]


def test_sequential_server(qtype):
    service = SequentialServer(queue_type=qtype)
    service.add_servlet(Double, cpus=[1, 2])
    service.add_servlet(Shift, cpus=[3])
    with service:
        z = service.call(3)
        assert z == 3 * 2 + 3

        x = list(range(10))
        y = [service.call(v) for v in x]
        assert y == [v * 2 + 3 for v in x]


def test_sequential_batch(qtype):
    service = SequentialServer(queue_type=qtype)
    service.add_servlet(Shift, cpus=[1, 2, 3], batch_size=10, stepsize=4)
    with service:
        z = service.call(3)
        assert z == 3 + 4

        x = list(range(111))
        y = [service.call(v) for v in x]
        assert y == [v + 4 for v in x]


def test_sequential_error(qtype):
    service = SequentialServer(queue_type=qtype)
    service.add_servlet(Double, cpus=[1, 2])
    service.add_servlet(Shift, cpus=[3], stepsize=4)
    with service:
        z = service.call(3)
        assert z == 3 * 2 + 4

        with pytest.raises(RemoteException):
            z = service.call('a')


@pytest.mark.asyncio
async def test_sequential_timeout_async(qtype):
    service = SequentialServer(queue_type=qtype)
    service.add_servlet(Delay)
    with service:
        with pytest.raises(TimeoutError):
            z = await service.async_call(2.2, timeout=1)


def test_sequential_timeout(qtype):
    service = SequentialServer(queue_type=qtype)
    service.add_servlet(Delay)
    with service:
        with pytest.raises(TimeoutError):
            z = service.call(2.2, timeout=1)


def test_sequential_stream(qtype):
    service = SequentialServer(queue_type=qtype)
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


@pytest.mark.asyncio
async def test_ensemble_server_async(qtype):
    service = MyWideServer(queue_type=qtype)

    with service:
        z = await service.async_call('abcde')
        assert z == 'aeaeaeaeae'

        x = ['xyz', 'abc', 'jk', 'opqs']
        tasks = [service.async_call(v) for v in x]
        y = await asyncio.gather(*tasks)
        assert y == ['xzxzxz', 'acacac', 'jkjk', 'osososos']


def test_ensemble_server(qtype):
    service = MyWideServer(queue_type=qtype)

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


@pytest.mark.asyncio
async def test_ensemble_timeout_async(qtype):
    service = YourWideServer(queue_type=qtype)
    with service:
        with pytest.raises(TimeoutError):
            z = await service.async_call(8.2, timeout=1)


def test_ensemble_timeout(qtype):
    service = YourWideServer(queue_type=qtype)
    with service:
        with pytest.raises(TimeoutError):
            z = service.call(8.2, timeout=1)


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


def test_ensemble_stream(qtype):
    service = HisWideServer(queue_type=qtype)
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


def test_simple_server(qtype):
    server = SimpleServer(func1, shift=3, queue_type=qtype)
    with server:
        data = range(1000)
        ss = server.stream(data, return_x=True)
        assert list(ss) == [(x, x + 3) for x in range(1000)]

    server = SimpleServer(func2, batch_size=99, shift=5, queue_type=qtype)
    with server:
        data = range(1000)
        ss = server.stream(data)
        assert list(ss) == [x + 5 for x in range(1000)]
