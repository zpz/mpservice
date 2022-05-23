import asyncio
import time

import pytest

from mpservice.remote_exception import RemoteException
from mpservice.mpserver import (
    ProcessWorker,
    Servlet, Sequential, Ensemble, Server,
    TimeoutError
)
from mpservice.remote_exception import RemoteException
from mpservice.streamer import Streamer


class Double(ProcessWorker):
    def call(self, x):
        return x * 2


class Shift(ProcessWorker):
    def __init__(self, stepsize=3, **kwargs):
        super().__init__(**kwargs)
        self._stepsize = stepsize

    def call(self, x):
        if self.batch_size == 0:
            return x + self._stepsize
        return [_ + self._stepsize for _ in x]


class Square(ProcessWorker):
    def __init__(self):
        super().__init__(batch_size=4)

    def call(self, x):
        return [v*v for v in x]


class Delay(ProcessWorker):
    def call(self, x):
        time.sleep(x)
        return x


def test_basic():
    with Server(Servlet(Double, cpus=[1])) as service:
        z = service.call(3)
        assert z == 3 * 2


@pytest.mark.asyncio
async def test_sequential_server_async():
    with Server(Sequential(
            Servlet(Double, cpus=[1,2]),
            Servlet(Shift, cpus=[3]),
            )) as service:
        z = await service.async_call(3)
        assert z == 3 * 2 + 3

        x = list(range(10))
        tasks = [service.async_call(v) for v in x]
        y = await asyncio.gather(*tasks)
        assert y == [v * 2 + 3 for v in x]


def test_sequential_server():
    with Server(Sequential(
            Servlet(Double, cpus=[1,2]),
            Servlet(Shift, cpus=[3]),
            )) as service:
        z = service.call(3)
        assert z == 3 * 2 + 3

        x = list(range(10))
        y = [service.call(v) for v in x]
        assert y == [v * 2 + 3 for v in x]


def test_sequential_batch():
    with Server(Servlet(Shift, cpus=[1, 2, 3], batch_size=10, stepsize=4)) as service:
        z = service.call(3)
        assert z == 3 + 4

        x = list(range(111))
        y = [service.call(v) for v in x]
        assert y == [v + 4 for v in x]


def test_sequential_error():
    s1 = Servlet(Double, cpus=[1, 2])
    s2 = Servlet(Shift, cpus=[3], stepsize=4)
    with Server(Sequential(s1, s2)) as service:
        z = service.call(3)
        assert z == 3 * 2 + 4

        with pytest.raises(RemoteException):
            z = service.call('a')


@pytest.mark.asyncio
async def test_sequential_timeout_async():
    with Server(Servlet(Delay)) as service:
        with pytest.raises(TimeoutError):
            z = await service.async_call(2.2, timeout=1)


def test_sequential_timeout():
    with Server(Servlet(Delay)) as service:
        with pytest.raises(TimeoutError):
            z = service.call(2.2, timeout=1)


def test_sequential_stream():
    with Server(Servlet(Square, cpus=[1, 2, 3])) as service:
        data = range(100)
        ss = service.stream(data)
        assert list(ss) == [v*v for v in data]

        with Streamer(data) as s:
            s.transform(service.call, concurrency=10)
            assert s.collect() == [v*v for v in data]


class GetHead(ProcessWorker):
    def call(self, x):
        return x[0]


class GetTail(ProcessWorker):
    def call(self, x):
        return x[-1]


class GetLen(ProcessWorker):
    def call(self, x):
        return len(x)


my_wide_server = Ensemble(
    Servlet(GetHead, cpus=[1,2]),
    Servlet(GetTail, cpus=[3]),
    Servlet(GetLen, cpus=[2]),
    post_func=lambda x, y: (y[0] + y[1]) * y[2],
    )


@pytest.mark.asyncio
async def test_ensemble_server_async():
    with Server(my_wide_server) as service:
        z = await service.async_call('abcde')
        assert z == 'aeaeaeaeae'

        x = ['xyz', 'abc', 'jk', 'opqs']
        tasks = [service.async_call(v) for v in x]
        y = await asyncio.gather(*tasks)
        assert y == ['xzxzxz', 'acacac', 'jkjk', 'osososos']


def test_ensemble_server():
    with Server(my_wide_server) as service:
        z = service.call('abcde')
        assert z == 'aeaeaeaeae'

        x = ['xyz', 'abc', 'jk', 'opqs']
        y = [service.call(v) for v in x]
        assert y == ['xzxzxz', 'acacac', 'jkjk', 'osososos']


class AddThree(ProcessWorker):
    def call(self, x):
        return x + 3


your_wide_server = Ensemble(
        Servlet(AddThree, cpus=[1, 2]),
        Servlet(Delay, cpus=[3]),
        post_func=lambda x, y: x + y[0] + y[1]
    )


@pytest.mark.asyncio
async def test_ensemble_timeout_async():
    with Server(your_wide_server) as service:
        with pytest.raises(TimeoutError):
            z = await service.async_call(8.2, timeout=1)


def test_ensemble_timeout():
    with Server(your_wide_server) as service:
        with pytest.raises(TimeoutError):
            z = service.call(8.2, timeout=1)


his_wide_server = Ensemble(
        Servlet(Shift, stepsize=1, cpus=[1], batch_size=0),
        Servlet(Shift, stepsize=3, cpus=[1, 2], batch_size=1),
        Servlet(Shift, stepsize=5, cpus=[0, 3], batch_size=4),
        Servlet(Shift, stepsize=7, cpus=[2]),
        post_func=lambda x, y: [min(y), max(y)],
        )


def test_ensemble_stream():
    with Server(his_wide_server) as service:
        data = range(100)
        ss = service.stream(data)
        assert list(ss) == [[v + 1, v + 7] for v in data]

        with Streamer(data) as s:
            s.transform(service.call, concurrency=10)
            assert s.collect() == [[v + 1, v + 7] for v in data]
