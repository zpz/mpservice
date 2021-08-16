import asyncio
import concurrent.futures
import time
import pytest

from mpservice.mpserver import (
    Servlet, LongServer, SimpleServer,
    EnqueueTimeout, TotalTimeout, WideServer,
)
from mpservice.streamer import Stream
from mpservice.async_streamer import Stream as AsyncStream


class Scale(Servlet):
    def __call__(self, x):
        return x * 2


class Shift(Servlet):
    def __init__(self, stepsize=3, **kwargs):
        super().__init__(**kwargs)
        self._stepsize = stepsize

    def __call__(self, x):
        if self.batch_size == 0:
            return x + self._stepsize
        return [_ + self._stepsize for _ in x]


class Square(Servlet):
    def __init__(self):
        super().__init__(batch_size=4)

    def __call__(self, x):
        return [v*v for v in x]


class Delay(Servlet):
    def __call__(self, x):
        time.sleep(x)
        return x


@pytest.mark.asyncio
async def test_long_server_async():
    service = LongServer(cpus=[0])
    service.add_servlet(Scale, cpus=[1, 2])
    service.add_servlet(Shift, cpus=[3])
    with service:
        z = await service.async_call(3)
        assert z == 3 * 2 + 3

        x = list(range(10))
        tasks = [service.async_call(v) for v in x]
        y = await asyncio.gather(*tasks)
        assert y == [v * 2 + 3 for v in x]


def test_long_server():
    service = LongServer(cpus=[0])
    service.add_servlet(Scale, cpus=[1, 2])
    service.add_servlet(Shift, cpus=[3])
    with service:
        z = service.call(3)
        assert z == 3 * 2 + 3

        x = list(range(10))
        y = [service.call(v) for v in x]
        assert y == [v * 2 + 3 for v in x]


def test_long_batch():
    service = LongServer(cpus=[0])
    service.add_servlet(Square, cpus=[1, 2, 3])
    with service:
        z = service.call(3)
        assert z == 3 * 3

        x = list(range(100))
        y = [service.call(v) for v in x]
        assert y == [v * v for v in x]


@pytest.mark.asyncio
async def test_long_timeout_async():
    queue_size = 4

    service = LongServer(cpus=[0], max_queue_size=queue_size)
    service.add_servlet(Delay)
    with service:
        z = await service.async_call(3)
        assert z == 3

        tasks = [asyncio.create_task(service.async_call(2, enqueue_timeout=10))
                 for _ in range(queue_size*2)]
        await asyncio.sleep(0.5)

        with pytest.raises(EnqueueTimeout):
            z = await service.async_call(1.5, enqueue_timeout=0)
        with pytest.raises(EnqueueTimeout):
            z = await service.async_call(1.5, enqueue_timeout=0.1)

        await asyncio.wait(tasks)

        with pytest.raises(TotalTimeout):
            z = await service.async_call(8, enqueue_timeout=0, total_timeout=2)


def test_long_timeout():
    queue_size = 4

    service = LongServer(cpus=[0], max_queue_size=queue_size)
    service.add_servlet(Delay)
    with service, concurrent.futures.ThreadPoolExecutor(10) as pool:
        z = service.call(3)
        assert z == 3

        tasks = [pool.submit(service.call, 2, enqueue_timeout=10)
                 for _ in range(queue_size*2)]
        time.sleep(0.5)

        with pytest.raises(EnqueueTimeout):
            z = service.call(1.5, enqueue_timeout=0)
        with pytest.raises(EnqueueTimeout):
            z = service.call(1.5, enqueue_timeout=0.1)

        concurrent.futures.wait(tasks)

        with pytest.raises(TotalTimeout):
            z = service.call(8, enqueue_timeout=0, total_timeout=2)


@pytest.mark.asyncio
async def test_long_stream_async():
    service = LongServer(cpus=[0])
    service.add_servlet(Square, cpus=[1, 2, 3])
    with service:
        data = range(100)
        ss = service.async_stream(data, return_x=True)
        assert await ss.collect() == [(v, v*v) for v in data]

        ss = AsyncStream(data).transform(service.async_call, workers=10)
        assert await ss.collect() == [v*v for v in data]


def test_long_stream():
    service = LongServer(cpus=[0])
    service.add_servlet(Square, cpus=[1, 2, 3])
    with service:
        data = range(100)
        ss = service.stream(data)
        assert ss.collect() == [v*v for v in data]

        ss = Stream(data).transform(service.call, workers=10)
        assert ss.collect() == [v*v for v in data]


class GetHead(Servlet):
    def __call__(self, x):
        return x[0]


class GetTail(Servlet):
    def __call__(self, x):
        return x[-1]


class GetLen(Servlet):
    def __call__(self, x):
        return len(x)


class MyWideServer(WideServer):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.add_servlet(GetHead, cpus=[1, 2])
        self.add_servlet(GetTail, cpus=[3])
        self.add_servlet(GetLen, cpus=[2])

    def ensemble(self, x, results):
        return (results[0] + results[1]) * results[2]


@pytest.mark.asyncio
async def test_wide_server_async():
    service = MyWideServer(cpus=[0])

    with service:
        z = await service.async_call('abcde')
        assert z == 'aeaeaeaeae'

        x = ['xyz', 'abc', 'jk', 'opqs']
        tasks = [service.async_call(v) for v in x]
        y = await asyncio.gather(*tasks)
        assert y == ['xzxzxz', 'acacac', 'jkjk', 'osososos']


def test_wide_server():
    service = MyWideServer(cpus=[0])

    with service:
        z = service.call('abcde')
        assert z == 'aeaeaeaeae'

        x = ['xyz', 'abc', 'jk', 'opqs']
        y = [service.call(v) for v in x]
        assert y == ['xzxzxz', 'acacac', 'jkjk', 'osososos']


class AddThree(Servlet):
    def __call__(self, x):
        return x + 3


class YourWideServer(WideServer):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.add_servlet(AddThree, cpus=[1, 2])
        self.add_servlet(Delay, cpus=[3])

    def ensemble(self, x, results):
        return results[0] + results[1] + x


@pytest.mark.asyncio
async def test_wide_timeout_async():
    queue_size = 4

    service = YourWideServer(cpus=[0], max_queue_size=queue_size)
    with service:
        z = await service.async_call(2)
        assert z == 2 + 3 + 2 + 2

        tasks = [asyncio.create_task(service.async_call(2, enqueue_timeout=10))
                 for _ in range(queue_size*2)]
        await asyncio.sleep(0.5)

        with pytest.raises(EnqueueTimeout):
            z = await service.async_call(1.5, enqueue_timeout=0)
        with pytest.raises(EnqueueTimeout):
            z = await service.async_call(1.5, enqueue_timeout=0.1)

        await asyncio.wait(tasks)

        with pytest.raises(TotalTimeout):
            z = await service.async_call(8, enqueue_timeout=0, total_timeout=2)


def test_wide_timeout():
    queue_size = 4

    service = YourWideServer(cpus=[0], max_queue_size=queue_size)
    with service, concurrent.futures.ThreadPoolExecutor(10) as pool:
        z = service.call(2)
        assert z == 2 + 3 + 2 + 2

        tasks = [pool.submit(service.call, 2, enqueue_timeout=10)
                 for _ in range(queue_size*2)]
        time.sleep(0.5)

        with pytest.raises(EnqueueTimeout):
            z = service.call(1.5, enqueue_timeout=0)
        with pytest.raises(EnqueueTimeout):
            z = service.call(1.5, enqueue_timeout=0.1)

        concurrent.futures.wait(tasks)

        with pytest.raises(TotalTimeout):
            z = service.call(8, enqueue_timeout=0, total_timeout=2)


def test_simple_server():
    def func(x, shift):
        return x + shift

    server = SimpleServer(func, shift=3)
    with server:
        data = range(1000)
        ss = server.stream(data, return_x=True)
        assert ss.collect() == [(x, x + 3) for x in range(1000)]

    def func2(x, shift):
        return [_ + shift for _ in x]

    server = SimpleServer(func2, batch_size=99, shift=5)
    with server:
        data = range(1000)
        ss = server.stream(data)
        assert ss.collect() == [x + 5 for x in range(1000)]


class HisWideServer(WideServer):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.add_servlet(Shift, stepsize=1, cpus=[1])
        self.add_servlet(Shift, stepsize=3, cpus=[1, 2])
        self.add_servlet(Shift, stepsize=5, cpus=[0, 3], batch_size=4)
        self.add_servlet(Shift, stepsize=7, cpus=[2])

    def ensemble(self, x, results: list):
        return [min(results), max(results)]


@pytest.mark.asyncio
async def test_wide_stream_async():
    service = HisWideServer(cpus=[0])
    with service:
        data = range(100)
        ss = service.async_stream(data, return_x=True)
        assert await ss.collect() == [(v, [v + 1, v + 7]) for v in data]

        ss = AsyncStream(data).transform(service.async_call, workers=10)
        assert await ss.collect() == [[v + 1, v + 7] for v in data]


def test_wide_stream():
    service = HisWideServer(cpus=[0])
    with service:
        data = range(100)
        ss = service.stream(data)
        assert ss.collect() == [[v + 1, v + 7] for v in data]

        ss = Stream(data).transform(service.call, workers=10)
        assert ss.collect() == [[v + 1, v + 7] for v in data]
