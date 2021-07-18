import asyncio
import time
import pytest

from mpservice._mpservice import (
    Servlet, Server,
    EnqueueTimeout, TotalTimeout,
)


class Scale(Servlet):
    def __call__(self, x):
        return x * 2


class Shift(Servlet):
    def __call__(self, x):
        return x + 3


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
async def test_service():
    service = Server(cpus=[0])
    service.add_servlet(Scale, cpus=[1, 2])
    service.add_servlet(Shift, cpus=[3])
    with service:
        z = await service(3)
        assert z == 3 * 2 + 3

        x = list(range(10))
        tasks = [service(v) for v in x]
        y = await asyncio.gather(*tasks)
        assert y == [v * 2 + 3 for v in x]


@pytest.mark.asyncio
async def test_batch():
    service = Server(cpus=[0])
    service.add_servlet(Square, cpus=[1, 2, 3])
    with service:
        z = await service(3)
        assert z == 3 * 3

        x = list(range(100))
        tasks = [service(v) for v in x]
        y = await asyncio.gather(*tasks)
        assert y == [v * v for v in x]


@pytest.mark.asyncio
async def test_timeout():
    queue_size = 4

    service = Server(cpus=[0], max_queue_size=queue_size)
    service.add_servlet(Delay)
    with service:
        z = await service(3)
        assert z == 3

        tasks = [asyncio.create_task(service(2, enqueue_timeout=10))
                 for _ in range(queue_size*2)]
        await asyncio.sleep(0.5)

        with pytest.raises(EnqueueTimeout):
            z = await service(1.5, enqueue_timeout=0)
        with pytest.raises(EnqueueTimeout):
            z = await service(1.5, enqueue_timeout=0.1)

        await asyncio.wait(tasks)

        with pytest.raises(TotalTimeout):
            z = await service(8, enqueue_timeout=0, total_timeout=2)
