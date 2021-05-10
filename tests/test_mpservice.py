import asyncio
import time
import pytest

from mpservice._mpservice import (
    Modelet, ModelService,
    EnqueueTimeout, TotalTimeout,
)


class Scale(Modelet):
    def predict(self, x):
        return x * 2


class Shift(Modelet):
    def predict(self, x):
        return x + 3


class Square(Modelet):
    def __init__(self):
        super().__init__(batch_size=4)

    def predict(self, x):
        return [v*v for v in x]


class Delay(Modelet):
    def predict(self, x):
        time.sleep(x)
        return x


@pytest.mark.asyncio
async def test_service():
    service = ModelService(cpus=[0])
    service.add_modelet(Scale, cpus=[1, 2])
    service.add_modelet(Shift, cpus=[3])
    with service:
        z = await service.a_predict(3)
        assert z == 3 * 2 + 3

        x = list(range(10))
        tasks = [service.a_predict(v) for v in x]
        y = await asyncio.gather(*tasks)
        assert y == [v * 2 + 3 for v in x]


@pytest.mark.asyncio
async def test_batch():
    service = ModelService(cpus=[0])
    service.add_modelet(Square, cpus=[1, 2, 3])
    with service:
        z = await service.a_predict(3)
        assert z == 3 * 3

        x = list(range(100))
        tasks = [service.a_predict(v) for v in x]
        y = await asyncio.gather(*tasks)
        assert y == [v * v for v in x]


@pytest.mark.asyncio
async def test_timeout():
    queue_size = 4

    service = ModelService(cpus=[0], max_queue_size=queue_size)
    service.add_modelet(Delay)
    with service:
        z = await service.a_predict(3)
        assert z == 3

        tasks = [asyncio.create_task(service.a_predict(2, enqueue_timeout=10))
                 for _ in range(queue_size*2)]
        await asyncio.sleep(0.5)

        with pytest.raises(EnqueueTimeout):
            z = await service.a_predict(1.5, enqueue_timeout=0)
        with pytest.raises(EnqueueTimeout):
            z = await service.a_predict(1.5, enqueue_timeout=0.1)

        await asyncio.wait(tasks)

        with pytest.raises(TotalTimeout):
            z = await service.a_predict(8, enqueue_timeout=0, total_timeout=2)
