import asyncio
import pytest

from mpservice._mpservice import Modelet, ModelService


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
