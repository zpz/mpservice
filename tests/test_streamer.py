import asyncio
import math
import random

import pytest


from mpservice.streamer import (
    stream, buffer, transform, unordered_transform,
    drain, batch, unbatch)


async def f1(x):
    await asyncio.sleep(random.random() * 0.01)
    return x + 3.8


async def f2(x):
    await asyncio.sleep(random.random() * 0.01)
    return x*2


SYNC_INPUT = list(range(278))


@pytest.mark.asyncio
async def test_transform_1_worker():
    expected = [v + 3.8 for v in SYNC_INPUT]
    s = transform(stream(SYNC_INPUT), f1, workers=1)
    got = [v async for v in s]
    assert got == expected


@pytest.mark.asyncio
async def test_transform():
    expected = [v + 3.8 for v in SYNC_INPUT]
    s = transform(stream(SYNC_INPUT), f1, workers=10)
    got = [v async for v in s]
    assert got == expected


@pytest.mark.asyncio
async def test_unordered_transform():
    expected = [v + 3.8 for v in SYNC_INPUT]
    s = unordered_transform(stream(SYNC_INPUT), f1, workers=5)
    got = [v async for v in s]
    assert got != expected
    assert sorted(got) == expected


def generate_data():
    for x in SYNC_INPUT:
        yield x


async def a_generate_data():
    for x in SYNC_INPUT:
        yield x


@pytest.mark.asyncio
async def test_input():
    expected = [v + 3.8 for v in SYNC_INPUT]

    s = transform(stream(generate_data()), f1)
    got = [v async for v in s]
    assert got == expected

    s = transform(a_generate_data(), f1)
    got = [v async for v in s]
    assert got == expected


@pytest.mark.asyncio
async def test_chain():
    expected = [(v + 3.8) * 2 for v in SYNC_INPUT]
    s = stream(SYNC_INPUT)
    s = transform(s, f1)
    s = transform(s, f2)
    got = [v async for v in s]
    assert got == expected


@pytest.mark.asyncio
async def test_buffer():
    expected = [v + 3.8 for v in SYNC_INPUT]
    s = transform(
        buffer(stream(SYNC_INPUT)),
        f1)
    got = [v async for v in s]
    assert got == expected

    expected = [(v + 3.8) * 2 for v in SYNC_INPUT]
    s = transform(
        transform(
            buffer(stream(SYNC_INPUT)),
            f1),
        f2)
    got = [v async for v in s]
    assert got == expected


@pytest.mark.asyncio
async def test_drain_1_worker():
    class MySink:
        def __init__(self):
            self.result = 0

        async def __call__(self, x):
            await asyncio.sleep(random.random() * 0.01)
            self.result += x * 3

    mysink = MySink()
    s = transform(stream(SYNC_INPUT), f1)
    n = await drain(s, mysink, workers=1)
    assert n == len(SYNC_INPUT)

    got = mysink.result
    expected = sum((v + 3.8) * 3 for v in SYNC_INPUT)
    assert math.isclose(got, expected)


@pytest.mark.asyncio
async def test_drain():
    class MySink:
        def __init__(self):
            self.result = 0

        async def __call__(self, x):
            await asyncio.sleep(random.random() * 0.01)
            self.result += x * 3

    mysink = MySink()
    s = transform(stream(SYNC_INPUT), f1)
    n = await drain(s, mysink)
    assert n == len(SYNC_INPUT)

    got = mysink.result
    expected = sum((v + 3.8) * 3 for v in SYNC_INPUT)
    assert math.isclose(got, expected)


async def f3(x):
    return sum(x)


@pytest.mark.asyncio
async def test_batch():
    data = list(range(11))
    s = transform(batch(stream(data), 3), f3)
    expected = [3, 12, 21, 19]
    got = [v async for v in s]
    assert got == expected


async def f4(x):
    return [x-1, x+1]


@pytest.mark.asyncio
async def test_unbatch():
    data = [1, 2, 3, 4, 5]
    s = unbatch(transform(stream(data), f4))
    expected = [0, 2, 1, 3, 2, 4, 3, 5, 4, 6]
    got = [v async for v in s]
    assert got == expected
