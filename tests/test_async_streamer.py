import asyncio
import math
import random

import pytest

from mpservice.async_streamer import (
    stream, buffer, transform, unordered_transform,
    drain, batch, unbatch, collect,
    Stream,
)


@pytest.mark.asyncio
async def test_stream():
    class A:
        def __init__(self):
            self.k = 0

        async def __anext__(self):
            if self.k < 5:
                self.k += 1
                return self.k
            raise StopAsyncIteration

    class B:
        async def __aiter__(self):
            for x in [1, 2, 3]:
                yield x

    class C:
        def __init__(self):
            self.k = 0

        def __next__(self):
            if self.k < 5:
                self.k += 1
                return self.k
            raise StopIteration

    class D:
        def __iter__(self):
            for x in [1, 2, 3]:
                yield x

    assert [_ async for _ in stream(range(4))] == [0, 1, 2, 3]
    assert [_ async for _ in stream(A())] == [1, 2, 3, 4, 5]
    assert [_ async for _ in stream(B())] == [1, 2, 3]
    assert [_ async for _ in stream(C())] == [1, 2, 3, 4, 5]
    assert [_ async for _ in stream(D())] == [1, 2, 3]
    assert [_ async for _ in stream(['a', 'b', 'c'])] == ['a', 'b', 'c']


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
    s = transform(transform(stream(SYNC_INPUT), f1), mysink)
    n = await drain(s)
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
    s = transform(transform(stream(SYNC_INPUT), f1), mysink)
    n = await drain(s)
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


@pytest.mark.asyncio
async def test_return_ex():

    async def corrupt_data():
        d = [1, 2, 3, 4, 5, 'a', 6, 7]
        for x in d:
            yield x

    async def process(x):
        return x + 2

    with pytest.raises(TypeError):
        z = transform(corrupt_data(), process, workers=2)
        zz = [v async for v in z]
        print(zz)

    z = transform(corrupt_data(), process, workers=2, return_exceptions=True)
    zz = [v async for v in z]
    print(zz)
    assert isinstance(zz[5], TypeError)


@pytest.mark.asyncio
async def test_ignore_ex():
    data = [1, 2, 3, 4, 5, 'a', 6, 7]

    async def corrupt_data():
        for x in data:
            yield x

    async def process(x):
        results.append(x + 2)

    with pytest.raises(TypeError):
        results = []
        await drain(corrupt_data(), process, workers=2)
        print(results)

    results = []
    await drain(transform(corrupt_data(), process, workers=2, return_exceptions=True))
    print(results)
    assert len(results) == len(data) - 1


@pytest.mark.asyncio
async def test_stream_class():
    s = Stream([1, 2, 3])
    assert [_ async for _ in s] == [1, 2, 3]

    x = [0, 1, 2, 3, 'a', ValueError(3), 4, 5]
    s = Stream(x).drop_if(lambda i, x: x == 'a').drop_exceptions()
    assert await collect(s) == [0, 1, 2, 3, 4, 5]

    y = [0, 1, 2, 3, 4, 5, 6]
    s = Stream(y).batch(3)
    ss = [_ async for _ in s]
    assert ss == [[0, 1, 2], [3, 4, 5], [6]]

    s = Stream(y).batch(3).unbatch()
    ss = [_ async for _ in s]
    assert ss == [0, 1, 2, 3, 4, 5, 6]

    async def double(x):
        return x * 2

    s = Stream(y).transform(double)
    ss = [_ async for _ in s]
    assert ss == [0, 2, 4, 6, 8, 10, 12]

    s = await Stream(y).transform(double).collect()
    assert s == [0, 2, 4, 6, 8, 10, 12]

    class Total:
        def __init__(self):
            self.value = 0

        async def __call__(self, x):
            self.value += x

    total = Total()
    s = Stream(y).transform(double).buffer().transform(total).drain()
    await s
    assert total.value == 42


@pytest.mark.asyncio
async def test_peek():
    data = list(range(10))

    n = await Stream(data).peek_at_intervals(3).drain()
    assert n == 10

    n = await Stream(data).peek_at_random(
        0.5, lambda i, x: print(f'--{i}--  {x}')).drain()
    assert n == 10


@pytest.mark.asyncio
async def test_keep():
    data = list(range(10))

    z = await Stream(data).keep_at_intervals(3).collect()
    assert z == [0, 3, 6, 9]

    z = await Stream(data).keep_at_random(0.5).collect()
    print(z)
