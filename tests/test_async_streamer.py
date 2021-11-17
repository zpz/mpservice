import asyncio
import math
import random
import time

import pytest

from mpservice._async_streamer import Stream
from mpservice._streamer import _default_peek_func


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

    assert await Stream(range(4)).collect() == [0, 1, 2, 3]
    assert await Stream(A()).collect() == [1, 2, 3, 4, 5]
    assert await Stream(B()).collect() == [1, 2, 3]
    assert await Stream(C()).collect() == [1, 2, 3, 4, 5]
    assert await Stream(D()).collect() == [1, 2, 3]
    assert await Stream(['a', 'b', 'c']).collect() == ['a', 'b', 'c']


@pytest.mark.asyncio
async def test_batch():
    s = Stream(range(11))
    assert await s.batch(3).collect() == [
        [0, 1, 2], [3, 4, 5], [6, 7, 8], [9, 10]]

    assert await s.collect() == []

    s = Stream(list(range(11)))
    assert await s.batch(3).unbatch().collect() == list(range(11))
    assert await s.collect() == []


@pytest.mark.asyncio
async def test_buffer():
    s = Stream(range(11))
    assert await s.buffer(5).collect() == list(range(11))
    s = Stream(range(11))
    assert await s.buffer(20).collect() == list(range(11))


@pytest.mark.asyncio
async def test_buffer_batch():
    s = Stream(range(19)).buffer(10).batch(5)
    n = await s.unbatch().log_every_nth(1).drain()
    assert n == 19


@pytest.mark.asyncio
async def test_drop():
    data = [0, 1, 2, 'a', 4, ValueError(8), 6, 7]

    s = Stream(data)
    assert await s.drop_exceptions().collect() == [
        0, 1, 2, 'a', 4, 6, 7]

    s = Stream(data)
    assert await s.drop_exceptions().drop_if(
        lambda i, x: isinstance(x, str)).collect() == [
        0, 1, 2, 4, 6, 7]

    s = Stream(data)
    assert await s.drop_first_n(6).collect() == [6, 7]

    assert await Stream((2, 3, 1, 5, 4, 7)).drop_if(
        lambda i, x: x > i).collect() == [1, 4]


@pytest.mark.asyncio
async def test_keep():
    data = [0, 1, 2, 3, 'a', 5]

    s = Stream(data)
    assert await s.keep_if(lambda i, x: isinstance(x, int)).collect() == [0, 1, 2, 3, 5]

    s = Stream(data)
    assert await s.keep_every_nth(2).collect() == [0, 2, 'a']

    s = Stream(data)
    assert await s.head(3).collect() == [0, 1, 2]

    s = Stream(data)
    assert await s.head(4).drop_first_n(3).collect() == [3]

    s = Stream(data)
    assert await s.drop_first_n(3).head(1).collect() == [3]

    s = Stream(data)
    ss = await s.keep_random(0.5).collect()
    print(ss)
    assert 0 <= len(ss) < len(data)


@pytest.mark.asyncio
async def test_peek():
    # The main point of this test is in checking the printout.

    data = list(range(10))

    n = await Stream(data).peek_every_nth(3).drain()
    assert n == 10

    n = await Stream(data).peek_random(
        0.5, lambda i, x: print(f'--{i}--  {x}')).drain()
    assert n == 10

    await Stream(data).log_every_nth(4).drain()

    def peek_func(i, x):
        if 4 < i < 7:
            _default_peek_func(i, x)

    await Stream(data).peek(peek_func).drain()

    await Stream(data).log_every_nth(4).drain()


@pytest.mark.asyncio
async def test_drain():
    z = await Stream(range(10)).drain()
    assert z == 10

    z = await Stream(range(10)).log_every_nth(3).drain()
    assert z == 10

    z = await Stream((1, 2, ValueError(3), 5, RuntimeError, 9)).drain()
    assert z == (6, 2)


@pytest.mark.asyncio
async def test_transform():

    async def f1(x):
        return x + 3.8

    async def f2(x):
        return x*2

    SYNC_INPUT = list(range(278))

    expected = [v + 3.8 for v in SYNC_INPUT]
    s = Stream(SYNC_INPUT).transform(f1).collect()
    assert await s == expected

    expected = [(v + 3.8) * 2 for v in SYNC_INPUT]
    s = Stream(SYNC_INPUT).transform(f1).transform(f2)
    assert await s.collect() == expected

    class MySink:
        def __init__(self):
            self.result = 0

        async def __call__(self, x):
            self.result += x * 3

    mysink = MySink()
    s = Stream(SYNC_INPUT).transform(f1).transform(mysink)
    n = await s.drain()
    assert n == len(SYNC_INPUT)

    got = mysink.result
    expected = sum((v + 3.8) * 3 for v in SYNC_INPUT)
    assert math.isclose(got, expected)


@pytest.mark.asyncio
async def test_transform_sync():

    def f1(x):
        return x + 3.8

    def f2(x):
        return x*2

    SYNC_INPUT = list(range(278))

    expected = [v + 3.8 for v in SYNC_INPUT]
    s = Stream(SYNC_INPUT).transform(f1).collect()
    assert await s == expected

    expected = [(v + 3.8) * 2 for v in SYNC_INPUT]
    s = Stream(SYNC_INPUT).transform(f1).transform(f2)
    assert await s.collect() == expected

    class MySink:
        def __init__(self):
            self.result = 0

        def __call__(self, x):
            self.result += x * 3

    mysink = MySink()
    s = Stream(SYNC_INPUT).transform(f1).transform(mysink)
    n = await s.drain()
    assert n == len(SYNC_INPUT)

    got = mysink.result
    expected = sum((v + 3.8) * 3 for v in SYNC_INPUT)
    assert math.isclose(got, expected)


@pytest.mark.asyncio
async def test_concurrent_transform():

    async def f1(x):
        await asyncio.sleep(random.random() * 0.01)
        return x + 3.8

    async def f2(x):
        await asyncio.sleep(random.random() * 0.01)
        return x*2

    SYNC_INPUT = list(range(278))

    expected = [v + 3.8 for v in SYNC_INPUT]
    s = Stream(SYNC_INPUT).transform(f1, workers=1)
    got = [v async for v in s]
    assert got == expected

    s = Stream(SYNC_INPUT).transform(f1, workers=10).collect()
    assert await s == expected

    s = Stream(SYNC_INPUT).transform(f1, workers='max').collect()
    assert await s == expected

    expected = [(v + 3.8) * 2 for v in SYNC_INPUT]
    s = Stream(SYNC_INPUT).transform(f1).transform(f2)
    assert await s.collect() == expected

    class MySink:
        def __init__(self):
            self.result = 0

        async def __call__(self, x):
            await asyncio.sleep(random.random() * 0.01)
            self.result += x * 3

    mysink = MySink()
    s = Stream(SYNC_INPUT).transform(f1).transform(mysink)
    n = await s.drain()
    assert n == len(SYNC_INPUT)

    got = mysink.result
    expected = sum((v + 3.8) * 3 for v in SYNC_INPUT)
    assert math.isclose(got, expected)


@pytest.mark.asyncio
async def test_concurrent_transform_sync():
    def f1(x):
        time.sleep(random.random() * 0.01)
        return x + 3.8

    def f2(x):
        time.sleep(random.random() * 0.01)
        return x*2

    SYNC_INPUT = list(range(278))

    expected = [v + 3.8 for v in SYNC_INPUT]
    s = Stream(SYNC_INPUT).transform(f1, workers=1)
    got = [v async for v in s]
    assert got == expected

    s = Stream(SYNC_INPUT).transform(f1, workers=10).collect()
    assert await s == expected

    s = Stream(SYNC_INPUT).transform(f1, workers='max').collect()
    assert await s == expected

    expected = [(v + 3.8) * 2 for v in SYNC_INPUT]
    s = Stream(SYNC_INPUT).transform(f1).transform(f2)
    assert await s.collect() == expected

    class MySink:
        def __init__(self):
            self.result = 0

        def __call__(self, x):
            time.sleep(random.random() * 0.01)
            self.result += x * 3

    mysink = MySink()
    s = Stream(SYNC_INPUT).transform(f1).transform(mysink)
    n = await s.drain()
    assert n == len(SYNC_INPUT)

    got = mysink.result
    expected = sum((v + 3.8) * 3 for v in SYNC_INPUT)
    assert math.isclose(got, expected)


@pytest.mark.asyncio
async def test_concurrent_transform_with_error():
    data = [1, 2, 3, 4, 5, 'a', 6, 7]

    async def corrupt_data():
        for x in data:
            yield x

    async def process(x):
        return x + 2

    with pytest.raises(TypeError):
        z = Stream(corrupt_data()).transform(process, workers=2)
        zz = await z.collect()
        print(zz)

    z = Stream(corrupt_data()).transform(
        process, workers=2, return_exceptions=True)
    zz = await z.collect()
    print(zz)
    assert isinstance(zz[5], TypeError)

    z = Stream(corrupt_data()).transform(
        process, workers=2, return_exceptions=True)
    zz = await z.drain()
    assert zz == (len(data), 1)


@pytest.mark.asyncio
async def test_chain():
    data = [1, 2, 3, 4, 5, 6, 7, 'a', 8, 9]

    def corrupt_data():
        for x in data:
            yield x

    async def process1(x):
        return x + 2

    async def process2(x):
        if x > 8:
            raise ValueError(x)
        return x - 2

    with pytest.raises(TypeError):
        z = Stream(corrupt_data()).transform(process1, workers=2)
        await z.drain()

    with pytest.raises((TypeError, ValueError)):
        z = (Stream(corrupt_data())
             .transform(process1, workers=2)
             .buffer(maxsize=3)
             .transform(process2, workers=3)
             )
        await z.drain()

    with pytest.raises(ValueError):
        z = (Stream(corrupt_data())
             .transform(process1, workers=2, return_exceptions=True)
             .buffer(maxsize=3)
             .transform(process2, workers=3)
             )
        await z.drain()

    z = (Stream(corrupt_data())
         .transform(process1, workers=2, return_exceptions=True)
         .buffer(maxsize=3)
         .transform(process2, workers=3, return_exceptions=True)
         .peek_every_nth(1))
    print(await z.collect())

    z = (Stream(corrupt_data())
         .transform(process1, workers=2, return_exceptions=True)
         .drop_exceptions()
         .buffer(maxsize=3)
         .transform(process2, workers=3, return_exceptions=True)
         .log_exceptions()
         .drop_exceptions()
         )
    assert await z.collect() == [1, 2, 3, 4, 5, 6]


@pytest.mark.asyncio
async def test_chain_sync():
    data = [1, 2, 3, 4, 5, 6, 7, 'a', 8, 9]

    def corrupt_data():
        for x in data:
            yield x

    def process1(x):
        return x + 2

    def process2(x):
        if x > 8:
            raise ValueError(x)
        return x - 2

    with pytest.raises(TypeError):
        z = Stream(corrupt_data()).transform(process1, workers=2)
        await z.drain()

    with pytest.raises((ValueError, TypeError)):
        z = (Stream(corrupt_data())
             .transform(process1, workers=2)
             .buffer(maxsize=3)
             .transform(process2, workers=3)
             )
        await z.drain()

    with pytest.raises(ValueError):
        z = (Stream(corrupt_data())
             .transform(process1, workers=2, return_exceptions=True)
             .buffer(maxsize=3)
             .transform(process2, workers=3)
             )
        await z.drain()

    z = (Stream(corrupt_data())
         .transform(process1, workers=2, return_exceptions=True)
         .buffer(maxsize=3)
         .transform(process2, workers=3, return_exceptions=True)
         .peek_every_nth(1))
    print(await z.collect())

    z = (Stream(corrupt_data())
         .transform(process1, workers=2, return_exceptions=True)
         .drop_exceptions()
         .buffer(maxsize=3)
         .transform(process2, workers=3, return_exceptions=True)
         .log_exceptions()
         .drop_exceptions()
         )
    z = await z.collect()
    print(z)
    assert z == [1, 2, 3, 4, 5, 6]
