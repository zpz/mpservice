import asyncio
import random
from time import perf_counter

import pytest

from mpservice._streamer_async import AsyncIter, AsyncStream, SyncIter


def plus2(x):
    return x + 2


async def async_plus_2(x):
    await asyncio.sleep(random.uniform(0.5, 1))
    return x + 2


async def arange(n=10):
    for k in range(n):
        yield k


async def agen(data):
    for d in data:
        yield d


@pytest.mark.asyncio
async def test_synciter():
    for i, x in enumerate(SyncIter(arange(10))):
        assert x == i


@pytest.mark.asyncio
async def test_asynciter():
    i = 0
    async for x in AsyncIter(range(10)):
        assert x == i
        i += 1


@pytest.mark.asyncio
async def test_async_drain():
    assert await AsyncStream(arange(8)).drain() == 8


@pytest.mark.asyncio
async def test_async_collect():
    assert await AsyncStream(arange(3)).collect() == [0, 1, 2]


@pytest.mark.asyncio
async def test_async_map():
    def inc(x, shift=1):
        return x + shift

    s = AsyncStream(arange(5)).map(inc, shift=2)

    assert await s.collect() == [2, 3, 4, 5, 6]
    assert await AsyncStream(arange(5)).map(inc, shift=2).collect() == [
        2,
        3,
        4,
        5,
        6,
    ]
    assert await AsyncStream(arange(5)).map(lambda x: x * 2).collect() == [
        0,
        2,
        4,
        6,
        8,
    ]


@pytest.mark.asyncio
async def test_async_filter():
    assert await AsyncStream(arange(7)).filter(lambda n: (n % 2) == 0).collect() == [
        0,
        2,
        4,
        6,
    ]

    def odd_or_even(x, even=True):
        if even:
            return (x % 2) == 0
        return (x % 2) != 0

    assert await AsyncStream(arange(7)).filter(odd_or_even).collect() == [
        0,
        2,
        4,
        6,
    ]
    assert await AsyncStream(arange(7)).filter(odd_or_even, even=False).collect() == [
        1,
        3,
        5,
    ]

    data = [0, 1, 2, 'a', 4, ValueError(8), 6, 7]

    class Tail:
        def __init__(self, n):
            self._idx = 0
            self.n = n

        def __call__(self, x):
            z = self._idx >= self.n
            self._idx += 1
            return z

    assert await AsyncStream(agen(data)).filter(Tail(6)).collect() == [6, 7]

    class Head:
        def __init__(self):
            self._idx = 0

        def __call__(self, x):
            z = x <= self._idx
            self._idx += 1
            return z

    assert [x async for x in AsyncStream(agen((2, 3, 1, 5, 4, 7))).filter(Head())] == [
        1,
        4,
    ]


@pytest.mark.asyncio
async def test_async_filter_exceptions():
    exc = [
        1,
        ValueError(3),
        2,
        IndexError(4),
        FileNotFoundError(),
        3,
        KeyboardInterrupt(),
        4,
    ]

    assert await AsyncStream(agen(exc)).filter_exceptions(BaseException).collect() == [
        1,
        2,
        3,
        4,
    ]

    assert await AsyncStream(agen(exc)).filter_exceptions(
        BaseException, Exception
    ).collect() == exc[:-2] + [exc[-1]]

    with pytest.raises(IndexError):
        assert (
            await AsyncStream(agen(exc)).filter_exceptions(ValueError).collect() == exc
        )

    with pytest.raises(FileNotFoundError):
        ss = AsyncStream(agen(exc))
        assert await ss.filter_exceptions((ValueError, IndexError)).collect() == exc

    ss = AsyncStream(agen(exc))
    with pytest.raises(KeyboardInterrupt):
        assert await ss.filter_exceptions(Exception, FileNotFoundError).collect() == exc

    assert await AsyncStream(agen(exc)).filter_exceptions(
        BaseException, FileNotFoundError
    ).collect() == [1, 2, exc[4], 3, 4]


@pytest.mark.asyncio
async def test_async_peek():
    # The main point of this test is in checking the printout.
    print('')

    async def data():
        for x in range(10):
            yield x

    s = AsyncStream(data())
    n = await s.peek(interval=3).drain()
    assert n == 10
    print('')

    def foo(x):
        print(x)

    assert await AsyncStream(data()).peek(print_func=foo, interval=0.6).drain() == 10
    print('')

    exc = [0, 1, 2, ValueError(100), 4]
    # `peek` does not drop exceptions
    assert await AsyncStream(agen(exc)).peek().drain() == len(exc)


@pytest.mark.asyncio
async def test_async_shuffle():
    async def data():
        for x in range(20):
            yield x

    print('')
    shuffled = [v async for v in AsyncStream(data()).shuffle(5)]
    print(shuffled)
    shuffled = [v async for v in AsyncStream(data()).shuffle(50)]
    print(shuffled)


@pytest.mark.asyncio
async def test_async_head():
    data = [0, 1, 2, 3, 'a', 5]
    # assert [x async for x in AsyncStream(agen(data)).head(3)] == data[:3]
    assert await AsyncStream(agen(data)).head(3).collect() == data[:3]
    assert await AsyncStream(agen(data)).head(30).collect() == data


@pytest.mark.asyncio
async def test_async_tail():
    data = [0, 1, 2, 3, 'a', 5]

    assert await AsyncStream(agen(data)).tail(2).collect() == ['a', 5]
    assert await AsyncStream(agen(data)).tail(10).collect() == data


@pytest.mark.asyncio
async def test_async_groupby():
    data = [
        'atlas',
        'apple',
        'answer',
        'bee',
        'block',
        'away',
        'peter',
        'question',
        'plum',
        'please',
    ]

    async def gather(x):
        key, grp = x
        return [v async for v in grp]

    assert await AsyncStream(agen(data)).groupby(lambda x: x[0]).map(
        gather
    ).collect() == [
        ['atlas', 'apple', 'answer'],
        ['bee', 'block'],
        ['away'],
        ['peter'],
        ['question'],
        ['plum', 'please'],
    ]


@pytest.mark.asyncio
async def test_async_batch():
    s = AsyncStream(arange(11))
    assert [x async for x in s.batch(3)] == [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9, 10]]

    s = AsyncStream(arange(11))
    assert await s.batch(3).unbatch().collect() == list(range(11))


@pytest.mark.asyncio
async def test_async_unbatch():
    data = [[0, 1, 2], [], [3, 4], [], [5, 6, 7]]
    assert await AsyncStream(agen(data)).unbatch().collect() == list(range(8))


@pytest.mark.asyncio
async def test_async_accumulate():
    async def data():
        for x in range(6):
            yield x

    assert await AsyncStream(data()).accumulate(lambda x, y: x + y).collect() == [
        0,
        1,
        3,
        6,
        10,
        15,
    ]
    assert await AsyncStream(data()).accumulate(lambda x, y: x + y, 3).collect() == [
        3,
        4,
        6,
        9,
        13,
        18,
    ]

    def add(x, y):
        if y % 2 == 0:
            return x + y
        return x - y

    assert await AsyncStream(data()).accumulate(add, -1).collect() == [
        -1,
        -2,
        0,
        -3,
        1,
        -4,
    ]


@pytest.mark.asyncio
async def test_async_buffer():
    assert await AsyncStream(arange(11)).buffer(5).collect() == list(range(11))
    assert await AsyncStream(arange(11)).buffer(20).collect() == list(range(11))


@pytest.mark.asyncio
async def test_async_buffer_batch():
    n = (
        await AsyncStream(arange(19))
        .buffer(10)
        .batch(5)
        .unbatch()
        .peek(interval=1)
        .drain()
    )
    assert n == 19


@pytest.mark.asyncio
async def test_async_buffer_break():
    async def make_data():
        for x in range(100):
            if x == 88:
                raise ValueError(x)
            await asyncio.sleep(random.random() * 0.02)
            yield x

    x = AsyncStream(make_data()).buffer(23).map(lambda x: x * 2)
    with pytest.raises(TypeError):
        for v in x:
            print(v)

    async for v in x:
        if v > 50:
            break

    x = AsyncStream(make_data()).buffer(23).map(lambda x: x * 2)
    with pytest.raises(ValueError):
        async for v in x:
            pass


# This test prints some warnings.
# It seems to be a py.test issue. See bottom of this function.
@pytest.mark.asyncio()
async def test_async_parmap():
    print('')
    stream = AsyncStream(arange(1000))
    stream.parmap(plus2, executor='thread')
    t0 = perf_counter()
    x = 0
    async for y in stream:
        assert y == x + 2
        x += 1
    t1 = perf_counter()
    print(t1 - t0)
    assert t1 - t0 < 5
    # sequential processing would take 500+ sec ??

    async def data1():
        for x in range(20):
            if x == 11:
                yield 'a'
            else:
                yield x

    # Test exception in the worker function
    stream = AsyncStream(data1()).parmap(
        plus2, executor='process', return_exceptions=True
    )
    x = 0
    async for y in stream:
        if x == 11:
            assert isinstance(y, TypeError)
        else:
            assert y == x + 2
        x += 1

    # # # Test premature quit, i.e. GeneratorExit
    stream = AsyncStream(data1()).parmap(plus2, executor='process')
    x = 0
    # async for y in stream:
    ss = stream.__aiter__()
    async for y in ss:
        assert y == x + 2
        x += 1
        if x == 10:
            break

    # # workaround pytest-asyncio issue; see https://github.com/pytest-dev/pytest-asyncio/issues/759
    # The follow no longer makes it clean.
    await ss.aclose()


@pytest.mark.asyncio
async def test_async_parmap_async():
    print('')
    stream = AsyncStream(arange(1000))
    stream.parmap(async_plus_2)
    t0 = perf_counter()
    x = 0
    async for y in stream:
        assert y == x + 2
        x += 1
    t1 = perf_counter()
    print(t1 - t0)
    assert t1 - t0 < 5
    # sequential processing would take 500+ sec

    async def data1():
        for x in range(20):
            if x == 11:
                yield 'a'
            else:
                yield x

    # Test exception in the worker function
    stream = AsyncStream(data1()).parmap(async_plus_2)
    with pytest.raises(TypeError):
        x = 0
        async for y in stream:
            assert y == x + 2
            x += 1

    # Test premature quit, i.e. GeneratorExit
    stream = AsyncStream(data1()).parmap(async_plus_2)
    x = 0
    # async for y in stream:
    data = stream.__aiter__()
    async for y in data:
        assert y == x + 2
        x += 1
        if x == 10:
            break

    # workaround pytest-asyncio issue; see https://github.com/pytest-dev/pytest-asyncio/issues/759
    await data.aclose()


@pytest.mark.asyncio
async def test_async_switch():
    s = AsyncStream(arange(100)).map(plus2)
    x = 0
    async for y in s:
        assert y == x + 2
        x += 1

    s = AsyncStream(arange(100)).parmap(async_plus_2).map(plus2)
    x = 0
    async for y in s:
        assert y == x + 4
        x += 1


if __name__ == '__main__':
    asyncio.run(test_async_parmap())
