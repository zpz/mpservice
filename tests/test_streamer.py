import asyncio
import math
import random
from time import sleep

import pytest

from mpservice._streamer import Streamer, _default_peek_func


def test_stream():
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

    with Streamer(range(4)) as s:
        assert s.collect() == [0, 1, 2, 3]
    with Streamer(C()) as s:
        assert s.collect() == [1, 2, 3, 4, 5]
    with Streamer(D()) as s:
        assert s.collect() == [1, 2, 3]
    with Streamer(['a', 'b', 'c']) as s:
        assert s.collect() == ['a', 'b', 'c']


def test_batch():
    with Streamer(range(11)) as s:
        assert s.batch(3).collect() == [
            [0, 1, 2], [3, 4, 5], [6, 7, 8], [9, 10]]

        assert s.collect() == []

    with Streamer(list(range(11))) as s:
        assert s.batch(3).unbatch().collect() == list(range(11))
        assert s.collect() == []


def test_buffer():
    with Streamer(range(11)) as s:
        assert s.buffer(5).collect() == list(range(11))
    with Streamer(range(11)) as s:
        assert s.buffer(20).collect() == list(range(11))


def test_buffer_batch():
    with Streamer(range(19)) as s:
        n, _ = s.buffer(10).batch(5).unbatch().log_every_nth(1).drain()
        assert n == 19


def test_drop():
    data = [0, 1, 2, 'a', 4, ValueError(8), 6, 7]

    with Streamer(data).drop_exceptions() as s:
        assert s.collect() == [0, 1, 2, 'a', 4, 6, 7]

    s = Streamer(data).drop_exceptions().drop_if(lambda i, x: isinstance(x, str))
    with s:
        assert s.collect() == [0, 1, 2, 4, 6, 7]

    with Streamer(data) as s:
        assert s.drop_first_n(6).collect() == [6, 7]

    with Streamer((2, 3, 1, 5, 4, 7)) as s:
        assert s.drop_if(lambda i, x: x > i).collect() == [1, 4]


def test_keep():
    data = [0, 1, 2, 3, 'a', 5]

    with Streamer(data) as s:
        assert s.keep_if(lambda i, x: isinstance(
            x, int)).collect() == [0, 1, 2, 3, 5]

    with Streamer(data) as s:
        assert s.keep_every_nth(2).collect() == [0, 2, 'a']

    with Streamer(data) as s:
        assert s.head(3).collect() == [0, 1, 2]

    with Streamer(data) as s:
        s.head(4)
        s.drop_first_n(3)
        assert s.collect() == [3]

    with Streamer(data) as s:
        assert s.drop_first_n(3).head(1).collect() == [3]

    with Streamer(data) as s:
        ss = s.keep_random(0.5).collect()
        print(ss)
        assert 0 <= len(ss) <= len(data)


def test_peek():
    # The main point of this test is in checking the printout.

    data = list(range(10))

    with Streamer(data) as s:
        n, _ = s.peek_every_nth(3).drain()
        assert n == 10

    with Streamer(data).peek_random(0.5, lambda i, x: print(f'--{i}--  {x}')) as s:
        n, _ = s.drain()
        assert n == 10

    def peek_func(i, x):
        if 4 < i < 7:
            _default_peek_func(i, x)

    with Streamer(data).peek(peek_func) as s:
        s.drain()

    with Streamer(data).log_every_nth(4) as s:
        s.drain()


def test_drain():
    with Streamer(range(10)) as s:
        z, _ = s.drain()
        assert z == 10

    with Streamer(range(10)).log_every_nth(3) as s:
        z, _ = s.drain()
        assert z == 10

    with Streamer((1, 2, ValueError(3), 5, RuntimeError, 9)) as s:
        assert s.drain() == (6, 2)


def test_transform():

    def f1(x):
        sleep(random.random() * 0.002)
        return x + 3.8

    def f2(x):
        sleep(random.random() * 0.003)
        return x*2

    SYNC_INPUT = list(range(278))

    expected = [v + 3.8 for v in SYNC_INPUT]
    with Streamer(SYNC_INPUT) as s:
        s.transform(f1, concurrency=1)
        got = [v for v in s]
        assert got == expected

    with Streamer(SYNC_INPUT) as s:
        assert s.transform(f1, concurrency=10).collect() == expected

    with Streamer(SYNC_INPUT) as ss:
        s = ss.transform(f1, concurrency='max').collect()
        assert s == expected

    expected = [(v + 3.8) * 2 for v in SYNC_INPUT]
    with Streamer(SYNC_INPUT) as ss:
        s = ss.transform(f1).transform(f2)
        assert s.collect() == expected

    class MySink:
        def __init__(self):
            self.result = 0

        def __call__(self, x):
            sleep(random.random() * 0.01)
            self.result += x * 3

    mysink = MySink()
    with Streamer(SYNC_INPUT) as ss:
        s = ss.transform(f1).transform(mysink)
        n, _ = s.drain()
        assert n == len(SYNC_INPUT)

    got = mysink.result
    expected = sum((v + 3.8) * 3 for v in SYNC_INPUT)
    assert math.isclose(got, expected)


def test_transform_with_error():
    data = [1, 2, 3, 4, 5, 'a', 6, 7]

    def corrupt_data():
        for x in data:
            yield x

    def process(x):
        return x + 2

    with pytest.raises(TypeError):
        with Streamer(corrupt_data()) as s:
            s.transform(process, concurrency=2)
            zz = s.collect()
            print(zz)

    with Streamer(corrupt_data()) as s:
        s.transform(process, concurrency=2, return_exceptions=True)
        zz = s.collect()
        print(zz)
        assert isinstance(zz[5], TypeError)

    with Streamer(corrupt_data()) as s:
        s.transform(process, concurrency=2, return_exceptions=True)
        zz = s.drain()
        assert zz == (len(data), 1)


def test_chain():
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
        with Streamer(corrupt_data()) as s:
            s.transform(process1, concurrency=2)
            s.drain()

    with pytest.raises((ValueError, TypeError)):
        with Streamer(corrupt_data()) as s:
            s.transform(process2, concurrency=3)
            s.drain()

    with pytest.raises((ValueError, TypeError)):
        with Streamer(corrupt_data()) as s:
            s.transform(process1, concurrency=2, return_exceptions=True)
            s.transform(process2, concurrency=4)
            s.drain()

    print('d')
    with pytest.raises(TypeError):
        with Streamer(corrupt_data()) as s:
            s.transform(process1, concurrency=2)
            s.transform(process2, concurrency=4, return_exceptions=True)
            s.drain()

    print('e')
    with Streamer(corrupt_data()) as s:
        s.transform(process1, concurrency=2, return_exceptions=True)
        s.transform(process2, concurrency=4, return_exceptions=True)
        s.drain()

    with pytest.raises((TypeError, ValueError)):
        with Streamer(corrupt_data()) as s:
            s.transform(process1, concurrency=2)
            s.buffer(3)
            s.transform(process2, concurrency=3)
            s.drain()

    with pytest.raises((ValueError, TypeError)):
        with Streamer(corrupt_data()) as s:
            s.transform(process1, concurrency=2, return_exceptions=True)
            s.buffer(2)
            s.transform(process2, concurrency=3)
            s.drain()

    with Streamer(corrupt_data()) as s:
        z = (s
             .transform(process1, concurrency=2, return_exceptions=True)
             .buffer(3)
             .transform(process2, concurrency=3, return_exceptions=True)
             .peek_every_nth(1))
        print(z.collect())

    with Streamer(corrupt_data()) as s:
        s.transform(process1, concurrency=2, return_exceptions=True)
        s.drop_exceptions()
        s.buffer(3)
        s.transform(process2, concurrency=3, return_exceptions=True)
        s.log_exceptions()
        s.drop_exceptions()
        assert s.collect() == [1, 2, 3, 4, 5, 6]


def test_early_stop():
    def double(x):
        sleep(0.5)
        return x * 2


    with Streamer(range(300000)) as s:
        z = s.transform(double, concurrency=3)
        n = 0
        for x in z:
            print(x)
            n += 1
            if n == 10:
                break
        assert n == 10


def test_async_func():
    async def double(x):
        await asyncio.sleep(random.uniform(0.1, 0.3))
        return x * 2

    with Streamer(range(200)) as s:
        z = s.transform(double, concurrency=100)
        for x, y in zip(range(200), z):
            assert y == x * 2


def test_async_func_ret_x():
    async def double(x):
        await asyncio.sleep(random.uniform(0.1, 0.3))
        return x * 2

    with Streamer(range(200)) as s:
        z = s.transform(double, concurrency=100, return_x=True)
        for x, y in zip(range(200), z):
            assert y == (x, x * 2)

