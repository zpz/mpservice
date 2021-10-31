import math
import random
from time import sleep

import pytest

from mpservice._streamer import Stream, _default_peek_func


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

    assert Stream(range(4)).collect() == [0, 1, 2, 3]
    assert Stream(C()).collect() == [1, 2, 3, 4, 5]
    assert Stream(D()).collect() == [1, 2, 3]
    assert Stream(['a', 'b', 'c']).collect() == ['a', 'b', 'c']


def test_batch():
    s = Stream(range(11))
    assert s.batch(3).collect() == [
        [0, 1, 2], [3, 4, 5], [6, 7, 8], [9, 10]]

    assert s.collect() == []

    s = Stream(list(range(11)))
    assert s.batch(3).unbatch().collect() == list(range(11))
    assert s.collect() == []


def test_buffer():
    s = Stream(range(11))
    assert s.buffer(5).collect() == list(range(11))
    s = Stream(range(11))
    assert s.buffer(20).collect() == list(range(11))


def test_drop():
    data = [0, 1, 2, 'a', 4, ValueError(8), 6, 7]

    s = Stream(data)
    assert s.drop_exceptions().collect() == [
        0, 1, 2, 'a', 4, 6, 7]

    s = Stream(data)
    assert s.drop_exceptions().drop_if(
        lambda i, x: isinstance(x, str)).collect() == [
        0, 1, 2, 4, 6, 7]

    s = Stream(data)
    assert s.drop_first_n(6).collect() == [6, 7]

    assert Stream((2, 3, 1, 5, 4, 7)).drop_if(
        lambda i, x: x > i).collect() == [1, 4]


def test_keep():
    data = [0, 1, 2, 3, 'a', 5]

    s = Stream(data)
    assert s.keep_if(lambda i, x: isinstance(
        x, int)).collect() == [0, 1, 2, 3, 5]

    s = Stream(data)
    assert s.keep_every_nth(2).collect() == [0, 2, 'a']

    s = Stream(data)
    assert s.head(3).collect() == [0, 1, 2]

    s = Stream(data)
    assert s.head(4).drop_first_n(3).collect() == [3]

    s = Stream(data)
    assert s.drop_first_n(3).head(1).collect() == [3]

    s = Stream(data)
    ss = s.keep_random(0.5).collect()
    print(ss)
    assert 0 <= len(ss) <= len(data)


def test_peek():
    # The main point of this test is in checking the printout.

    data = list(range(10))

    n = Stream(data).peek_every_nth(3).drain()
    assert n == 10

    n = Stream(data).peek_random(
        0.5, lambda i, x: print(f'--{i}--  {x}')).drain()
    assert n == 10

    def peek_func(i, x):
        if 4 < i < 7:
            _default_peek_func(i, x)

    Stream(data).peek(peek_func).drain()

    Stream(data).log_every_nth(4).drain()


def test_drain():
    z = Stream(range(10)).drain()
    assert z == 10

    z = Stream(range(10)).log_every_nth(3).drain()
    assert z == 10

    z = Stream((1, 2, ValueError(3), 5, RuntimeError, 9)).drain()
    assert z == (6, 2)


def test_transform():

    def f1(x):
        sleep(random.random() * 0.01)
        return x + 3.8

    def f2(x):
        sleep(random.random() * 0.01)
        return x*2

    SYNC_INPUT = list(range(278))

    expected = [v + 3.8 for v in SYNC_INPUT]
    s = Stream(SYNC_INPUT).transform(f1, workers=1)
    got = [v for v in s]
    assert got == expected

    s = Stream(SYNC_INPUT).transform(f1, workers=10).collect()
    assert s == expected

    s = Stream(SYNC_INPUT).transform(f1, workers='max').collect()
    assert s == expected

    expected = [(v + 3.8) * 2 for v in SYNC_INPUT]
    s = Stream(SYNC_INPUT).transform(f1).transform(f2)
    assert s.collect() == expected

    class MySink:
        def __init__(self):
            self.result = 0

        def __call__(self, x):
            sleep(random.random() * 0.01)
            self.result += x * 3

    mysink = MySink()
    s = Stream(SYNC_INPUT).transform(f1).transform(mysink)
    n = s.drain()
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
        z = Stream(corrupt_data()).transform(process, workers=2)
        zz = z.collect()
        print(zz)

    z = Stream(corrupt_data()).transform(
        process, workers=2, return_exceptions=True)
    zz = z.collect()
    print(zz)
    assert isinstance(zz[5], TypeError)

    z = Stream(corrupt_data()).transform(
        process, workers=2, return_exceptions=True)
    zz = z.drain()
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
        z = Stream(corrupt_data()).transform(process1, workers=2)
        z.drain()

    with pytest.raises((TypeError, ValueError)):
        z = (Stream(corrupt_data())
             .transform(process1, workers=2)
             .buffer(3)
             .transform(process2, workers=3)
             )
        z.drain()

    with pytest.raises(ValueError):
        z = (Stream(corrupt_data())
             .transform(process1, workers=2, return_exceptions=True)
             .buffer(2)
             .transform(process2, workers=3)
             )
        z.drain()

    z = (Stream(corrupt_data())
         .transform(process1, workers=2, return_exceptions=True)
         .buffer(3)
         .transform(process2, workers=3, return_exceptions=True)
         .peek_every_nth(1))
    print(z.collect())

    z = (Stream(corrupt_data())
         .transform(process1, workers=2, return_exceptions=True)
         .drop_exceptions()
         .buffer(3)
         .transform(process2, workers=3, return_exceptions=True)
         .log_exceptions()
         .drop_exceptions()
         )
    assert z.collect() == [1, 2, 3, 4, 5, 6]
