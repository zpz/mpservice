import math
import random
from time import sleep

import pytest
from deprecation import DeprecatedWarning

from mpservice.streamer import Stream


def test_stream():
    class D:
        def __iter__(self):
            for x in [1, 2, 3]:
                yield x

    assert Stream(range(4)).collect() == [0, 1, 2, 3]
    assert list(Stream(D())) == [1, 2, 3]
    assert list(Stream(['a', 'b', 'c'])) == ['a', 'b', 'c']


def test_drain():
    assert Stream(range(8)).drain() == 8


def test_collect():
    assert Stream(range(3)).collect() == [0, 1, 2]


def test_map():
    def inc(x, shift=1):
        return x + shift

    assert Stream(range(5)).map(inc, shift=2).collect() == [2, 3, 4, 5, 6]
    assert Stream(range(5)).map(lambda x: x * 2).collect() == [0, 2, 4, 6, 8]


def test_filter():
    assert Stream(range(7)).filter(lambda n: (n % 2) == 0).collect() == [0, 2, 4, 6]

    def odd_or_even(x, even=True):
        if even:
            return (x % 2) == 0
        return (x % 2) != 0

    assert Stream(range(7)).filter(odd_or_even).collect() == [0, 2, 4, 6]
    assert Stream(range(7)).filter(odd_or_even, even=False).collect() == [1, 3, 5]

    data = [0, 1, 2, 'a', 4, ValueError(8), 6, 7]

    class Tail:
        def __init__(self, n):
            self._idx = 0
            self.n = n
        def __call__(self, x):
            z = self._idx >= self.n
            self._idx += 1
            return z

    assert Stream(data).filter(Tail(6)).collect() == [6, 7]

    class Head:
        def __init__(self):
            self._idx = 0
        def __call__(self, x):
            z = (x <= self._idx)
            self._idx += 1
            return z

    assert list(Stream((2, 3, 1, 5, 4, 7)).filter(Head())) == [1, 4]


def test_filter_exceptions():
    exc = [1, ValueError(3), 2, IndexError(4), FileNotFoundError(), 3, KeyboardInterrupt(), 4]

    assert Stream(exc).filter_exceptions(BaseException).collect() == [1, 2, 3, 4]

    assert Stream(exc).filter_exceptions(BaseException, Exception).collect() == exc[:-2] + [exc[-1]]

    with pytest.raises(IndexError):
        assert Stream(exc).filter_exceptions(ValueError).collect() == exc

    with pytest.raises(FileNotFoundError):
        ss = Stream(exc)
        assert ss.filter_exceptions((ValueError, IndexError)).collect() == exc

    ss = Stream(exc)
    with pytest.raises(KeyboardInterrupt):
        assert ss.filter_exceptions(Exception, FileNotFoundError).collect() == exc

    assert Stream(exc).filter_exceptions(BaseException, FileNotFoundError).collect() == [1, 2, exc[4], 3, 4]


def test_peek():
    # The main point of this test is in checking the printout.

    data = list(range(10))

    s = Stream(data)
    n = s.peek(interval=3).drain()
    assert n == 10

    def foo(x):
        print(x)

    assert Stream(data).peek(print_func=foo, interval=0.6).drain() == 10

    with pytest.warns(DeprecatedWarning):
        Stream(data).peek_every_nth(4).drain()

    exc = [0, 1, 2, ValueError(100), 4]
    # `peek` does not drop exceptions
    assert Stream(exc).peek().drain() == len(exc)


def test_head():
    data = [0, 1, 2, 3, 'a', 5]
    assert list(Stream(data).head(3)) == data[:3]
    assert list(Stream(data).head(30)) == data


def test_tail():
    data = [0, 1, 2, 3, 'a', 5]

    assert Stream(data).tail(2).collect() == ['a', 5]
    assert list(Stream(data).tail(10)) == data


def test_groupby():
    data = ['atlas', 'apple', 'answer', 'bee', 'block', 'away', 'peter', 'question', 'plum', 'please']
    assert Stream(data).groupby(lambda x: x[0]).collect() == [['atlas', 'apple', 'answer'], ['bee', 'block'], ['away'], ['peter'], ['question'], ['plum', 'please']]


def test_batch():
    s = Stream(range(11))
    assert list(s.batch(3)) == [
        [0, 1, 2], [3, 4, 5], [6, 7, 8], [9, 10]]

    assert list(s) == [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9, 10]]

    s = Stream(list(range(11)))
    assert list(s.batch(3).unbatch()) == list(range(11))


def test_unbatch():
    data = [[0, 1, 2], [], [3, 4], [], [5, 6, 7]]
    assert Stream(data).unbatch().collect() == list(range(8))


def test_accumulate():
    data = list(range(6))
    assert Stream(data).accumulate(lambda x, y: x + y).collect() == [0, 1, 3, 6, 10, 15]
    assert Stream(data).accumulate(lambda x, y: x + y, 3).collect() == [3, 4, 6, 9, 13, 18]

    def add(x, y):
        if y % 2 == 0:
            return x + y
        return x - y

    assert Stream(data).accumulate(add, -1).collect() == [-1, -2, 0, -3, 1, -4]


def test_buffer():
    assert list(Stream(range(11)).buffer(5)) == list(range(11))
    assert list(Stream(range(11)).buffer(20)) == list(range(11))


def test_buffer_noop():
    # No action if buffer is not used.
    x = Stream(range(1000)).buffer(20)
    assert True


def test_buffer_batch():
    n = Stream(range(19)).buffer(10).batch(5).unbatch().peek(interval=1).drain()
    assert n == 19


def test_buffer_break():
    def make_data():
        for x in range(100):
            if x == 88:
                raise ValueError(x)
            sleep(random.random() * 0.02)
            yield x

    x = Stream(make_data()).buffer(23).map(lambda x: x * 2)
    for v in x:
        if v > 50:
            break

    x = Stream(make_data()).buffer(23).map(lambda x: x * 2)
    with pytest.raises(ValueError):
        for v in x:
            pass


def test_parmap():

    def f1(x):
        sleep(random.random() * 0.002)
        return x + 3.8

    def f2(x):
        sleep(random.random() * 0.003)
        return x*2

    SYNC_INPUT = list(range(278))

    expected = [v + 3.8 for v in SYNC_INPUT]

    with pytest.warns(DeprecatedWarning):
        assert Stream(SYNC_INPUT).transform(f1, concurrency=1, executor='thread').collect() == expected

    assert list(Stream(SYNC_INPUT).parmap(f1, num_workers=10, executor='thread')) == expected

    assert list(Stream(SYNC_INPUT).parmap(f1, num_workers=20, executor='thread')) == expected

    expected = [(v + 3.8) * 2 for v in SYNC_INPUT]
    assert list(Stream(SYNC_INPUT).parmap(f1, executor='thread').parmap(f2, executor='thread')) == expected

    class MySink:
        def __init__(self):
            self.result = 0

        def __call__(self, x):
            sleep(random.random() * 0.01)
            self.result += x * 3

    mysink = MySink()
    n = Stream(SYNC_INPUT).parmap(f1, executor='thread').parmap(mysink, executor='thread').drain()
    assert n == len(SYNC_INPUT)

    got = mysink.result
    expected = sum((v + 3.8) * 3 for v in SYNC_INPUT)
    assert math.isclose(got, expected)


def test_parmap_noop():
    # No problem if no action.
    def foo(n):
        return range(n)
 
    x = Stream(range(1000)).parmap(foo, executor='thread')
    assert True


def test_parmap_with_error():
    data = [1, 2, 3, 4, 5, 'a', 6, 7]

    def corrupt_data():
        for x in data:
            yield x

    def process(x):
        return x + 2

    with pytest.raises(TypeError):
        s = Stream(corrupt_data())
        s.parmap(process, executor='thread', num_workers=2)
        zz = list(s)
        print(zz)

    zz = list(Stream(corrupt_data()).parmap(process, executor='thread', num_workers=2, return_exceptions=True))
    print(zz)
    assert isinstance(zz[5], TypeError)

    s = Stream(corrupt_data())
    s.parmap(process, executor='thread', num_workers=2, return_exceptions=True)
    n = 0
    nexc = 0
    for x in s:
        n += 1
        if isinstance(x, BaseException):
            nexc += 1
    assert n == len(data)
    assert nexc == 1


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
        s = Stream(corrupt_data())
        s.parmap(process1, executor='thread', num_workers=2)
        s.drain()

    with pytest.raises((ValueError, TypeError)):
        s = Stream(corrupt_data())
        s.parmap(process2, executor='thread', num_workers=3)
        s.drain()

    with pytest.raises((ValueError, TypeError)):
        s = Stream(corrupt_data())
        s.parmap(process1, executor='thread', num_workers=2, return_exceptions=True)
        s.parmap(process2, executor='thread', num_workers=4)
        s.drain()

    with pytest.raises(TypeError):
        s = Stream(corrupt_data())
        s.parmap(process1, executor='thread', num_workers=2)
        s.parmap(process2, executor='thread', num_workers=4, return_exceptions=True)
        s.drain()

    s = Stream(corrupt_data())
    s.parmap(process1, executor='thread', num_workers=2, return_exceptions=True)
    s.parmap(process2, executor='thread', num_workers=4, return_exceptions=True)
    s.drain()

    with pytest.raises((TypeError, ValueError)):
        s = Stream(corrupt_data())
        s.parmap(process1, executor='thread', num_workers=1) #2)
        s.buffer(3)
        s.parmap(process2, executor='thread', num_workers=1) #3)
        s.drain()

    with pytest.raises((ValueError, TypeError)):
        s = Stream(corrupt_data())
        s.parmap(process1, executor='thread', num_workers=2, return_exceptions=True)
        s.buffer(2)
        s.parmap(process2, executor='thread', num_workers=3)
        s.drain()

    zz = list(Stream(corrupt_data())
             .parmap(process1, executor='thread', num_workers=2, return_exceptions=True)
             .buffer(3)
             .parmap(process2, executor='thread', num_workers=3, return_exceptions=True)
             .peek(interval=1)
             )
    print('')
    print(zz)

    s = Stream(corrupt_data())
    s.parmap(process1, executor='thread', num_workers=2, return_exceptions=True)
    s.filter_exceptions(BaseException)
    s.buffer(3)
    s.parmap(process2, executor='thread', num_workers=3, return_exceptions=True)
    s.peek()
    s.filter_exceptions(BaseException)
    assert list(s) == [1, 2, 3, 4, 5, 6]


def test_stream_early_stop():
    def double(x):
        sleep(0.5)
        return x * 2

    s = Stream(range(300000))
    z = s.parmap(double, executor='thread', num_workers=3)
    n = 0
    for x in z:
        # print(x)
        n += 1
        if n == 10:
            break
    assert n == 10


def double(x):
    return x * 2


def test_parmap_mp():
    SYNC_INPUT = list(range(278))
    got = Stream(SYNC_INPUT).parmap(double, executor='process', num_workers=4).collect()
    assert got == [v * 2 for v in SYNC_INPUT]


class Pad:
    def __init__(self, value: str):
        self._val = value

    def __call__(self, x):
        return f"{self._val} {x}"


padder: Pad
# A global to be used in other processes


def prepare_pad(value: str):
    global padder
    padder = Pad(value)


def pad_worker(x):
    return padder(x)


def test_parmap_initializer():
    data = Stream(range(30)).parmap(
        pad_worker, executor='process', num_workers=3,
        executor_initializer=prepare_pad,
        executor_init_args=('abc', ),
    )
    assert data.collect() == [f"abc {x}" for x in range(30)]



def add_four(x):
    return x + 4


def worker1(n):
    data = Stream(range(n)).parmap(add_four, executor='thread', num_workers=3)
    return list(data)


def test_parmap_nest():
    data = Stream([10, 20, 30]).parmap(worker1, executor='process', num_workers=3)
    assert data.collect() == [
        [v + 4 for v in range(n)]
        for n in (10, 20, 30)
    ]