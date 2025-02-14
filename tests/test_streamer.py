import asyncio
import concurrent.futures
import functools
import math
import queue
import random
from time import perf_counter, sleep

import pytest

import mpservice
from mpservice.concurrent.futures import ThreadPoolExecutor
from mpservice.streamer import (
    EagerBatcher,
    Stream,
    async_fifo_stream,
    fifo_stream,
    tee,
)


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
            z = x <= self._idx
            self._idx += 1
            return z

    assert list(Stream((2, 3, 1, 5, 4, 7)).filter(Head())) == [1, 4]


def test_filter_exceptions():
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

    assert Stream(exc).filter_exceptions(BaseException).collect() == [1, 2, 3, 4]

    assert Stream(exc).filter_exceptions(BaseException, Exception).collect() == exc[
        :-2
    ] + [exc[-1]]

    with pytest.raises(IndexError):
        assert Stream(exc).filter_exceptions(ValueError).collect() == exc

    with pytest.raises(FileNotFoundError):
        ss = Stream(exc)
        assert ss.filter_exceptions((ValueError, IndexError)).collect() == exc

    ss = Stream(exc)
    with pytest.raises(KeyboardInterrupt):
        assert ss.filter_exceptions(Exception, FileNotFoundError).collect() == exc

    assert Stream(exc).filter_exceptions(
        BaseException, FileNotFoundError
    ).collect() == [1, 2, exc[4], 3, 4]


def test_peek():
    # The main point of this test is in checking the printout.
    print('')
    data = list(range(10))

    s = Stream(data)
    n = s.peek(interval=3).drain()
    assert n == 10
    print('')

    def foo(x):
        print(x)

    assert (
        Stream(data).peek(print_func=foo, interval=0.6, prefix='\n++++\n').drain() == 10
    )
    print('')

    exc = [0, 1, 2, ValueError(100), 4]
    # `peek` does not drop exceptions
    assert Stream(exc).peek().drain() == len(exc)


def test_shuffle():
    print('')
    data = list(range(20))
    shuffled = list(Stream(data).shuffle(5))
    print(shuffled)
    shuffled = list(Stream(data).shuffle(50))
    print(shuffled)


def test_head():
    data = [0, 1, 2, 3, 'a', 5]
    assert list(Stream(data).head(3)) == data[:3]
    assert list(Stream(data).head(30)) == data


def test_tail():
    data = [0, 1, 2, 3, 'a', 5]

    assert Stream(data).tail(2).collect() == ['a', 5]
    assert list(Stream(data).tail(10)) == data


def test_groupby():
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
    assert Stream(data).groupby(lambda x: x[0]).map(lambda x: list(x[1])).collect() == [
        ['atlas', 'apple', 'answer'],
        ['bee', 'block'],
        ['away'],
        ['peter'],
        ['question'],
        ['plum', 'please'],
    ]


def test_batch():
    s = Stream(range(11))
    assert list(s.batch(3)) == [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9, 10]]

    s = Stream(list(range(11)))
    assert list(s.batch(3).unbatch()) == list(range(11))


def test_unbatch():
    data = [[0, 1, 2], [], [3, 4], [], [5, 6, 7]]
    assert Stream(data).unbatch().collect() == list(range(8))


def test_accumulate():
    data = list(range(6))
    assert Stream(data).accumulate(lambda x, y: x + y).collect() == [0, 1, 3, 6, 10, 15]
    assert Stream(data).accumulate(lambda x, y: x + y, 3).collect() == [
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

    assert Stream(data).accumulate(add, -1).collect() == [-1, -2, 0, -3, 1, -4]


def test_buffer():
    assert list(Stream(range(11)).buffer(5)) == list(range(11))
    assert list(Stream(range(11)).buffer(20)) == list(range(11))


def test_buffer_noop():
    # No action if buffer is not used.
    Stream(range(1000)).buffer(20)
    assert True


def test_buffer_batch():
    n = Stream(range(19)).buffer(10).batch(5).unbatch().peek(interval=1).drain()
    assert n == 19


# @pytest.mark.filterwarnings("ignore::pytest.PytestUnhandledThreadExceptionWarning")
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


def parmap_f1(x):
    sleep(random.random() * 0.002)
    return x + 3.8


def parmap_f2(x):
    sleep(random.random() * 0.003)
    return x * 2


class MySink:
    def __init__(self):
        self.result = 0

    def __call__(self, x):
        sleep(random.random() * 0.01)
        self.result += x * 3


@pytest.mark.parametrize('executor', ['thread', 'process'])
def test_parmap(executor):
    f1 = parmap_f1
    f2 = parmap_f2

    SYNC_INPUT = list(range(278))

    expected = [v + 3.8 for v in SYNC_INPUT]

    assert list(Stream(SYNC_INPUT).parmap(f1, executor=executor)) == expected

    assert list(Stream(SYNC_INPUT).parmap(f1, executor=executor)) == expected

    expected = [(v + 3.8) * 2 for v in SYNC_INPUT]
    assert (
        list(
            Stream(SYNC_INPUT)
            .parmap(f1, executor=executor)
            .parmap(f2, executor=executor)
        )
        == expected
    )

    mysink = MySink()
    n = (
        Stream(SYNC_INPUT)
        .parmap(f1, executor=executor)
        .parmap(mysink, executor='thread')
        .drain()
    )
    # The second executor must be 'thread' here.
    assert n == len(SYNC_INPUT)

    got = mysink.result
    expected = sum((v + 3.8) * 3 for v in SYNC_INPUT)
    assert math.isclose(got, expected)


def parmap_noop_foo(n):
    return range(n)


@pytest.mark.parametrize('executor', ['thread', 'process'])
def test_parmap_noop(executor):
    # No problem if no action.
    Stream(range(1000)).parmap(parmap_noop_foo, executor=executor)
    assert True


@pytest.mark.parametrize('executor', ['thread', 'process'])
def test_parmap_with_error(executor):
    data = [1, 2, 3, 4, 5, 'a', 6, 7]

    def corrupt_data():
        for x in data:
            yield x

    process = plus2

    with pytest.raises(TypeError):
        s = Stream(corrupt_data())
        s.parmap(process, executor=executor)
        zz = list(s)
        print(zz)

    zz = list(
        Stream(corrupt_data()).parmap(
            process, executor=executor, return_exceptions=True
        )
    )
    print(zz)
    assert isinstance(zz[5], TypeError)

    s = Stream(corrupt_data())
    s.parmap(process, executor=executor, return_exceptions=True)
    n = 0
    nexc = 0
    for x in s:
        n += 1
        if isinstance(x, BaseException):
            nexc += 1
    assert n == len(data)
    assert nexc == 1


def plus2(x):
    return x + 2


def minus2(x):
    if x > 8:
        raise ValueError(x)
    return x - 2


# @pytest.mark.filterwarnings("ignore::pytest.PytestUnhandledThreadExceptionWarning")
@pytest.mark.parametrize('executor', ['thread', 'process'])
def test_chain(executor):
    data = [1, 2, 3, 4, 5, 6, 7, 'a', 8, 9]

    def corrupt_data():
        for x in data:
            yield x

    process1 = plus2
    process2 = minus2

    with pytest.raises(TypeError):
        s = Stream(corrupt_data())
        s.parmap(process1, executor=executor)
        s.drain()

    with pytest.raises((ValueError, TypeError)):
        s = Stream(corrupt_data())
        s.parmap(process2, executor=executor)
        s.drain()

    with pytest.raises((ValueError, TypeError)):
        s = Stream(corrupt_data())
        s.parmap(process1, executor=executor, return_exceptions=True)
        s.parmap(process2, executor=executor)
        s.drain()

    with pytest.raises(TypeError):
        s = Stream(corrupt_data())
        s.parmap(process1, executor=executor)
        s.parmap(process2, executor=executor, return_exceptions=True)
        s.drain()

    s = Stream(corrupt_data())
    s.parmap(process1, executor=executor, return_exceptions=True)
    s.parmap(process2, executor=executor, return_exceptions=True)
    s.drain()

    with pytest.raises((TypeError, ValueError)):
        s = Stream(corrupt_data())
        s.parmap(process1, executor=executor, parmapper_name='---first')  # 2)
        s.buffer(3)
        s.parmap(process2, executor=executor, parmapper_name='+++second')  # 3)
        s.drain()

    with pytest.raises((ValueError, TypeError)):
        s = Stream(corrupt_data())
        s.parmap(process1, executor=executor, return_exceptions=True)
        s.buffer(2)
        s.parmap(process2, executor=executor)
        s.drain()

    zz = list(
        Stream(corrupt_data())
        .parmap(process1, executor=executor, return_exceptions=True)
        .buffer(3)
        .parmap(process2, executor=executor, return_exceptions=True)
        .peek(interval=1)
    )
    print('')
    print(zz)

    s = Stream(corrupt_data())
    s.parmap(process1, executor=executor, return_exceptions=True)
    s.filter_exceptions(BaseException)
    s.buffer(3)
    s.parmap(process2, executor=executor, return_exceptions=True)
    s.peek()
    s.filter_exceptions(BaseException)
    assert list(s) == [1, 2, 3, 4, 5, 6]


def stream_early_stop_double(x):
    sleep(0.5)
    return x * 2


@pytest.mark.parametrize('executor', ['thread', 'process'])
def test_stream_early_stop(executor):
    s = Stream(range(300000))
    z = s.parmap(stream_early_stop_double, executor=executor)
    n = 0
    for x in z:
        # print(x)
        n += 1
        if n == 10:
            break
    assert n == 10


def test_fifo_stream():
    def delayed_double(x):
        sleep(random.uniform(0.01, 0.1))
        return x * 2

    data = list(range(100))

    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        results = fifo_stream(
            data,
            functools.partial(executor.submit, delayed_double),
            capacity=30,
        )
        results = list(results)
        assert results == [_ * 2 for _ in data]


@pytest.mark.asyncio
async def test_async_fifo_stream():
    async def delayed_double(x):
        await asyncio.sleep(random.uniform(0.01, 0.1))
        return x * 2

    async def _func(x, loop):
        return loop.create_task(delayed_double(x))

    data = list(range(100))

    async def get_data():
        for d in data:
            yield d

    results = [
        y
        async for y in async_fifo_stream(
            get_data(),
            _func,
            loop=asyncio.get_running_loop(),
        )
    ]

    assert results == [_ * 2 for _ in data]


def double(x):
    return x * 2


def test_parmap_mp():
    SYNC_INPUT = list(range(278))
    got = Stream(SYNC_INPUT).parmap(double, executor='process').collect()
    assert got == [v * 2 for v in SYNC_INPUT]


class Pad:
    def __init__(self, value: str):
        self._val = value

    def __call__(self, x):
        return f'{self._val} {x}'


padder: Pad
# A global to be used in other processes


def prepare_pad(value: str):
    global padder
    padder = Pad(value)


def pad_worker(x):
    return padder(x)


def test_parmap_initializer():
    data = Stream(range(30)).parmap(
        pad_worker,
        executor='process',
        executor_initializer=prepare_pad,
        executor_init_args=('abc',),
    )
    assert data.collect() == [f'abc {x}' for x in range(30)]


def add_four(x):
    return x + 4


def worker1(n):
    data = Stream(range(n)).parmap(add_four, executor='thread')
    return list(data)


def test_parmap_nest():
    data = Stream([10, 20, 30]).parmap(worker1, executor='process')
    assert data.collect() == [[v + 4 for v in range(n)] for n in (10, 20, 30)]


async def async_plus_2(x):
    await asyncio.sleep(random.uniform(0.5, 1))
    return x + 2


def test_parmap_async():
    data = range(1000)
    stream = Stream(data)
    stream.parmap(async_plus_2)
    t0 = perf_counter()
    for x, y in zip(data, stream):
        assert y == x + 2
    t1 = perf_counter()
    print(t1 - t0)
    assert t1 - t0 < 6
    # sequential processing would take 500+ sec

    data = list(range(20))
    data[12] = 'a'

    # Test exception in the worker function
    stream = Stream(data).parmap(async_plus_2)
    with pytest.raises(TypeError):
        for x, y in zip(data, stream):
            assert y == x + 2

    stream = Stream(data).parmap(async_plus_2, return_x=True, return_exceptions=True)
    for x, y in stream:
        if x == 'a':
            assert isinstance(y, TypeError)
        else:
            assert y == x + 2

    # Test premature quit
    stream = Stream(data).parmap(async_plus_2)
    istream = iter(stream)
    for i, x in enumerate(data):
        print(i, x)
        if i == 12:
            break
        y = next(istream)
        print(x, y)
        assert y == x + 2


class AsyncWrapper:
    def __init__(self, shift: int):
        self._shift = shift

    async def __aenter__(self):
        print(f'----- {self.__class__.__name__}.__aenter__ -----')
        self._shift += 1
        return self

    async def __aexit__(self, *args):
        print(f'----- {self.__class__.__name__}.__aexit__ -----')
        pass

    async def __call__(self, x):
        return x + self._shift


async def wrap(x, wrapper: AsyncWrapper):
    await asyncio.sleep(0.1)
    return await wrapper(x)


def test_parmap_async_context():
    print('')
    data = range(1000)
    stream = Stream(data).parmap(
        wrap,
        async_context={'wrapper': AsyncWrapper(3)},
        return_x=True,
    )
    t0 = perf_counter()
    for x, y in stream:
        assert y == x + 4
    t1 = perf_counter()
    print(t1 - t0)
    assert t1 - t0 < 2


def delayed_shift(x, shift, sleep_cap):
    sleep(random.uniform(0.0, sleep_cap))
    return x + shift


def test_tee():
    data = range(20)
    t1, t2 = tee(data, buffer_size=4)
    t1.map(lambda x: x + 2).parmap(lambda x: x + 2, executor='thread')
    t2.map(lambda x: x + 3).parmap(lambda x: x + 3, executor='thread')

    def worker(stream, prefix):
        for x in stream:
            print(prefix, '  ', x)

    with ThreadPoolExecutor() as pool:
        f1 = pool.submit(worker, t1, '**')
        f2 = pool.submit(worker, t2, '--    ')
        concurrent.futures.wait((f1, f2))

    data = range(256)
    for buffer_size in (2,):  # 1024, 64, 2):
        print('buffer size', buffer_size)
        t1, t2 = tee(data, buffer_size=buffer_size)
        t1.parmap(
            delayed_shift,
            shift=2,
            sleep_cap=0.2,
            executor='thread',
        )
        t2.parmap(
            delayed_shift,
            shift=3,
            sleep_cap=0.3,
            executor='process',
        )
        with ThreadPoolExecutor() as pool:
            f1 = pool.submit(sum, t1)
            f2 = pool.submit(sum, t2)
            assert f1.result() == sum(x + 2 for x in data)
            assert f2.result() == sum(x + 3 for x in data)


def test_eager_batcher():
    def stuff(q):
        sleep(0.2)
        q.put('OK')
        q.put(1)
        q.put(2)
        sleep(0.1)
        q.put(3)
        q.put(4)
        sleep(0.05)
        q.put(5)
        sleep(0.4)
        q.put(6)
        sleep(0.3)
        q.put(7)
        sleep(0.25)
        q.put(None)

    q = queue.Queue()
    stuffer = mpservice.threading.Thread(target=stuff, args=(q,))
    stuffer.start()
    walker = EagerBatcher(q, batch_size=3, timeout=0.2)
    q.get()
    zz = list(walker)
    print(zz)
    assert zz == [[1, 2, 3], [4, 5], [6], [7]]
