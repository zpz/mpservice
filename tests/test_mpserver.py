import asyncio
import pickle
import random
import time

import pytest
from mpservice import TimeoutError
from mpservice.mpserver import (
    AsyncServer,
    EnsembleError,
    EnsembleServlet,
    PassThrough,
    ProcessServlet,
    SequentialServlet,
    Server,
    SwitchServlet,
    ThreadServlet,
    Worker,
    make_worker,
)
from mpservice.multiprocessing import RemoteException, is_remote_exception
from mpservice.streamer import Stream


class Double(Worker):
    def call(self, x):
        return x * 2


class Shift(Worker):
    def __init__(self, stepsize=3, **kwargs):
        super().__init__(**kwargs)
        self._stepsize = stepsize

    def call(self, x):
        if self.batch_size == 0:
            return x + self._stepsize
        return [_ + self._stepsize for _ in x]


class Square(Worker):
    def __init__(self, **kwargs):
        super().__init__(batch_size=4, **kwargs)

    def call(self, x):
        return [v * v for v in x]


class Delay(Worker):
    def call(self, x):
        time.sleep(x)
        return x


def test_basic():
    with Server(ProcessServlet(Double, cpus=[1])) as service:
        z = service.call(3)
        assert z == 3 * 2


@pytest.mark.asyncio
async def test_sequential_server_async():
    async with AsyncServer(
        SequentialServlet(
            ProcessServlet(Double, cpus=[1, 2]),
            ProcessServlet(Shift, cpus=[3]),
        )
    ) as service:
        z = await service.call(3)
        assert z == 3 * 2 + 3

        print('debug_info:', service.debug_info())

        x = list(range(10))
        tasks = [service.call(v) for v in x]
        y = await asyncio.gather(*tasks)
        assert y == [v * 2 + 3 for v in x]
        print('debug_info:', service.debug_info())


def test_sequential_server():
    with Server(
        SequentialServlet(
            ProcessServlet(Double, cpus=[1, 2]),
            ProcessServlet(Shift, cpus=[3]),
        )
    ) as service:
        z = service.call(3)
        assert z == 3 * 2 + 3

        print('debug_info:', service.debug_info())

        x = list(range(10))
        y = [service.call(v) for v in x]
        assert y == [v * 2 + 3 for v in x]
        print('debug_info:', service.debug_info())


def test_sequential_batch():
    with Server(
        ProcessServlet(Shift, cpus=[1, 2, 3], batch_size=10, stepsize=4)
    ) as service:
        z = service.call(3)
        assert z == 3 + 4

        x = list(range(111))
        y = [service.call(v) for v in x]
        assert y == [v + 4 for v in x]


def test_sequential_error():
    s1 = ProcessServlet(Double, cpus=[1, 2])
    s2 = ProcessServlet(Shift, cpus=[3], stepsize=4)
    with Server(SequentialServlet(s1, s2)) as service:
        z = service.call(3)
        assert z == 3 * 2 + 4

        print('debug_info:', service.debug_info())
        with pytest.raises(TypeError):
            z = service.call('a')


@pytest.mark.asyncio
async def test_sequential_timeout_async():
    async with AsyncServer(ProcessServlet(Delay)) as service:
        with pytest.raises(TimeoutError):
            await service.call(2.2, timeout=1)


def test_sequential_timeout():
    with Server(ProcessServlet(Delay)) as service:
        with pytest.raises(TimeoutError):
            service.call(2.2, timeout=1)


def test_sequential_stream():
    with Server(ProcessServlet(Square, cpus=[1, 2, 3])) as service:
        data = range(100)
        ss = service.stream(data)
        assert list(ss) == [v * v for v in data]

        s = Stream(data).parmap(service.call, executor='thread', num_workers=10)
        assert list(s) == [v * v for v in data]


def test_stream_error():
    with Server(ProcessServlet(Square, cpus=[1, 2, 3])) as service:
        data = list(range(30))
        data[22] = 'a'
        ss = service.stream(data)
        with pytest.raises(TypeError):
            assert list(ss) == [v * v for v in data]


def test_stream_early_quit():
    with Server(ProcessServlet(Square, cpus=[1, 2, 3]), capacity=10) as service:
        data = range(100)
        ss = service.stream(data)
        n = 0
        for x, y in zip(data, ss):
            assert y == x * x
            n += 1
            if n > 33:
                break


@pytest.mark.asyncio
async def test_sequential_async_stream():
    async def data():
        for x in range(100):
            yield x

    async with AsyncServer(ProcessServlet(Square, cpus=[1, 2, 3])) as service:
        ss = service.stream(data())
        assert [v async for v in ss] == [v * v for v in range(100)]
        s = Stream(data()).parmap(service.call, num_workers=50, _async=True)
        assert (await s.collect()) == [v * v for v in range(100)]


@pytest.mark.asyncio
async def test_async_stream_error():
    async def adata():
        for x in range(30):
            if x == 22:
                yield 'a'
            else:
                yield x

    async with AsyncServer(ProcessServlet(Square, cpus=[1, 2, 3])) as service:
        data = list(range(30))
        data[22] = 'a'
        ss = service.stream(adata())
        with pytest.raises(TypeError):
            assert [v async for v in ss] == [v * v for v in data]


@pytest.mark.asyncio
async def test_async_stream_early_quit():
    async def data():
        for x in range(100):
            yield x

    async with AsyncServer(
        ProcessServlet(Square, cpus=[1, 2, 3]), capacity=10
    ) as service:
        ss = service.stream(data(), return_x=True)
        n = 0
        async for x, y in ss:
            assert y == x * x
            n += 1
            if n > 33:
                break


class GetHead(Worker):
    def call(self, x):
        return x[0]


class GetTail(Worker):
    def call(self, x):
        return x[-1]


class GetLen(Worker):
    def call(self, x):
        return len(x)


my_wide_server = SequentialServlet(
    EnsembleServlet(
        ProcessServlet(GetHead, cpus=[1, 2]),
        ProcessServlet(GetTail, cpus=[3]),
        ProcessServlet(GetLen, cpus=[2]),
    ),
    ThreadServlet(make_worker(lambda x: (x[0] + x[1]) * x[2])),
)


@pytest.mark.asyncio
async def test_ensemble_server_async():
    async with AsyncServer(my_wide_server) as service:
        z = await service.call('abcde')
        assert z == 'aeaeaeaeae'

        x = ['xyz', 'abc', 'jk', 'opqs']
        tasks = [service.call(v) for v in x]
        y = await asyncio.gather(*tasks)
        assert y == ['xzxzxz', 'acacac', 'jkjk', 'osososos']


def test_ensemble_server():
    with Server(my_wide_server) as service:
        z = service.call('abcde')
        assert z == 'aeaeaeaeae'

        x = ['xyz', 'abc', 'jk', 'opqs']
        y = [service.call(v) for v in x]
        assert y == ['xzxzxz', 'acacac', 'jkjk', 'osososos']


class AddThree(Worker):
    def call(self, x):
        return x + 3


class PostCombine(Worker):
    def call(self, x):
        x, *y = x
        return x + y[0] + y[1]


your_wide_server = SequentialServlet(
    EnsembleServlet(
        ThreadServlet(PassThrough),
        ProcessServlet(AddThree, cpus=[1, 2]),
        ProcessServlet(Delay, cpus=[3]),
    ),
    ThreadServlet(PostCombine),
)


@pytest.mark.asyncio
async def test_ensemble_timeout_async():
    async with AsyncServer(your_wide_server) as service:
        with pytest.raises(TimeoutError):
            await service.call(8.2, timeout=1)


def test_ensemble_timeout():
    with Server(your_wide_server) as service:
        with pytest.raises(TimeoutError):
            service.call(8.2, timeout=1)


his_wide_server = SequentialServlet(
    EnsembleServlet(
        ProcessServlet(Shift, stepsize=1, cpus=[1], batch_size=0),
        ProcessServlet(Shift, stepsize=3, cpus=[1, 2], batch_size=1),
        ProcessServlet(Shift, stepsize=5, cpus=[0, 3], batch_size=4),
        ProcessServlet(Shift, stepsize=7, cpus=[2]),
    ),
    ThreadServlet(make_worker(lambda y: [min(y), max(y)])),
)


def test_ensemble_stream():
    with Server(his_wide_server) as service:
        data = range(100)
        ss = service.stream(data)
        assert list(ss) == [[v + 1, v + 7] for v in data]

        s = Stream(data).parmap(service.call, executor='thread', num_workers=10)
        assert list(s) == [[v + 1, v + 7] for v in data]


class AddOne(Worker):
    def call(self, x):
        time.sleep(0.3)
        return x + 1


class AddFive(Worker):
    def call(self, x):
        time.sleep(0.1)
        return x + 5


class TakeMean(Worker):
    def call(self, x):
        return sum(x) / len(x)


def test_thread():
    s1 = ThreadServlet(AddOne, num_threads=3)
    s2 = ThreadServlet(AddFive)
    s3 = ThreadServlet(TakeMean)
    s = SequentialServlet(EnsembleServlet(s1, s2), s3)
    with Server(s) as service:
        assert service.call(3) == 6
        for x, y in service.stream(range(100), return_x=True):
            assert y == x + 3


class Error1(Exception):
    pass


class Error2(Exception):
    pass


class Error3(Exception):
    pass


class Error4(Exception):
    pass


class Error5(Exception):
    pass


class Worker1(Worker):
    def call(self, x):
        if x == 1:
            raise Error1(x)
        return x


class Worker2(Worker):
    def call(self, x):
        if x == 2:
            raise Error2(x)
        return x


class Worker3(Worker):
    def call(self, x):
        if x == 3:
            raise Error3(x)
        return x


class Worker4(Worker):
    def call(self, x):
        if x == 4:
            raise Error4(x)
        return x


class Worker5(Worker):
    def call(self, x):
        if x == 5:
            raise Error5(x)
        return x


def test_exceptions():
    s = SequentialServlet(
        ProcessServlet(Worker1),
        ProcessServlet(Worker2),
        ProcessServlet(Worker3),
        ProcessServlet(Worker4),
        ProcessServlet(Worker5),
    )
    with Server(s) as server:
        assert server.call(0) == 0
        with pytest.raises(Error1):
            server.call(1)
        with pytest.raises(Error2):
            server.call(2)
        with pytest.raises(Error3):
            server.call(3)

        try:
            server.call(3)
        except Exception as e:
            assert is_remote_exception(e)

        with pytest.raises(Error4):
            server.call(4)
        with pytest.raises(Error5):
            server.call(5)
        assert server.call(6) == 6


def test_ensemble_error():
    err1 = None
    try:
        raise ValueError(20)
    except Exception as e:
        err1 = e

    z = {
        'y': [38, None, RemoteException(err1), None],
        'n': 2,
    }
    e = EnsembleError(z)
    print('')
    print(repr(e))
    print(str(e))
    print(e.args)
    print(e.args[0])
    print(e.args[1])
    print('')
    ep = pickle.loads(pickle.dumps(e))
    print(ep)
    print(e.args)


def test_ensemble_error2():
    s = EnsembleServlet(
        ProcessServlet(Double),
        ThreadServlet(make_worker(lambda x: x + 1)),
        ProcessServlet(Square, cpus=[1]),
    )
    with Server(s) as service:
        y = service.call(3)
        assert y == [6, 4, 9]

        with pytest.raises(EnsembleError):
            try:
                try:
                    y = service.call('a')
                except EnsembleError as e:
                    print(repr(e))
                    print(e)
                    print(e.args)
                    print('')
                    raise pickle.loads(pickle.dumps(RemoteException(e)))
            except EnsembleError as e:
                print(repr(e))
                print(e)
                print(e.args)
                raise


def test_switch_servlet():
    class MySwitch(SwitchServlet):
        def __init__(self):
            super().__init__(
                ThreadServlet(PassThrough),
                ProcessServlet(Double),
                ThreadServlet(AddFive),
                ProcessServlet(Worker3),
            )

        def switch(self, x):
            if x > 100:
                return 0
            if x > 50:
                return 1
            if x > 10:
                return 2
            return 3

    with Server(MySwitch()) as service:
        assert service.call(123) == 123
        assert service.call(62) == 124
        assert service.call(30) == 35
        assert service.call(10) == 10
        assert service.call(7) == 7

        with pytest.raises(Error3):
            service.call(3)


class DelayedShift(Worker):
    def __init__(self, *, delay=1, shift=2, **kwargs):
        super().__init__(**kwargs)
        self._delay = delay
        self._shift = shift

    def call(self, x):
        time.sleep(self._delay)
        return x + self._shift


@pytest.mark.asyncio
async def test_cancel():
    async with AsyncServer(
        SequentialServlet(
            ProcessServlet(DelayedShift, delay=1, shift=2),
            ProcessServlet(DelayedShift, delay=1, shift=3),
        )
    ) as server:
        x = 3
        y = await server.call(x)
        assert y == x + 5

        try:
            y = await asyncio.wait_for(
                server.call(x),
                0.5,
            )
        except asyncio.TimeoutError:
            pass
        assert server.backlog == 1
        await asyncio.sleep(2)
        assert server.backlog == 0

        with pytest.raises(TimeoutError):
            y = await server.call(x, timeout=0.6)
        assert server.backlog == 1
        await asyncio.sleep(0.5)
        assert server.backlog == 1
        # the 2nd servlet still runs, hence this is 1.

    async with AsyncServer(
        SequentialServlet(
            ProcessServlet(DelayedShift, delay=1, shift=2),
            EnsembleServlet(
                ProcessServlet(DelayedShift, delay=1, shift=2),
                ThreadServlet(DelayedShift, delay=0.8, shift=3),
                ProcessServlet(DelayedShift, delay=1.2, shift=4),
            ),
        )
    ) as server:
        assert await server.call(2) == [6, 7, 8]
        assert server.backlog == 0

        try:
            y = await asyncio.wait_for(
                server.call(x),
                1.1,
            )
        except asyncio.TimeoutError:
            pass
        assert server.backlog == 1  # the ensemble step is running
        await asyncio.sleep(0.65)  # total 1.75 sec
        assert server.backlog == 1  # the ensemble step is running
        await asyncio.sleep(0.1)  # total 1.85 sec
        assert server.backlog == 1

        with pytest.raises(TimeoutError):
            y = await server.call(2, timeout=0.7)
        assert (
            server.backlog == 1
        )  # the first servlet is already underway; wait for it to finish
        time.sleep(0.31)  # total 1.01 sec
        assert (
            server.backlog == 1
        )  # the ensemble servlet stills runs even though the item is already cancelled when the servlet receives it.


class RandomDelayedShift(Worker):
    def __init__(self, shift, sleep_cap=0.5, **kwargs):
        super().__init__(**kwargs)
        self._shift = shift
        self._sleep_cap = sleep_cap

    def call(self, x):
        time.sleep(random.random() * self._sleep_cap)
        return x + self._shift


class WorkerWithPreprocess(Worker):
    def preprocess(self, x):
        if x > 100:
            raise ValueError(x)
        return x

    def call(self, x):
        if self.batch_size:
            return [v + 3 for v in x]
        return x + 3


def test_preprocess():
    server = Server(ProcessServlet(WorkerWithPreprocess))
    with server:
        assert server.call(8) == 11
        with pytest.raises(ValueError):
            server.call(123)

    server = Server(
        ProcessServlet(WorkerWithPreprocess, batch_size=10, batch_wait_time=0.1)
    )
    with server:
        for x, y in server.stream(range(200), return_x=True, return_exceptions=True):
            if x > 100:
                assert isinstance(y, ValueError)
            else:
                assert y == x + 3


class FailableWorker(Worker):
    def __init__(self, x, **kwargs):
        super().__init__(**kwargs)
        if x > 10:
            raise ValueError(x)
        self._x = x

    def call(self, x):
        if self.batch_size:
            return [_ + self._x for _ in x]
        return x + self._x
    

def test_worker_init_failure():
    server = Server(ProcessServlet(FailableWorker, x=3))
    with server:
        assert server.call(8) == 11

    server = Server(ProcessServlet(FailableWorker, x=30))
    with pytest.raises(ValueError):
        with server:
            assert server.call(8) == 38

    server = Server(ThreadServlet(FailableWorker, x=30))
    with pytest.raises(ValueError):
        with server:
            assert server.call(8) == 38