import asyncio
import pickle
import time

import pytest
from mpservice.mpserver import (
    EnsembleError,
    EnsembleServlet,
    PassThrough,
    ProcessServlet,
    ProcessWorker,
    SequentialServlet,
    Server,
    SwitchServlet,
    ThreadServlet,
    ThreadWorker,
    TimeoutError,
    make_threadworker,
)
from mpservice.multiprocessing import RemoteException, is_remote_exception
from mpservice.streamer import Stream


class Double(ProcessWorker):
    def call(self, x):
        return x * 2


class Shift(ProcessWorker):
    def __init__(self, stepsize=3, **kwargs):
        super().__init__(**kwargs)
        self._stepsize = stepsize

    def call(self, x):
        if self.batch_size == 0:
            return x + self._stepsize
        return [_ + self._stepsize for _ in x]


class Square(ProcessWorker):
    def __init__(self, **kwargs):
        super().__init__(batch_size=4, **kwargs)

    def call(self, x):
        return [v * v for v in x]


class Delay(ProcessWorker):
    def call(self, x):
        time.sleep(x)
        return x


def test_basic():
    with Server(ProcessServlet(Double, cpus=[1])) as service:
        z = service.call(3)
        assert z == 3 * 2


@pytest.mark.asyncio
async def test_sequential_server_async():
    with Server(
        SequentialServlet(
            ProcessServlet(Double, cpus=[1, 2]),
            ProcessServlet(Shift, cpus=[3]),
        )
    ) as service:
        z = await service.async_call(3)
        assert z == 3 * 2 + 3

        x = list(range(10))
        tasks = [service.async_call(v) for v in x]
        y = await asyncio.gather(*tasks)
        assert y == [v * 2 + 3 for v in x]


def test_sequential_server():
    with Server(
        SequentialServlet(
            ProcessServlet(Double, cpus=[1, 2]),
            ProcessServlet(Shift, cpus=[3]),
        )
    ) as service:
        z = service.call(3)
        assert z == 3 * 2 + 3

        x = list(range(10))
        y = [service.call(v) for v in x]
        assert y == [v * 2 + 3 for v in x]


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
    with Server(SequentialServlet(s1, s2), sys_info_log_cadence=None) as service:
        z = service.call(3)
        assert z == 3 * 2 + 4

        with pytest.raises(TypeError):
            z = service.call('a')


@pytest.mark.asyncio
async def test_sequential_timeout_async():
    with Server(ProcessServlet(Delay), sys_info_log_cadence=None) as service:
        with pytest.raises(TimeoutError):
            await service.async_call(2.2, timeout=1)


def test_sequential_timeout():
    with Server(ProcessServlet(Delay), sys_info_log_cadence=None) as service:
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
    with Server(ProcessServlet(Square, cpus=[1, 2, 3]), backlog=10) as service:
        data = range(100)
        ss = service.stream(data)
        n = 0
        for x, y in zip(data, ss):
            assert y == x * x
            n += 1
            if n > 33:
                break


class GetHead(ProcessWorker):
    def call(self, x):
        return x[0]


class GetTail(ProcessWorker):
    def call(self, x):
        return x[-1]


class GetLen(ProcessWorker):
    def call(self, x):
        return len(x)


my_wide_server = SequentialServlet(
    EnsembleServlet(
        ProcessServlet(GetHead, cpus=[1, 2]),
        ProcessServlet(GetTail, cpus=[3]),
        ProcessServlet(GetLen, cpus=[2]),
    ),
    ThreadServlet(make_threadworker(lambda x: (x[0] + x[1]) * x[2])),
)


@pytest.mark.asyncio
async def test_ensemble_server_async():
    with Server(my_wide_server, sys_info_log_cadence=None) as service:
        z = await service.async_call('abcde')
        assert z == 'aeaeaeaeae'

        x = ['xyz', 'abc', 'jk', 'opqs']
        tasks = [service.async_call(v) for v in x]
        y = await asyncio.gather(*tasks)
        assert y == ['xzxzxz', 'acacac', 'jkjk', 'osososos']


def test_ensemble_server():
    with Server(my_wide_server, sys_info_log_cadence=None) as service:
        z = service.call('abcde')
        assert z == 'aeaeaeaeae'

        x = ['xyz', 'abc', 'jk', 'opqs']
        y = [service.call(v) for v in x]
        assert y == ['xzxzxz', 'acacac', 'jkjk', 'osososos']


class AddThree(ProcessWorker):
    def call(self, x):
        return x + 3


class PostCombine(ThreadWorker):
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
    with Server(your_wide_server, sys_info_log_cadence=None) as service:
        with pytest.raises(TimeoutError):
            await service.async_call(8.2, timeout=1)


def test_ensemble_timeout():
    with Server(your_wide_server, sys_info_log_cadence=None) as service:
        with pytest.raises(TimeoutError):
            service.call(8.2, timeout=1)


his_wide_server = SequentialServlet(
    EnsembleServlet(
        ProcessServlet(Shift, stepsize=1, cpus=[1], batch_size=0),
        ProcessServlet(Shift, stepsize=3, cpus=[1, 2], batch_size=1),
        ProcessServlet(Shift, stepsize=5, cpus=[0, 3], batch_size=4),
        ProcessServlet(Shift, stepsize=7, cpus=[2]),
    ),
    ThreadServlet(make_threadworker(lambda y: [min(y), max(y)])),
)


def test_ensemble_stream():
    with Server(his_wide_server, sys_info_log_cadence=None) as service:
        data = range(100)
        ss = service.stream(data)
        assert list(ss) == [[v + 1, v + 7] for v in data]

        s = Stream(data).parmap(service.call, executor='thread', num_workers=10)
        assert list(s) == [[v + 1, v + 7] for v in data]


class AddOne(ThreadWorker):
    def call(self, x):
        time.sleep(0.3)
        return x + 1


class AddFive(ThreadWorker):
    def call(self, x):
        time.sleep(0.1)
        return x + 5


class TakeMean(ThreadWorker):
    def call(self, x):
        return sum(x) / len(x)


def test_thread():
    s1 = ThreadServlet(AddOne, num_threads=3)
    s2 = ThreadServlet(AddFive)
    s3 = ThreadServlet(TakeMean)
    s = SequentialServlet(EnsembleServlet(s1, s2), s3)
    with Server(s, sys_info_log_cadence=None) as service:
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


class Worker1(ProcessWorker):
    def call(self, x):
        if x == 1:
            raise Error1(x)
        return x


class Worker2(ProcessWorker):
    def call(self, x):
        if x == 2:
            raise Error2(x)
        return x


class Worker3(ProcessWorker):
    def call(self, x):
        if x == 3:
            raise Error3(x)
        return x


class Worker4(ProcessWorker):
    def call(self, x):
        if x == 4:
            raise Error4(x)
        return x


class Worker5(ProcessWorker):
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
    with Server(s, sys_info_log_cadence=None) as server:
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
        ThreadServlet(make_threadworker(lambda x: x + 1)),
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


class DelayedShift(ProcessWorker, ThreadWorker):
    def __init__(self, *, delay=1, shift=2, **kwargs):
        super().__init__(**kwargs)
        self._delay = delay
        self._shift = shift

    def call(self, x):
        time.sleep(self._delay)
        return x + self._shift


@pytest.mark.asyncio
async def test_cancel():
    with Server(
        SequentialServlet(
            ProcessServlet(DelayedShift, delay=1, shift=2),
            ProcessServlet(DelayedShift, delay=1, shift=3),
        )
    ) as server:
        x = 3
        y = await server.async_call(x)
        assert y == x + 5

        try:
            y = await asyncio.wait_for(
                server.async_call(x),
                0.5,
            )
        except asyncio.TimeoutError:
            pass
        assert server.backlog == 1
        time.sleep(2)
        assert server.backlog == 0

        with pytest.raises(TimeoutError):
            y = server.call(x, timeout=0.6)
        assert server.backlog == 1
        await asyncio.sleep(0.5)
        assert (
            server.backlog == 0
        )  # the 2nd servlet didn't run, otherwise this would be 1 at this moment

    with Server(
        SequentialServlet(
            ProcessServlet(DelayedShift, delay=1, shift=2),
            EnsembleServlet(
                ProcessServlet(DelayedShift, delay=1, shift=2),
                ThreadServlet(DelayedShift, delay=0.8, shift=3),
                ProcessServlet(DelayedShift, delay=1.2, shift=4),
            ),
        )
    ) as server:
        assert server.call(2) == [6, 7, 8]
        assert server.backlog == 0

        try:
            y = await asyncio.wait_for(
                server.async_call(x),
                1.1,
            )
        except asyncio.TimeoutError:
            pass
        assert server.backlog == 1  # the ensemble step is running
        await asyncio.sleep(0.65)  # total 1.75 sec
        assert server.backlog == 1  # the ensemble step is running
        await asyncio.sleep(0.1)  # total 1.85 sec
        assert (
            server.backlog == 0
        )  # the ensemble step finished thanks to short-circut when its 2nd member returns

        with pytest.raises(TimeoutError):
            y = server.call(2, timeout=0.7)
        assert (
            server.backlog == 1
        )  # the first servlet is already underway; wait for it to finish
        time.sleep(0.31)  # total 1.01 sec
        assert (
            server.backlog == 0
        )  # the ensemble servlet did not run because the item is already cancelled when the servlet receives it.
