import asyncio
import time

import pytest

from mpservice.mpserver import (
    ProcessWorker, ThreadWorker, ProcessServlet, ThreadServlet, PassThrough,
    SequentialServlet, EnsembleServlet, Server, make_threadworker,
    TimeoutError
)
from mpservice.streamer import Streamer
from mpservice.util import is_remote_exception


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
    def __init__(self):
        super().__init__(batch_size=4)

    def call(self, x):
        return [v*v for v in x]


class Delay(ProcessWorker):
    def call(self, x):
        time.sleep(x)
        return x


def test_basic():
    with Server(ProcessServlet(Double, cpus=[1])) as service:
        z = service.call(3)
        assert z == 3 * 2


def test_sysinfolog():
    s = SequentialServlet(
            ProcessServlet(Double, cpus=[0, 1]),
            ProcessServlet(Square, cpus=[1]),
            EnsembleServlet(
                ProcessServlet(Double),
                ThreadServlet(make_threadworker(lambda x: x + 1)),
                ProcessServlet(Square, cpus=[1]),
                ),
            ThreadServlet(make_threadworker(lambda x: sum(x))),
            )
    with Server(s, sys_info_log_cadence=100) as service:
        s = list(service.stream(range(1000)))
        assert len(s) == 1000


@pytest.mark.asyncio
async def test_sequential_server_async():
    with Server(SequentialServlet(
            ProcessServlet(Double, cpus=[1,2]),
            ProcessServlet(Shift, cpus=[3]),
            ), sys_info_log_cadence=None) as service:
        z = await service.async_call(3)
        assert z == 3 * 2 + 3

        x = list(range(10))
        tasks = [service.async_call(v) for v in x]
        y = await asyncio.gather(*tasks)
        assert y == [v * 2 + 3 for v in x]


def test_sequential_server():
    with Server(SequentialServlet(
            ProcessServlet(Double, cpus=[1,2]),
            ProcessServlet(Shift, cpus=[3]),
            ), sys_info_log_cadence=None) as service:
        z = service.call(3)
        assert z == 3 * 2 + 3

        x = list(range(10))
        y = [service.call(v) for v in x]
        assert y == [v * 2 + 3 for v in x]


def test_sequential_batch():
    with Server(ProcessServlet(Shift, cpus=[1, 2, 3], batch_size=10, stepsize=4),
            sys_info_log_cadence=None) as service:
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
            z = await service.async_call(2.2, timeout=1)


def test_sequential_timeout():
    with Server(ProcessServlet(Delay), sys_info_log_cadence=None) as service:
        with pytest.raises(TimeoutError):
            z = service.call(2.2, timeout=1)


def test_sequential_stream():
    with Server(ProcessServlet(Square, cpus=[1, 2, 3]),
            sys_info_log_cadence=None) as service:
        data = range(100)
        ss = service.stream(data)
        assert list(ss) == [v*v for v in data]

        with Streamer(data) as s:
            s.parmap(service.call, executor='thread', num_workers=10)
            assert list(s) == [v*v for v in data]


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
            ProcessServlet(GetHead, cpus=[1,2]),
            ProcessServlet(GetTail, cpus=[3]),
            ProcessServlet(GetLen, cpus=[2])),
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
            ProcessServlet(Delay, cpus=[3])),
        ThreadServlet(PostCombine),
        )


@pytest.mark.asyncio
async def test_ensemble_timeout_async():
    with Server(your_wide_server, sys_info_log_cadence=None) as service:
        with pytest.raises(TimeoutError):
            z = await service.async_call(8.2, timeout=1)


def test_ensemble_timeout():
    with Server(your_wide_server, sys_info_log_cadence=None) as service:
        with pytest.raises(TimeoutError):
            z = service.call(8.2, timeout=1)


his_wide_server = SequentialServlet(
    EnsembleServlet(
        ProcessServlet(Shift, stepsize=1, cpus=[1], batch_size=0),
        ProcessServlet(Shift, stepsize=3, cpus=[1, 2], batch_size=1),
        ProcessServlet(Shift, stepsize=5, cpus=[0, 3], batch_size=4),
        ProcessServlet(Shift, stepsize=7, cpus=[2])),
    ThreadServlet(make_threadworker(lambda y: [min(y), max(y)])),
    )


def test_ensemble_stream():
    with Server(his_wide_server, sys_info_log_cadence=None) as service:
        data = range(100)
        ss = service.stream(data)
        assert list(ss) == [[v + 1, v + 7] for v in data]

        with Streamer(data) as s:
            s.parmap(service.call, executor='thread', num_workers=10)
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
