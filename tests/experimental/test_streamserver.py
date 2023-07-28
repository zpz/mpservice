import time

import pytest
from mpservice.mpserver import (
    EnsembleServlet,
    ProcessServlet,
    SequentialServlet,
    ThreadServlet,
    Worker,
    make_worker,
)
from mpservice.experimental.mpserver import StreamServer


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



def test_sequential_streamserver():
    with StreamServer(ProcessServlet(Square, cpus=[1, 2, 3])) as service:
        data = range(100)
        ss = service.stream(data)
        assert list(ss) == [v * v for v in data]



def test_streamserver_error():
    with StreamServer(ProcessServlet(Square, cpus=[1, 2, 3])) as service:
        data = list(range(30))
        data[22] = 'a'
        ss = service.stream(data)
        with pytest.raises(TypeError):
            assert list(ss) == [v * v for v in data]



def test_streamserver_early_quit():
    with StreamServer(ProcessServlet(Square, cpus=[1, 2, 3]), capacity=10) as service:
        data = range(100)
        ss = service.stream(data)
        n = 0
        for x, y in zip(data, ss):
            assert y == x * x
            n += 1
            if n > 33:
                break




his_wide_server = SequentialServlet(
    EnsembleServlet(
        ProcessServlet(Shift, stepsize=1, cpus=[1], batch_size=0),
        ProcessServlet(Shift, stepsize=3, cpus=[1, 2], batch_size=1),
        ProcessServlet(Shift, stepsize=5, cpus=[0, 3], batch_size=4),
        ProcessServlet(Shift, stepsize=7, cpus=[2]),
    ),
    ThreadServlet(make_worker(lambda y: [min(y), max(y)])),
)


def test_ensemble_streamserver():
    with StreamServer(his_wide_server) as service:
        data = range(100)
        ss = service.stream(data)
        assert list(ss) == [[v + 1, v + 7] for v in data]


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



def test_thread_streamserver():
    s1 = ThreadServlet(AddOne, num_threads=3)
    s2 = ThreadServlet(AddFive)
    s3 = ThreadServlet(TakeMean)
    s = SequentialServlet(EnsembleServlet(s1, s2), s3)
    with StreamServer(s) as service:
        for x, y in service.stream(range(100), return_x=True):
            assert y == x + 3




class WorkerWithPreprocess(Worker):
    def preprocess(self, x):
        if x > 100:
            raise ValueError(x)
        return x

    def call(self, x):
        if self.batch_size:
            return [v + 3 for v in x]
        return x + 3



def test_preprocess_streamserver():
    server = StreamServer(
        ProcessServlet(WorkerWithPreprocess, batch_size=10, batch_wait_time=0.1)
    )
    with server:
        for x, y in server.stream(range(200), return_x=True, return_exceptions=True):
            if x > 100:
                assert isinstance(y, ValueError)
            else:
                assert y == x + 3
