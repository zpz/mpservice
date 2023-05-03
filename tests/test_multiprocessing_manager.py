import multiprocessing as mp
import threading
import time
from multiprocessing import active_children

import pytest
from mpservice.multiprocessing import CpuAffinity, Manager, SpawnProcess
from mpservice.threading import Thread


class Doubler:
    def __init__(self, name):
        self.name = name

    @property
    def myname(self):
        return self.name

    def get_name(self):
        return self.name

    def scale(self, x):
        return x * 2

    def get_mp(self):
        return mp.current_process().name

    def get_tr(self):
        return threading.current_thread().name

    def get_id(self):
        return id(self)

    def sleep(self, n):
        print(
            type(self).__name__,
            id(self),
            mp.current_process().name,
            threading.current_thread().name,
            'to sleep',
        )
        time.sleep(n)
        print(
            '  ',
            type(self).__name__,
            id(self),
            mp.current_process().name,
            threading.current_thread().name,
            'done sleeping',
        )
        return n


class Tripler:
    def __init__(self, name):
        self.name = name

    @property
    def myname(self):
        return self.name

    def get_name(self):
        return self.name

    def scale(self, x):
        return x * 3

    def get_mp(self):
        return mp.current_process().name

    def get_tr(self):
        return threading.current_thread().name

    def get_id(self):
        return id(self)

    def sleep(self, n):
        print(
            type(self).__name__,
            id(self),
            mp.current_process().name,
            threading.current_thread().name,
            'to sleep',
        )
        time.sleep(n)
        print(
            '  ',
            type(self).__name__,
            id(self),
            mp.current_process().name,
            threading.current_thread().name,
            'done sleeping',
        )
        return n


Manager.register(Doubler)
Manager.register(Tripler)


def test_manager():
    with Manager(process_cpu=1, process_name='my_manager_process') as manager:
        assert manager._process.name == 'my_manager_process'
        assert CpuAffinity.get(pid=manager._process.pid) == [1]

        doubler = manager.Doubler('d')
        print(doubler.get_mp())
        assert doubler.get_name() == 'd'
        assert doubler.scale(3) == 6
        with pytest.raises(AttributeError):
            # 'properties' do not work
            assert doubler.myname == 'd'

        tripler = manager.Tripler('t')
        print(tripler.get_mp())
        assert tripler.get_name() == 't'
        assert tripler.scale(3) == 9
        with pytest.raises(AttributeError):
            assert doubler.myname == 't'

        assert doubler.get_mp() == tripler.get_mp()
        assert doubler.get_tr() == tripler.get_tr()

        with Manager() as manager2:
            doubler2 = manager2.Doubler('dd')
            print(doubler2.get_mp())
            assert doubler2.get_name() == 'dd'
            assert doubler2.scale(4) == 8

            assert doubler2.get_mp() != doubler.get_mp()

            doubler3 = manager2.Doubler('ddd')
            print(doubler3.get_mp())
            assert doubler3.get_name() == 'ddd'
            assert doubler3.scale(5) == 10

            assert doubler3.get_mp() == doubler2.get_mp()

    with pytest.warns(UserWarning):
        Manager.register(Doubler)  # this will trigger a warning log.


def worker(sleeper, n):
    print('worker in', mp.current_process().name)
    sleeper.sleep(n)
    sleeper.sleep(n)


def test_concurrency():
    print('')
    with Manager() as manager:
        d = manager.Doubler('d')
        t = manager.Tripler('t')

        for cls in (SpawnProcess, Thread):
            print('cls:', cls)
            print('')
            pp = [
                cls(target=worker, args=(d, 3)),
                cls(target=worker, args=(t, 3)),
            ]
            t0 = time.perf_counter()
            for p in pp:
                p.start()
            for p in pp:
                p.result()
            t1 = time.perf_counter()
            print('took', t1 - t0, 'seconds')
            assert 6 < t1 - t0 < 7

            print('')
            pp = [
                cls(target=worker, args=(d, 3)),
                cls(target=worker, args=(d, 3)),
                cls(target=worker, args=(d, 3)),
            ]
            t0 = time.perf_counter()
            for p in pp:
                p.start()
            for p in pp:
                p.result()
            t1 = time.perf_counter()
            print('took', t1 - t0, 'seconds')
            assert 6 < t1 - t0 < 7

            # TODO: running in container, the value below is 1; running for release,
            # the value is 2 or 3 when doing 'process'.
            # But this is a recent change. Previously it's always 1.
            # assert len(active_children()) in (1, 2, 3)
            assert len(active_children()) == 1



