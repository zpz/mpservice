import multiprocessing as mp
import threading
import time
from mpservice.managers import Manager
from mpservice.util import SpawnProcess, Thread


class Doubler:
    def __init__(self, name):
        self.name = name

    def get_name(self):
        return self.name

    def scale(self, x):
        return x * 2

    def get_mp(self):
        return mp.current_process().name

    def sleep(self, n):
        print(type(self).__name__, id(self), mp.current_process().name, threading.current_thread().name, 'to sleep')
        time.sleep(n)
        print('  ', type(self).__name__, id(self), mp.current_process().name, threading.current_thread().name, 'done sleeping')
        return n


class Tripler:
    def __init__(self, name):
        self.name = name

    def get_name(self):
        return self.name

    def scale(self, x):
        return x * 3

    def get_mp(self):
        return mp.current_process().name

    def sleep(self, n):
        print(type(self).__name__, id(self), mp.current_process().name, threading.current_thread().name, 'to sleep')
        time.sleep(n)
        print('  ', type(self).__name__, id(self), mp.current_process().name, threading.current_thread().name, 'done sleeping')
        return n


def test_manager():
    Manager.register(Doubler)
    Manager.register(Tripler)

    manager = Manager()
    manager.start()

    doubler = manager.Doubler('d')
    print(doubler.get_mp())
    assert doubler.get_name() == 'd'
    assert doubler.scale(3) == 6

    tripler = manager.Tripler('t')
    print(tripler.get_mp())
    assert tripler.get_name() == 't'
    assert tripler.scale(3) == 9

    assert doubler.get_mp() == tripler.get_mp()

    manager2 = Manager()
    manager2.start()

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


def worker(sleeper, n):
    print('worker in', mp.current_process().name)
    sleeper.sleep(n)
    sleeper.sleep(n)


def test_concurrency():
    print('')
    Manager.register(Doubler)
    Manager.register(Tripler)
    m = Manager()
    m.start()

    d = m.Doubler('d')
    t = m.Tripler('t')

    for cls in (SpawnProcess, Thread):
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

