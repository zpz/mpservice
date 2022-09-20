import multiprocessing as mp
import threading
import time
from multiprocessing import current_process, active_children

import pytest
from mpservice.util import SpawnProcess, Thread
from mpservice.server_process import Manager



class MyDataServer:
    def __init__(self, inc=1):
        print('initiating {} instance in {}'.format(
            self.__class__.__name__, current_process().name))

        self._inc = inc

    def inc(self, x):
        # print(f'serving `inc({x})` in {current_process().name}')
        return x + self._inc

    def set_inc_base(self, x):
        self._inc = x

    def long_process(self, n):
        time.sleep(n)
        return n


def increment(server, inc=1):
    time.sleep(1)
    # print(f'calling `increment` in {current_process().name}')
    for x in range(10):
        assert server.inc(x) == x + inc
        # print(f'  done `inc({x})`')


def increment2(server):
    time.sleep(1)
    inc = 8.3
    server.set_inc_base(inc)
    for x in range(10):
        assert server.inc(x) == x + inc


def wait_long(server):
    time.sleep(1)
    for n in (0.4, 1.3, 2.5):
        # print(f'to wait for {n} seconds')
        nn = server.long_process(n)
        # print(f'  done waiting for {n} seconds')
        assert nn == n


def test_data_server():
    print('')

    Manager.register(MyDataServer)
    manager = Manager()
    manager.start()

    data_server = manager.MyDataServer(inc=3.2)
    data_server2 = manager.MyDataServer()

    p1 = SpawnProcess(
        target=increment,
        args=(data_server, 3.2),
    )
    p2 = SpawnProcess(
        target=wait_long,
        args=(data_server,),
    )
    p3 = SpawnProcess(
        target=increment2,
        args=(data_server2, ),
    )
    p1.start()
    p2.start()
    p3.start()

    pp = [p.name for p in active_children()]
    print('all active processes:', pp)
    assert len(pp) == 4
    # This failed when run in `./run-tests`
    # but succeeded within container.

    p1.join()
    p2.join()
    p3.join()

    pp = [p.name for p in active_children()]
    print('all active processes:', pp)
    assert len(pp) == 1

    del data_server
    pp = [p.name for p in active_children()]
    print('all active processes:', pp)
    assert len(pp) == 1

    del data_server2
    pp = [p.name for p in active_children()]
    print('all active processes:', pp)
    assert len(pp) == 1


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

