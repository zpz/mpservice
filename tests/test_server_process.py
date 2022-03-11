import multiprocessing
import time
from multiprocessing import current_process, active_children

from mpservice.server_process import ServerProcess


class MyDataServer(ServerProcess):
    def __init__(self, inc=1):
        print('initiating {} instance in {}'.format(
            self.__class__.__name__, current_process().name))

        self._inc = inc

    def inc(self, x):
        print(f'serving `inc({x})` in {current_process().name}')
        return x + self._inc

    def set_inc_base(self, x):
        self._inc = x

    def long_process(self, n):
        time.sleep(n)
        return n


def increment(server, inc=1):
    time.sleep(1)
    print(f'calling `increment` in {current_process().name}')
    for x in range(10):
        assert server.inc(x) == x + inc
        print(f'  done `inc({x})`')


def increment2(server):
    time.sleep(1)
    inc = 8.3
    server.set_inc_base(inc)
    for x in range(10):
        assert server.inc(x) == x + inc
        print(f'  done `inc({x})`')


def wait_long(server):
    time.sleep(1)
    for n in (0.4, 1.3, 2.5):
        print(f'to wait for {n} seconds')
        nn = server.long_process(n)
        print(f'  done waiting for {n} seconds')
        assert nn == n


def test_data_server():
    print('')
    print('starting test in %s' % current_process().name)
    data_server = MyDataServer.start(inc=3.2)
    data_server2 = MyDataServer.start()

    mp = multiprocessing.get_context('spawn')

    p1 = mp.Process(
        target=increment,
        args=(data_server, 3.2),
    )
    p2 = mp.Process(
        target=wait_long,
        args=(data_server,),
    )
    p3 = mp.Process(
        target=increment2,
        args=(data_server2, ),
    )
    p1.start()
    p2.start()
    p3.start()

    pp = [p.name for p in active_children()]
    print('all active processes:', pp)
    assert len(pp) == 5
    # This failed when run in `./run-tests`
    # but succeeded within container.

    p1.join()
    p2.join()
    p3.join()

    pp = [p.name for p in active_children()]
    print('all active processes:', pp)
    assert len(pp) == 2

    del data_server
    pp = [p.name for p in active_children()]
    print('all active processes:', pp)
    assert len(pp) == 1

    del data_server2
    pp = [p.name for p in active_children()]
    print('all active processes:', pp)
    assert len(pp) == 0
