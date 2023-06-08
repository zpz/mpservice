import multiprocessing as mp
import threading
import time
from multiprocessing import active_children

import pytest
from mpservice.multiprocessing import Process, Queue, ServerProcess, SpawnProcess
from mpservice.multiprocessing._server_process import (
    MemoryBlock,
    hosted,
)
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


ServerProcess.register('Doubler', Doubler)
ServerProcess.register('Tripler', Tripler)


def test_manager():
    with ServerProcess(name='test_server_process') as manager:
        assert manager._manager._process.name == 'test_server_process'

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

        with ServerProcess(name='test_server_process_2') as manager2:
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
        ServerProcess.register('Doubler', Doubler)  # this will trigger a warning log.


def worker(sleeper, n):
    print('worker in', mp.current_process().name)
    sleeper.sleep(n)
    sleeper.sleep(n)


def test_concurrency():
    print('')
    with ServerProcess(name='test_concurrency') as manager:
        d = manager.Doubler('d')
        t = manager.Tripler('t')

        for cls in (SpawnProcess, Thread):
            print('cls:', cls)
            print('')
            pp = [
                cls(target=worker, args=(d, 3), name=f"{cls.__name__}-1"),
                cls(target=worker, args=(t, 3), name=f"{cls.__name__}-2"),
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
                cls(target=worker, args=(d, 3), name=f"{cls.__name__}-3"),
                cls(target=worker, args=(d, 3), name=f"{cls.__name__}-4"),
                cls(target=worker, args=(d, 3), name=f"{cls.__name__}-5"),
            ]
            t0 = time.perf_counter()
            for p in pp:
                p.start()
            for p in pp:
                p.result()
            t1 = time.perf_counter()
            print('took', t1 - t0, 'seconds')
            assert 6 < t1 - t0 < 7
            assert len(active_children()) == 1


def inc_worker(q):
    time.sleep(1)
    mem = q.get()
    assert mem.size == 10
    buf = mem.buf
    assert len(buf) == 10
    assert buf[4] == 100
    buf[4] += 1

    blocks = mem.list_memory_blocks()
    print('memory blocks in worker:', blocks)
    assert len(blocks) == 1


def test_shared_memory():
    print('')
    with ServerProcess() as manager:
        mem = manager.MemoryBlock(10)
        assert type(mem.buf) is memoryview
        assert len(mem.buf) == 10
        mem.buf[4] = 100

        blocks = manager.list_memory_blocks()
        print('memory blocks in main:', blocks)
        assert len(blocks) == 1

        q = Queue()
        q.put(mem)
        del mem

        blocks = manager.list_memory_blocks()
        assert len(blocks) == 1

        p = Process(target=inc_worker, args=(q,))
        p.start()
        p.join()

        assert len(manager.list_memory_blocks()) == 0


class MemoryWorker:
    def memory_block(self, size):
        return hosted(MemoryBlock(size))

    def make_dict(self, size):
        mem = MemoryBlock(size)
        mem.buf[3] = 26

        return {
            'size': size,
            'block': hosted(mem),
            # 'tuple': ('first', hosted([1, 2]), 'third'),
        }

    def make_list(self):
        return [
            hosted(MemoryBlock(10)),
            {'a': 3, 'b': hosted([1, 2])},
            hosted(MemoryBlock(20)),
        ]


def worker_dict(data, size):
    assert data['size'] == size
    mem = data['block']
    assert mem.size == size
    assert mem.buf[3] == 26
    mem.buf[3] = 62
    data['tuple'][1].append(3)
    assert len(data['tuple'][1]) == 3


def worker_mem(data):
    data.buf[10] = 10


def test_hosted():
    ServerProcess.register(
        'MemoryWorker',
        MemoryWorker,
        method_to_typeid={
            'memory_block': 'hosted',
            'make_dict': 'hosted',
            'make_list': 'hosted',
        },
    )
    with ServerProcess() as server:
        print('=======')
        worker = server.MemoryWorker()
        m = worker.memory_block(20)
        server.MemoryBlock(8)
        # These two references to memory blocks will be
        # taken care of when exiting the `server` context manager.

        print('=======')
        print(m.buf[10])
        p = Process(target=worker_mem, args=(m,))
        p.start()
        p.join()
        assert m.buf[10] == 10

        print('=======')
        worker.make_dict(64)
        # assert data['size'] == 64
        # assert data['block'].size == 64
        # assert data['block'].buf[3] == 26

        # p = Process(target=worker_dict, args=(data, 64))
        # p.start()
        # p.join()

        # assert data['block'].buf[3] == 62

        # print('=======')
        # data = worker.make_list()
