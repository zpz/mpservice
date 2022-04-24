import multiprocessing
import time
from queue import Empty
import mpservice.mpqueue
from mpservice.streamer import Streamer

import pytest

methods = [None, 'spawn']
names = ['StandardQueue', 'FastQueue', 'ZeroQueue']


@pytest.mark.parametrize('method', methods)
@pytest.mark.parametrize('name', names)
def test_basic0(method, name):
    print('')
    mp = multiprocessing.get_context(method)
    q = getattr(mp, name)()
    q.put(3)
    assert q.get() == 3
    with pytest.raises(Empty):
        _ = q.get(timeout=0.8)
    q.put_many_nowait(range(8))
    assert q.get_many(3) == [0, 1, 2]
    assert q.get_many(4) == [3, 4, 5, 6]
    assert q.get_many(4, first_timeout=0.2, extra_timeout=0.2) == [7]
    with pytest.raises(Empty):
        q.get_many(3, first_timeout=0.2, extra_timeout=0.2)

    assert q.empty()
    q.put_nowait('abc')

    time.sleep(0.1) # To prevent BrokenPipeError with BasicQueue.
    q.close()
    with pytest.raises(ValueError):
        q.put(8)


def worker_put(q):
    # print('\nworker_put starting')
    q.put(1)
    q.put('ab')
    q.put(['1', 'yes', [2, 3]])
    q.put(b'none')
    print('worker_put done')


def worker_get(q):
    # print('\nworker_get starting')
    z = q.get()
    assert z == 1
    z = q.get(timeout=5)
    assert z == 'ab'
    z = q.get()
    assert z == ['1', 'yes', [2, 3]]
    z = q.get()
    assert z == b'none'
    print('worker_get done')


@pytest.mark.parametrize('method', methods)
@pytest.mark.parametrize('name', names)
def test_basic1(method, name):
    print('')
    mp = multiprocessing.get_context(method)
    q = getattr(mp, name)()
    worker_put(q)
    worker_get(q)


@pytest.mark.parametrize('method', methods)
@pytest.mark.parametrize('name', names)
def test_basic2(method, name):
    print('')
    mp = multiprocessing.get_context(method)
    q = getattr(mp, name)()
    p = mp.Process(target=worker_put, args=(q,))
    p.start()
    worker_get(q)
    p.join()


@pytest.mark.parametrize('method', methods)
@pytest.mark.parametrize('name', names)
def test_basic3(method, name):
    print('')
    mp = multiprocessing.get_context(method)
    q = getattr(mp, name)()
    p = mp.Process(target=worker_get, args=(q,))
    p.start()
    worker_put(q)
    p.join()


@pytest.mark.parametrize('method', methods)
@pytest.mark.parametrize('name', names)
def test_basic4(method, name):
    print('')
    mp = multiprocessing.get_context(method)
    q = getattr(mp, name)()
    pp = [
        mp.Process(target=worker_put, args=(q,)),
        mp.Process(target=worker_get, args=(q,)),
    ]
    for p in pp:
        p.start()
    time.sleep(1)
    for p in pp:
        p.join()


def put_many(q, n0, n):
    nn = 0
    with Streamer(range(n0, n0 + n)) as data:
        for xs in data.batch(23):
            q.put_many(xs)
            nn += len(xs)
        print(multiprocessing.current_process().name, 'put', nn, 'items')


def get_many(q, done):
    n = 0
    while True:
        try:
            z = q.get_many(30, first_timeout=1, extra_timeout=1)
        except Empty:
            if done.is_set():
                break
            continue
        n += len(z)
    print(multiprocessing.current_process().name, 'got', n, 'items')


@pytest.mark.parametrize('method', methods)
@pytest.mark.parametrize('name', names)
def test_many(method, name):
    print('')
    mp = multiprocessing.get_context(method)
    q = getattr(mp, name)()
    done = mp.Event()
    pp = []
    pp.append(mp.Process(target=put_many, args=(q, 0, 1000)))
    pp.append(mp.Process(target=put_many, args=(q, 1000, 1000)))
    pp.append(mp.Process(target=get_many, args=(q, done)))
    pp.append(mp.Process(target=get_many, args=(q, done)))
    pp.append(mp.Process(target=get_many, args=(q, done)))
    for p in pp:
        p.start()
    pp[0].join()
    pp[1].join()
    done.set()
    pp[2].join()
    pp[3].join()
    pp[4].join()

