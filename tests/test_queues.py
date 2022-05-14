import multiprocessing
import time
from queue import Empty
from mpservice._queues import Unique
from mpservice.streamer import Streamer

import pytest

methods = [None, 'spawn']


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


def test_uni():
    q = Unique()
    writer = q.writer()
    reader = q.reader()

    writer.put(2)
    writer.put(3)
    assert reader.get() == 2
    writer.put('a')
    writer.put('b')

    assert reader.get() == 3
    assert reader.get() == 'a'
    assert reader.get() == 'b'
    with pytest.raises(Empty):
        reader.get(timeout=0.3)
    assert reader.empty()

    writer.put_many([1, 3, 'a', 5])
    assert reader.get_many(2) == [1, 3]
    assert reader.get_many(5, first_timeout=0.1, extra_timeout=0.1) == ['a', 5]
    writer.close()


def unireader(q):
    q = q.reader(3)
    k = 0
    while True:
        z = q.get()
        if z is None:
            q.close()
            break
        assert z == k
        k += 1
    q.close()


def uniwriter(q):
    q = q.writer()
    for i in range(10):
        q.put(i)
    q.put_many(range(10, 20))
    q.put(None)
    q.close()


def test_unimany():
    ctx = multiprocessing.get_context('spawn')
    q = ctx.Unique()
    w = ctx.Process(target=uniwriter, args=(q,))
    r = ctx.Process(target=unireader, args=(q,))
    w.start()
    r.start()
    w.join()
    r.join()
    q.close()


def unireader2(q):
    name = multiprocessing.current_process().name
    q = q.reader(10)
    n = 0
    while True:
        try:
            z = q.get(timeout=1)
            n += 1
        except Empty:
            break
    print('reader', name, 'got', n, 'items')
    q.close()


def uniwriter2(q, k):
    name = multiprocessing.current_process().name
    q = q.writer()
    n = 0
    for i in range(100):
        time.sleep(0.001)
        q.put(i*10 + k)
        n += 1
    q.close()
    print('writer', name, 'wrote', n, 'items')


def test_unimany2():
    print('')
    ctx = multiprocessing.get_context('spawn')
    q = ctx.Unique()
    ps = []
    for i in range(3):
        ps.append(ctx.Process(target=uniwriter2, args=(q, i)))
    for i in range(3):
        ps.append(ctx.Process(target=unireader2, args=(q, )))
    for p in ps:
        p.start()
    for p in ps:
        p.join()
    q.close()

