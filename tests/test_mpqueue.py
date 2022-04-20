import multiprocessing
from queue import Empty
from mpservice.mpqueue import ZeroQueue


def worker_put(q):
    # print('\nworker_put starting')
    q.put(1)
    q.put('ab')
    q.put(['1', 'yes', [2, 3]])
    q.put_bytes(b'none')
    q.close()
    # print('worker_put done')


def worker_get(q):
    # print('\nworker_get starting')
    z = q.get()
    assert z == 1
    z = q.get(timeout=5)
    assert z == 'ab'
    assert q.get() == ['1', 'yes', [2, 3]]
    assert q.get_bytes() == b'none'
    # print('worker_get done')


def test_basic1():
    q = ZeroQueue()
    worker_put(q)
    worker_get(q)


def test_basic2():
    q = ZeroQueue()
    p = multiprocessing.Process(target=worker_put, args=(q,))
    p.start()
    worker_get(q)
    p.join()


def test_basic3():
    q = ZeroQueue()
    p = multiprocessing.Process(target=worker_get, args=(q,))
    p.start()
    worker_put(q)
    p.join()


def test_basic4():
    q = ZeroQueue()
    pp = [
        multiprocessing.Process(target=worker_put, args=(q,)),
        multiprocessing.Process(target=worker_get, args=(q,)),
    ]
    for p in pp:
        p.start()
    for p in pp:
        p.join()


def test_basic5():
    q = ZeroQueue()
    pp = [
        multiprocessing.Process(target=worker_put, args=(q,)),
        multiprocessing.Process(target=worker_get, args=(q,)),
    ]
    for p in pp:
        p.start()
    for p in pp:
        p.join()


def test_basic_spawn():
    mp = multiprocessing.get_context('spawn')
    q = mp.ZeroQueue()
    pp = [
        mp.Process(target=worker_put, args=(q,)),
        mp.Process(target=worker_get, args=(q,)),
    ]
    for p in pp:
        p.start()
    for p in pp:
        p.join()


def put_many(q, n0, n):
    for x in range(n0, n0 + n):
        q.put(x)
    print(multiprocessing.current_process().name, 'put', n, 'items')
    q.close()


def get_many(q, done):
    n = 0
    while True:
        try:
            z = q.get(timeout=1)
        except Empty:
            if done.is_set():
                break
        else:
            n += 1
    print(multiprocessing.current_process().name, 'got', n, 'items')


def test_many():
    mp = multiprocessing
    done = mp.Event()
    q = ZeroQueue()
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



def test_many_spawn():
    mp = multiprocessing.get_context('spawn')
    done = mp.Event()
    q = mp.ZeroQueue()
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
    print('done set')
    pp[2].join()
    pp[3].join()
    pp[4].join()


