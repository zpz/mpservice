import multiprocessing
import time
from concurrent.futures import ProcessPoolExecutor
from mpservice.multiprocessing import FastQueue

import pytest


def test_simple():
    mp = multiprocessing.get_context('spawn')
    q = mp.FastQueue()
    q.put(1)
    q.put(2)
    q.put(3)
    q.get() == 1
    q.get() == 2
    q.get() == 3


def worker(q):
    print('\nworker starting')
    assert q.get(timeout=0.1) == 1
    assert q.get(timeout=0.1) == 2
    print('worker putting 3')
    q.put(3)
    time.sleep(0.2)
    z = q.get()
    assert z == 18, f"{z} != 18"
    time.sleep(0.1)
    try:
        z = q.get(timeout=0.1)
    except BrokenPipeError as e:
        print(e)


def test_close():
    mp = multiprocessing.get_context('spawn')
    q = mp.FastQueue()
    q.put(1)
    q.put(2)
    p = mp.Process(target=worker, args=(q,))
    p.start()
    try:
        time.sleep(0.2)
        assert q.get() == 3
        assert q.empty()
        q.put(18)
        q.close()
        assert q.closed()
        with pytest.raises(BrokenPipeError):
            q.put('abc')
        with pytest.raises(BrokenPipeError):
            z = q.get()
    finally:
        p.join()




