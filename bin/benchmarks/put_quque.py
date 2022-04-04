from threading import Thread
from queue import Queue, Full
from time import sleep, perf_counter
from random import uniform, seed


NOMORE = object()


def get(q):
    while True:
        sleep(uniform(0.001, 0.01))
        z = q.get()
        if z is NOMORE:
            break


def put1(q, s):
    for x in range(10000):
        while True:
            try:
                q.put_nowait(x)
                break
            except Full:
                sleep(s)
    q.put(NOMORE)


def put2(q, s):
    for x in range(10000):
        while True:
            try:
                q.put(x, timeout=s)
                break
            except Full:
                continue
    q.put(NOMORE)


def bench1(s):
    print('sleep time in put', s)
    seed(100)

    q = Queue(100)
    t = Thread(target=put1, args=(q, s))

    t0 = perf_counter()
    t.start()
    get(q)
    t.join()
    t1 = perf_counter()

    print('  ', t1 - t0)


def bench2(s):
    print('timeout in put', s)
    seed(100)

    q = Queue(100)
    t = Thread(target=put2, args=(q, s))

    t0 = perf_counter()
    t.start()
    get(q)
    t.join()
    t1 = perf_counter()

    print('  ', t1 - t0)


bench1(0.01)
bench1(0.001)
bench1(0.0001)
bench1(0.00001)
bench1(0.01)
bench2(0.001)
bench2(0.0001)
bench2(0.00001)

