from queue import Empty
import multiprocessing
from time import monotonic, sleep
import mpservice._queues


record = b'OK' * 100
NN = 1000000


def push(q):
    data = record
    for _ in range(NN):
        # q.put(data)
        q.put('OK')
    q.put(None)


def pull(q, done):
    while True:
        try:
            z = q.get(timeout=0.01)
            if z is None:
                done.set()
                break
        except Empty:
            if done.is_set():
                break


def pull_many(q, done):
    while True:
        try:
            z = q.get_many(100, timeout=0.01)
            sleep(0.015)
        except Empty:
            if done.is_set():
                break


def bench_push():
    print('==== push ====')
    mp = multiprocessing.get_context('spawn')

    def _push(q, target, nworkers=1):
        done = mp.Event()
        pp = []
        pp.append(mp.Process(target=push, args=(q,)))
        pp.extend((mp.Process(target=target, args=(q, done)) for _ in range(nworkers)))
        for p in pp:
            p.start()
        t0 = monotonic()
        for p in pp:
            p.join()
        t1 = monotonic()
        print(q.__class__.__name__, round(t1 - t0, 2), 'seconds')

    print('---- pull one ----')
    print('---- one worker ----')
    for qq in (mp.BasicQueue, mp.FastQueue):
        q = qq()
        _push(q, pull)

    print('---- 5 workers ----')
    for qq in (mp.BasicQueue, mp.FastQueue):
        q = qq()
        _push(q, pull, 5)

    def _push_many(q, target, nworkers=1):
        for _ in range(NN):
            # q.put(data)
            q.put('OK')

        done = mp.Event()
        done.set()
        pp = []
        pp.extend((mp.Process(target=target, args=(q, done)) for _ in range(nworkers)))
        for p in pp:
            p.start()
        t0 = monotonic()
        for p in pp:
            p.join()
        t1 = monotonic()
        print(q.__class__.__name__, round(t1 - t0, 2), 'seconds')

    print('---- pull many ----')
    print('---- one worker ----')
    q = mp.BasicQueue()
    _push_many(q, pull)

    q = mp.FastQueue()
    _push_many(q, pull_many)

    print('---- 5 workers ----')

    q = mp.BasicQueue()
    _push_many(q, pull_many, 5)

    q = mp.FastQueue()
    _push_many(q, pull_many, 5)


def main():
    bench_push()


if __name__ == '__main__':
    main()

