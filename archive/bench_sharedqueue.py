from time import monotonic
import multiprocessing
from sharedqueue import Queue


def push(q, n):
    for k in range(n):
        q.put(k)
    q.put(None)
    # print('pushed', n, 'items')


def pull(q):
    n = 0
    while True:
        z = q.get()
        if z is None:
            break
        n += 1
    # print('pulled', n, 'items')


def main(mp, q, n):
    t1 = mp.Process(target=pull, args=(q,))
    t2 = mp.Process(target=push, args=(q, n))
    t0 = monotonic()
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    t1 = monotonic()
    print(f"      {q.__class__.__name__:<12}{t1 - t0:.2f}")


if __name__ == '__main__':
    mp = multiprocessing.get_context('spawn')
    for n in (10000, 100000, 1000000):
        print('putting', n, 'items')
        for maxsize in (100, 1000):
            print('  maxsize', maxsize)
            main(mp, mp.Queue(maxsize), n)
            print('  maxsize_bytes', maxsize * 100)
            main(mp, Queue(ctx=mp, maxsize_bytes=maxsize*100), n)
            print('  simple')
            main(mp, mp.SimpleQueue(), n)
