from queue import Empty
import multiprocessing
from time import monotonic, sleep
from mpservice._queues import Unique


def push(q, n):
    for _ in range(n):
        q.put('OK')


def pull(q, done):
    while True:
        try:
            z = q.get()  # (timeout=0.01)
            if z is None:
                done.set()
                break
        except Empty:
            if done.is_set():
                break


def run(q, n, mp):
    pp = []
    done = mp.Event()
    pp.append(mp.Process(target=push, args=(q, n)))
    pp.append(mp.Process(target=pull, args=(q, done)))
    t0 = monotonic()
    for p in pp:
        p.start()
    pp[0].join()
    q.put(None)
    pp[1].join()
    t1 = monotonic()
    print(f"{q.__class__.__name__:<12} {n:>8} {round(t1 - t0, 2):>8}")


def main():
    mp = multiprocessing.get_context('spawn')
    for n in (10000, 100000, 1000000):
        run(mp.SimpleQueue(), n, mp)
        run(Unique(ctx=mp), n, mp)
        run(mp.Queue(), n, mp)
        print('')


if __name__ == '__main__':
    main()

