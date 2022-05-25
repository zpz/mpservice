from time import perf_counter
from threading import Thread
from queue import Queue
from mpservice._queues import SingleLane


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


def main(q, n):
    t1 = Thread(target=pull, args=(q,))
    t2 = Thread(target=push, args=(q, n))
    t0 = perf_counter()
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    t1 = perf_counter()
    print(f"  {q.__class__.__name__:<12}{t1 - t0:.2f}")


if __name__ == '__main__':
    for n in (10000, 100000, 1000000, 5000000):
        for maxsize in (100, 1000, 10000):
            print('capacity', maxsize, n, 'items')
            for q in (Queue, SingleLane):
                main(q(maxsize), n)

'''
# Benchmark results 2022/5/8:

$ python bench_queue.py
capacity 100 10000 items
  Queue       0.08
  SingleLane  0.07
capacity 1000 10000 items
  Queue       0.07
  SingleLane  0.07
capacity 10000 10000 items
  Queue       0.13
  SingleLane  0.10
capacity 100 100000 items
  Queue       0.90
  SingleLane  0.77
capacity 1000 100000 items
  Queue       0.74
  SingleLane  0.66
capacity 10000 100000 items
  Queue       0.95
  SingleLane  0.93
capacity 100 1000000 items
  Queue       8.91
  SingleLane  7.81
capacity 1000 1000000 items
  Queue       7.35
  SingleLane  6.61
capacity 10000 1000000 items
  Queue       9.22
  SingleLane  8.05
capacity 100 5000000 items
  Queue       43.40
  SingleLane  38.88
capacity 1000 5000000 items
  Queue       36.94
  SingleLane  32.89
capacity 10000 5000000 items
  Queue       54.95
  SingleLane  51.22
'''
