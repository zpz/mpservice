import multiprocessing
import queue
import threading
from time import perf_counter

import faster_fifo
import faster_fifo_reduction
import mpservice.multiprocessing

from util import make_message


mp = multiprocessing.get_context('spawn')

data = make_message()
N = 10000


def enqueue(q):
    # t0 = perf_counter()
    x = data
    for _ in range(N):
        # sleep(0.0001)
        q.put(x)
    # t1 = perf_counter()
    # print('enqueue took', t1 - t0, 'seconds')


def dequeue(q, to_stop):
    # t0 = perf_counter()
    n = 0
    while True:
        try:
            z = q.get(timeout=0.01)
            # sleep(0.0002)
            n += 1
        except queue.Empty:
            if to_stop.is_set():
                t1 = perf_counter()
                # print('dequeue took', t1 - t0, 'seconds')
                if isinstance(q, (queue.Queue, queue.SimpleQueue)):
                    print('got', n, 'items in', threading.current_thread().name)
                else:
                    print('got', n, 'items in', mp.current_process().name)
                return



def dequeue_many(q, to_stop):
    # t0 = perf_counter()
    n = 0
    while True:
        try:
            z = q.get_many(max_messages_to_get=100, timeout=0.01)
            # sleep(0.0002)
            n += len(z)
        except queue.Empty:
            if to_stop.is_set():
                # t1 = perf_counter()
                # print('dequeue took', t1 - t0, 'seconds')
                if isinstance(q, (queue.Queue, queue.SimpleQueue)):
                    print('got', n, 'items in', threading.current_thread().name)
                else:
                    print('got', n, 'items in', mp.current_process().name)
                return


def main(q, P, enq, deq):
    to_stop = multiprocessing.Event()
    p1 = P(target=enq, args=(q,))
    p2 = P(target=enq, args=(q,))
    p3 = P(target=enq, args=(q,))
    p4 = P(target=enq, args=(q,))
    p10 = P(target=deq, args=(q, to_stop))
    p20 = P(target=deq, args=(q, to_stop))
    p30 = P(target=deq, args=(q, to_stop))
    p40 = P(target=deq, args=(q, to_stop))
    p1.start()
    p2.start()
    p3.start()
    p4.start()
    p10.start()
    p20.start()
    p30.start()
    p40.start()
    t0 = perf_counter()
    p1.join()
    p2.join()
    p3.join()
    p4.join()
    to_stop.set()
    p10.join()
    p20.join()
    p30.join()
    p40.join()
    t1 = perf_counter()

    print('')
    print(q, P, enq, deq)
    print(t1 - t0, 'seconds for', N*4, 'items')
    print('QPS:', N*4 / (t1 - t0))
    print('')


def main_all():
    print('\n====== unbounded ========\n')

    q = mp.Queue()
    main(q, mp.Process, enqueue, dequeue)

    q = mp.FastQueue()
    main(q, mp.Process, enqueue, dequeue)

    print('\n====== bounded ==========\n')

    q = mp.Queue(1000)
    main(q, mp.Process, enqueue, dequeue)

    q = faster_fifo.Queue()
    main(q, mp.Process, enqueue, dequeue)

    q = faster_fifo.Queue()
    main(q, mp.Process, enqueue, dequeue_many)


if __name__ == '__main__':
    main_all()
