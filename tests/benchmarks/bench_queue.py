import multiprocessing
import pickle
import queue
import threading
from time import perf_counter, sleep
from typing import Any

import numpy
import orjson
from faker import Faker
import faster_fifo
import faster_fifo_reduction
from mpservice.util import BoundedSimpleQueue


mp = multiprocessing.get_context('spawn')

fake = Faker()
Faker.seed(1234)

data = {
    'data': {
        'np': numpy.random.rand(100, 100),
        'float': [x + 0.3 for x in range(1000)],
        'int': list(range(1000)),
        },
    'attributes': {fake.name(): fake.sentence() for _ in range(500)},
    'details': [fake.text() for _ in range(200)],
    }
N = 10000


class OrjsonPickler:
    def __init__(self, x: Any):
        self.value = x

    def __getstate__(self):
        return orjson.dumps(
            self.value,
            option=orjson.OPT_SERIALIZE_NUMPY | orjson.OPT_PASSTHROUGH_DATACLASS)

    def __setstate__(self, data):
        self.value = orjson.loads(data)


def orjson_dumps(x):
    return orjson.dumps(x,
        option=orjson.OPT_SERIALIZE_NUMPY)


def enqueue(q):
    x = data
    for _ in range(N):
        # sleep(0.0001)
        q.put(x)


def dequeue(q, to_stop):
    n = 0
    while True:
        try:
            z = q.get(timeout=0.01)
            # sleep(0.0002)
            n += 1
        except queue.Empty:
            if to_stop.is_set():
                if isinstance(q, (queue.Queue, queue.SimpleQueue)):
                    print('got', n, 'items in', threading.current_thread().name)
                else:
                    print('got', n, 'items in', mp.current_process().name)
                return



def dequeue_many(q, to_stop):
    n = 0
    while True:
        try:
            z = q.get_many(max_messages_to_get=100, timeout=0.01)
            # sleep(0.0002)
            n += len(z)
        except queue.Empty:
            if to_stop.is_set():
                if isinstance(q, (queue.Queue, queue.SimpleQueue)):
                    print('got', n, 'items in', threading.current_thread().name)
                else:
                    print('got', n, 'items in', mp.current_process().name)
                return


def enqueue_orjson(q):
    x = data
    for _ in range(N):
        q.put(OrjsonPickler(x))


def dequeue_orjson(q, to_stop):
    n = 0
    while True:
        try:
            z = q.get(timeout=0.1)
            z = z.value
            n += 1
        except queue.Empty:
            if to_stop.is_set():
                print('got', n, 'items in process', mp.current_process().name)
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
    print('\nthreading\n')

    q = queue.Queue()
    main(q, threading.Thread, enqueue, dequeue)

    q = queue.SimpleQueue()
    main(q, threading.Thread, enqueue, dequeue)

    q = queue.Queue(1000)
    main(q, threading.Thread, enqueue, dequeue)

    q = BoundedSimpleQueue(1000)
    main(q, threading.Thread, enqueue, dequeue)

    print('\nmultiprocessing\n')

    q = mp.Queue(1000)
    main(q, mp.Process, enqueue, dequeue)

    q = faster_fifo.Queue()
    main(q, mp.Process, enqueue, dequeue)

    q = faster_fifo.Queue()
    main(q, mp.Process, enqueue, dequeue_many)

    # q = mp.Queue(1000)
    # main(q, mp.Process, enqueue_orjson, dequeue_orjson)

    # q = faster_fifo.Queue()
    # main(q, mp.Process, enqueue_orjson, dequeue_orjson)

    # q = faster_fifo.Queue(loads=orjson.loads, dumps=orjson_dumps)
    # main(q, mp.Process, enqueue, dequeue)


if __name__ == '__main__':
    main_all()

    print('')
    dumps, loads = pickle.dumps, pickle.loads
    t0 = perf_counter()
    for _ in range(1000):
        z = loads(dumps(data))
    t1 = perf_counter()
    print('pickle', t1 - t0)

    print('')
    dumps, loads = orjson_dumps, orjson.loads
    t0 = perf_counter()
    for _ in range(1000):
        z = loads(dumps(data))
    t1 = perf_counter()
    print('orjson', t1 - t0)

