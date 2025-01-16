import queue
import random
import threading
from time import perf_counter, sleep

import pytest

import mpservice.multiprocessing
import mpservice.multiprocessing.queues
import mpservice.multiprocessing.synchronize
import mpservice.threading
from mpservice.queue import IterableQueue, Queue, ResponsiveQueue, StopRequested


def wait_forever(q):
    return q.get()


def test_responsive_queue():
    stop_requested = threading.Event()
    q = ResponsiveQueue(Queue(), stop_requested=stop_requested)
    q.put(3)
    q.put(4)
    assert q.get() == 3

    worker = mpservice.threading.Thread(target=wait_forever, args=(q,))
    assert q.get() == 4
    assert q.empty()

    worker.start()
    t0 = perf_counter()
    sleep(1.5)
    stop_requested.set()
    with pytest.raises(StopRequested):
        worker.result()
    t1 = perf_counter()
    assert t1 - t0 < 3.0

    stop_requested = mpservice.multiprocessing.Event()
    q = ResponsiveQueue(
        mpservice.multiprocessing.Queue(), stop_requested=stop_requested
    )
    worker = mpservice.multiprocessing.Process(target=wait_forever, args=(q,))
    worker.start()
    t0 = perf_counter()
    sleep(1.5)
    stop_requested.set()
    with pytest.raises(StopRequested):
        worker.result()
    t1 = perf_counter()
    assert t1 - t0 < 3.0


def test_iterable_queue_basic():
    data = [1, 2, 3, 4, 5]
    q = IterableQueue(queue.Queue(maxsize=10))
    for d in data:
        q.put(d)
    q.put_end()

    z = []
    for d in q:
        z.append(d)

    assert z == [1, 2, 3, 4, 5]

    q = IterableQueue(mpservice.multiprocessing.Queue(maxsize=3))
    q.put(1, timeout=3)
    q.put(2, timeout=3)
    q.put(3, timeout=3)
    t0 = perf_counter()
    with pytest.raises(queue.Full):
        q.put(4, timeout=3)
    t1 = perf_counter()
    assert (t1 - t0) > 2.9


def _produce(qin, qout):
    while True:
        sleep(random.uniform(0.001, 0.02))
        try:
            z = qin.get_nowait()
            qout.put(z)
        except queue.Empty:
            break
    qout.put_end()


def _consume(qin, qout):
    for z in qin:
        qout.put(z)
        sleep(random.uniform(0.001, 0.01))
    qout.put_end()


def test_iterable_queue_multi_parties():
    n_suppliers = 3
    n_consumers = 4

    for qcls, wcls in [
        (mpservice.queue.Queue, mpservice.threading.Thread),
        (mpservice.multiprocessing.Queue, mpservice.multiprocessing.Process),
    ]:
        # print()
        # print(qcls, wcls)
        q0 = qcls()
        for d in range(80):
            q0.put(d)

        q1 = IterableQueue(qcls(maxsize=1000), num_suppliers=n_suppliers)
        q2 = IterableQueue(qcls(maxsize=200), num_suppliers=n_consumers)

        producers = [wcls(target=_produce, args=(q0, q1)) for _ in range(n_suppliers)]
        consumers = [wcls(target=_consume, args=(q1, q2)) for _ in range(n_consumers)]

        for w in producers + consumers:
            w.start()
        for w in producers + consumers:
            w.join()

        zz = []
        for z in q2:
            zz.append(z)
        assert sorted(zz) == list(range(80))
