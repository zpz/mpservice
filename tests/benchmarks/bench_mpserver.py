import logging
import multiprocessing as mp
import queue
import threading
import time

# from zpz.profile import profiled
from mpservice.mpserver import Servlet, SequentialServer
from mpservice.streamer import Stream
from mpservice._streamer import IterQueue

from zpz.logging import config_logger
logger = logging.getLogger(__name__)


NN = 20 # 100000


class Double(Servlet):
    def call(self, x):
        time.sleep(0.001)
        return x * 2


def double(x):
    time.sleep(0.001)
    return x * 2



#@profiled()
def test_sequential_stream():
    service = SequentialServer(cpus=[0])
    service.add_servlet(Double, cpus=[1, 2, 3] * 4)
    with service:
        t0 = time.perf_counter()
        ss = service.stream(range(NN))
        _ = ss.collect()
        t1 = time.perf_counter()
        print('\ntime taken:', t1 - t0)


def enqueue(q, n):
    for x in range(n):
        q.put(x)
    for _ in range(20):
        q.put(None)


def dequeue(qin, qout):
    while True:
        x = qin.get()
        if x is None:
            qout.put(x)
            return
        qout.put(double(x))


def test_queue():
    q1 = queue.Queue()
    q2 = queue.Queue()
    th1 = threading.Thread(target=enqueue, args=(q1, NN))
    th2 = [
            threading.Thread(target=dequeue, args=(q1, q2))
            for _ in range(12)
            ]

    t0 = time.perf_counter()
    th1.start()
    for t in th2:
        t.start()
    while True:
        x = q2.get()
        if x is None:
            break
    th1.join()
    for t in th2:
        t.join()
    t1 = time.perf_counter()
    print('\ntime taken:', t1 - t0)


def test_mp_queue():
    q = mp.Queue()
    th1 = mp.Process(target=enqueue, args=(q, range(NN)))
    th2 = mp.Process(target=dequeue, args=(q,))

    t0 = time.perf_counter()
    th1.start()
    th2.start()
    th1.join()
    th2.join()
    t1 = time.perf_counter()
    print('\ntime taken:', t1 - t0)


def test_iterqueue():
    q = IterQueue(1000000, threading.Event())
    th1 = threading.Thread(target=enqueue, args=(q, range(NN)))
    th2 = threading.Thread(target=dequeue, args=(q,))

    t0 = time.perf_counter()
    th1.start()
    th2.start()
    th1.join()
    th2.join()
    t1 = time.perf_counter()
    print('\ntime taken:', t1 - t0)


#@profiled()
def test_streamer(keep_order=False):
    s = Stream(range(NN)).transform(double, workers=12, keep_order=keep_order) #.transform(lambda x: x + 4, workers=0)
    t0 = time.perf_counter()
    _ = s.collect()
    t1 = time.perf_counter()
    print('\ntime taken:', t1 - t0)


if __name__ == '__main__':
    config_logger(level='info')

    #test_queue()
    #test_iterqueue()
    #test_mp_queue()
    #test_streamer(False)
    #test_streamer(True)
    test_sequential_stream()


