from queue import Empty, Full
import multiprocessing
from time import perf_counter
from mpservice.mpqueue import ZeroQueue

data = b'abc' * 1000
NN = 10_000_000


def push_zmq(q, n0, n, qq):
    record = data
    pusher = q.writer()
    for _ in range(n0, n0 + n):
        pusher.put_bytes(record)
    qq.put('OK')


def pull_zmq(q, done):
    getter = q.reader()
    n = 0
    while True:
        try:
            z = getter.get_bytes(timeout=0.1)
        except Empty:
            if done.is_set():
                break
        else:
            n += 1
    print('received', n, 'messages')



def push_queue(q):
    record = data
    for _ in range(NN):
        q.put(record)
    q.put(b'END')


def pull_quque(q):
    n = 0
    while True:
        z = q.get()
        if z == b'END':
            break
        n += 1
    print('received', n, 'messages')


def bench():
    mp = multiprocessing.get_context('spawn')

    def do_zmq():
        q = ZeroQueue(5678, 5679)
        pp = []
        qq = mp.Queue()
        done = mp.Event()
        pp.append(mp.Process(target=push_zmq, args=(q, 0, 1000, qq)))
        pp.append(mp.Process(target=push_zmq, args=(q, 1000, 1000, qq)))
        pp.append(mp.Process(target=pull_zmq, args=(q, done)))
        pp.append(mp.Process(target=pull_zmq, args=(q, done)))
        pp.append(mp.Process(target=pull_zmq, args=(q, done)))
        t0 = perf_counter()
        for p in pp:
            p.start()

        assert qq.get() == 'OK'
        assert qq.get() == 'OK'
        done.set()

        for p in pp:
            p.join()
        t1 = perf_counter()
        print(round(t1 - t0, 2), 'seconds')


    def do_q():
        pp = []
        q = mp.Queue()
        pp.append(mp.Process(target=push_queue, args=(q,)))
        pp.append(mp.Process(target=pull_quque, args=(q,)))
        for p in pp:
            p.start()
        t0 = perf_counter()
        for p in pp:
            p.join()
        t1 = perf_counter()
        print(round(t1 - t0, 2), 'seconds')

    print('---- zmq ----')
    do_zmq()

    # print('---- queue ----')
    # do_q()


def main():
    bench()


if __name__ == '__main__':
    main()

