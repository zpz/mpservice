import multiprocessing
import threading
from mpservice.multiprocessing import ServerProcess
import time
from mpservice.streamer import Stream



class Doubler:
    def do(self, x):
        # time.sleep(0.5)
        # print(multiprocessing.current_process().name, threading.current_thread().name)
        return x + x
    

ServerProcess.register(Doubler)


def main():
    with ServerProcess() as server:
        doubler = server.Doubler()
        N = 100000
        # ss = Stream(range(100)).parmap(doubler.do, executor='thread', num_workers=50)
        ss = (doubler.do(x) for x in range(N))
        t0 = time.perf_counter()
        zz = list(ss)
        t1 = time.perf_counter()
        print(t1 - t0)
        print((t1 - t0) / N)
        print(N / (t1 - t0))
        # assert zz == [x + x for x in range(100)]


if __name__ == '__main__':
    main()
