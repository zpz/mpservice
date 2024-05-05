# Use this script to verify that ctl-C can cleanly stop the service.

import asyncio
import random
import time

from mpservice.mpserver import Worker, ProcessServlet, SequentialServlet, Server
from mpservice.streamer import Stream


class Doubler(Worker):
    def call(self, x):
        time.sleep(random.uniform(0.001, 0.01))
        return x + x


class Tripler(Worker):
    def call(self, x):
        time.sleep(random.uniform(0.001, 0.01))
        return [v + v + v for v in x]


def main():
    server = Server(
        SequentialServlet(
            ProcessServlet(Doubler, cpus=[1, 2]),
            ProcessServlet(Tripler, cpus=[2, 3, 4], batch_size=100, batch_wait_time=0.01),
        ),
        capacity=256,
    )
    with server:
        N = 10
        data = Stream(range(N))
        t0 = time.perf_counter()
        results = data.parmap(server.call, num_workers=256, timeout=10, executor='thread').collect()
        t1 = time.perf_counter()
        assert results == [v * 6 for v in range(N)]
        print(t1 - t0)
        for i in range(10):
            print(i)
            time.sleep(1)



if __name__ == '__main__':
    main()
