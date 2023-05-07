import asyncio
import random
import time

from mpservice.mpserver import ProcessWorker, ProcessServlet, SequentialServlet, Server
from mpservice.streamer import Stream


class Doubler(ProcessWorker):
    def call(self, x):
        time.sleep(random.uniform(0.001, 0.01))
        return x + x


class Tripler(ProcessWorker):
    def call(self, x):
        time.sleep(random.uniform(0.001, 0.01))
        return [v + v + v for v in x]


def main():
    server = Server(
        SequentialServlet(
            ProcessServlet(Doubler, cpus=[1]),
            ProcessServlet(Tripler, cpus=[2], batch_size=100, batch_wait_time=0.01),
        ),
        capacity=256,
    )
    with server:
        N = 1000
        server._count_cancelled_in_backlog = False
        data = Stream(range(N))
        t0 = time.perf_counter()
        results = data.parmap(server.async_call, num_workers=256, timeout=10).collect()
        t1 = time.perf_counter()
        assert results == [v * 6 for v in range(N)]
        print(t1 - t0)


if __name__ == '__main__':
    main()
