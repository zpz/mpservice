import asyncio
# from mpservice.multiprocessing import Queue, Process, SpawnProcess
# from mpservice.concurrent.futures import ProcessPoolExecutor
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import Queue


def worker(q):
    q.put(3)


async def main():
    q = Queue()
    with ProcessPoolExecutor() as pool:
        t = pool.submit(worker, q)
        t.result()

    assert q.get() == 3


if __name__ == '__main__':
    asyncio.run(main())
