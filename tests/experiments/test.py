import asyncio
# from mpservice.multiprocessing import Queue, Process, SpawnProcess
# from mpservice.concurrent.futures import ProcessPoolExecutor
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import Queue
from mpservice.threading import Thread


def worker(loop):
    print('sleeping in worker')
    asyncio.run_coroutine_threadsafe(asyncio.sleep(1), loop).result()
    print('1 in worker')



async def main():
    loop = asyncio.get_running_loop()

    w = Thread(target=worker, args=(loop,))
    w.start()
    print('a in main')
    # await asyncio.sleep(1)
    # print('b in main')
    w.join()


if __name__ == '__main__':
    asyncio.run(main())
