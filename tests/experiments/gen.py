import multiprocessing as mp

from mpservice import util
from mpservice.threading import get_shared_thread_pool


def worker():
    print(
        'in child', mp.current_process().name, list(util._global_thread_pools_.items())
    )


def main():
    get_shared_thread_pool()
    with util._global_thread_pools_lock:
        p = mp.get_context('fork').Process(target=worker)
        p.start()
        p.join()
    with util._global_thread_pools_lock:
        print('lock acquired')


if __name__ == '__main__':
    main()
