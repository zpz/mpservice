from concurrent.futures import ProcessPoolExecutor
import multiprocessing as mp
import weakref

_global_pool = weakref.WeakValueDictionary()


def prepare_pool():
    pool = ProcessPoolExecutor()
    _global_pool['default'] = pool
    return pool


def worker():
    print(list(_global_pool.items()))


def main():
    print(list(_global_pool.items()))
    pool = prepare_pool()
    print(list(_global_pool.items()))

    p = mp.get_context('spawn').Process(target=worker)
    p.start()
    p.join()


if __name__ == '__main__':
    main()

