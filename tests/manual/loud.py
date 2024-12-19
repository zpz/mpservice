import multiprocessing
import threading

from mpservice.multiprocessing import Process
from mpservice.threading import Thread
from mpservice.concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

def loud_worker():
    raise ValueError(3)


def main():
    print('\nstandard thread\n')
    # This prints error info within the thread, as `join` does not propagate the exception.
    t = threading.Thread(target=loud_worker)
    t.start()
    t.join()

    print('\ncustom thread\n')
    t = Thread(target=loud_worker)
    t.start()
    try:
        t.join()
    except ValueError as e:
        print(repr(e))
    # Also remove the exception handling and see the printout when it crashes here.

    print('\nstandard process\n')
    # This prints error info within the process, as `join` does not propagate the exception.
    t = multiprocessing.Process(target=loud_worker)
    t.start()
    t.join()

    print('\ncustom process\n')
    t = Process(target=loud_worker)
    t.start()
    try:
        t.join()
    except ValueError as e:
        print(repr(e))
    # Also remove the exception handling and see the printout when it crashes here.


    print('\ncustom thread pool loud\n')
    with ThreadPoolExecutor() as pool:
        t = pool.submit(loud_worker)

    print('\ncustom thread pool not loud\n')
    with ThreadPoolExecutor() as pool:
        t = pool.submit(loud_worker, loud_exception=False)

    print('\ncustom process pool loud\n')
    with ProcessPoolExecutor() as pool:
        t = pool.submit(loud_worker)

    print('\ncustom process pool not loud\n')
    with ProcessPoolExecutor() as pool:
        t = pool.submit(loud_worker, loud_exception=False)


if __name__ == '__main__':
    # Run this and watch the printout behavior.
    main()
