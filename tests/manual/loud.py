import multiprocessing
import threading


from mpservice.util import (
    Process,
    ProcessPoolExecutor,
    Thread,
    ThreadPoolExecutor,
)



def loud_worker():
    raise ValueError(3)


def main():
    # This prints error info
    print('\nstandard thread\n')
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

    print('\nstandard process\n')
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

