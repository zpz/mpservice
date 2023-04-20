import sys
import threading
import traceback
from concurrent.futures import ThreadPoolExecutor


def worker():
    try:
        pass
    except BaseException:
        print(threading.current_thread().name)
        traceback.print_exception(*sys.exc_info())
        raise


def main():
    with ThreadPoolExecutor() as pool1, ThreadPoolExecutor() as pool2:
        pool1.submit(worker)
        pool2.submit(worker)
        print('work submitted')
    print('work done')


if __name__ == '__main__':
    main()
