# from mpservice.multiprocessing import Process
from mpservice.mpserver import Process
from zpz.logging import config_logger
import logging
from time import sleep


logger = logging.getLogger(__name__)


def worker(n):
    sleep(n)
    # raise BaseException(f'slept for {n} seconds!')
    return 3


def main():
    p = Process(target=worker, args=(2,))
    p.start()

    sleep(1)
    print('\n\n\n')
    p.join()


if __name__ == '__main__':
    config_logger(level='info', with_process_name=True, with_thread_name=True)
    main()
