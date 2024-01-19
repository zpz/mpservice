# from mpservice.threading import Thread
from mpservice.mpserver import Thread
from zpz.logging import config_logger
import logging
from time import sleep


logger = logging.getLogger(__name__)


def worker(n):
    sleep(n)
    raise BaseException(f'slept for {n} seconds!')


def main():
    p = Thread(target=worker, args=(2,))
    p.start()

    sleep(1)
    p.join()


if __name__ == '__main__':
    config_logger(level='info', with_process_name=True, with_thread_name=True)
    main()
