from mpservice.multiprocessing import Process
import logging
from zpz.logging import config_logger
from time import sleep

logger = logging.getLogger(__name__)


def worker():
    sleep(2)
    logger.info('info')
    logger.warning('warn')


def main():
    p = Process(target=worker, name='myprocess')
    p.start()
    p.name = 'yourprocess'
    p.join()
    

if __name__ == '__main__':
    config_logger(with_process_name=True, with_thread_name=True)
    main()
