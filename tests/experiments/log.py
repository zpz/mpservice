# log.py
import logging

from mpservice.util import SpawnProcess


def worker():
    logging.getLogger('worker.error').error('worker error')
    logging.getLogger('worker.warn').warning('worker warning')
    logging.getLogger('worker.info').info('worker info')
    logging.getLogger('worker.debug').debug('worker debug')


def main():
    logging.getLogger('main.error').error('main error')
    logging.getLogger('main.info').info('main info')
    p = SpawnProcess(target=worker)
    p.start()
    p.join()
    logging.getLogger('main.warn').warning('main warning')
    logging.getLogger('main.debug').debug('main debug')


if __name__ == '__main__':
    logging.basicConfig(
        format='[%(asctime)s.%(msecs)02d; %(levelname)s; %(name)s; %(funcName)s, %(lineno)d] [%(processName)s]  %(message)s',
        level=logging.DEBUG,
    )
    main()
