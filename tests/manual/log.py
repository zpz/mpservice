# log.py
import logging
import warnings

from mpservice.multiprocessing import SpawnProcess


def worker():
    # root = logging.getLogger()
    # print('level:', root.getEffectiveLevel())
    # print('has handlers', root.hasHandlers())

    logging.getLogger('worker.error').error('worker error')
    logging.getLogger('worker.warn').warning('worker warning')
    logging.getLogger('worker.info').info('worker info')
    logging.getLogger('worker.debug').debug('worker debug')
    warnings.warn('warning in the child process')


def main():
    logging.getLogger('main.error').error('main error')
    logging.getLogger('main.info').info('main info')
    print('---starting process---')
    p = SpawnProcess(target=worker)
    p.start()
    p.join()
    print('---ending process---')
    logging.getLogger('main.warn').warning('main warning')
    logging.getLogger('main.debug').debug('main debug')


if __name__ == '__main__':
    logging.basicConfig(
        format='[%(asctime)s.%(msecs)02d; %(levelname)s; %(name)s; %(funcName)s, %(lineno)d] [%(processName)s]  %(message)s',
        level=logging.INFO,
    )
    main()
