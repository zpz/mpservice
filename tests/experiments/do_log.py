import concurrent.futures
import logging
import multiprocessing as mp
from mpservice.util import MP_SPAWN_CTX


logger = logging.getLogger('mytest')


def worker(ev):
    logging.getLogger('worker').error('worker error')
    logging.getLogger('worker.warn').warning('worker warning')

    logging.getLogger('worker.info').info('worker info')
    logging.getLogger('worker.debug').debug('worker debug')


def main():
    logger.error('main error')
    logger.info('main info')
    with concurrent.futures.ProcessPoolExecutor(mp_context=MP_SPAWN_CTX) as pool:
        t = pool.submit(worker, mp.get_context('spawn').Manager().Event())
        t.result()
    logger.warning('main warning')
    logger.debug('main debug')


if __name__ == '__main__':
    logging.basicConfig(
        format='[%(asctime)s.%(msecs)02d; %(levelname)s; %(name)s; %(funcName)s, %(lineno)d] [%(processName)s]  %(message)s',
        level=logging.DEBUG,
    )
    main()
