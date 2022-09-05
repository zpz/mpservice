import logging
import multiprocessing as mp 
from mpservice.util import ProcessLogger


logger = logging.getLogger('mytest')


def worker2(pl):
    with pl:
        logger.error('worker 2 error')
        logger.warning('worker 2 warning')
        logger.info('worker 2 info')
        logger.debug('worker 2 debug')


def worker(pl):
    with pl:
        logging.getLogger('worker').error('worker error')
        logging.getLogger('worker.warn').warning('worker warning')

        with ProcessLogger(ctx=mp.get_context('spawn')) as pl2:
            p = mp.get_context('spawn').Process(target=worker2, args=(pl2,))
            p.start()
            p.join()

        logging.getLogger('worker.info').info('worker info')
        logging.getLogger('worker.debug').debug('worker debug')


def main():
    logger.error('main error')
    logger.info('main info')
    with ProcessLogger(ctx=mp.get_context('spawn')) as pl:
        p = mp.get_context('spawn').Process(target=worker, args=(pl,))
        p.start()
        p.join()
    logger.warning('main warning')
    logger.debug('main debug')


if __name__ == '__main__':
    logging.basicConfig(
        format='[%(asctime)s.%(msecs)02d; %(levelname)s; %(name)s; %(funcName)s, %(lineno)d] [%(processName)s]  %(message)s',
        level=logging.DEBUG,
    )
    main()
