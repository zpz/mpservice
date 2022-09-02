import multiprocessing as mp
import logging
from mpservice.util import ProcessLogger


def worker1(pl):
    with pl:
        logger = logging.getLogger('worker1')
        logger.warning('worker1 warning')
        logger.info('worker1 info')
        logger.debug('worker1 debug')


def worker2():
    logger = logging.getLogger('worker2')
    logger.warning('worker2 warning')
    logger.info('worker2 info')
    logger.debug('worker2 debug')


def test_logging():
    ctx = mp.get_context('spawn')
    with ProcessLogger(ctx=ctx) as pl:
        task1 = ctx.Process(target=worker1, args=(pl,))
        task2 = ctx.Process(target=worker2)
        task1.start()
        task2.start()
        task1.join()
        task2.join()


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    test_logging()



