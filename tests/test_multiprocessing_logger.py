import logging
import multiprocessing as mp
import warnings

from mpservice.multiprocessing import Process


def worker1():
    logger = logging.getLogger('worker1')
    logger.warning('worker1 warning')
    warnings.warn('warning in worker1')
    logger.info('worker1 info')
    logger.debug('worker1 debug')


def worker2():
    logger = logging.getLogger('worker2')
    logger.warning('worker2 warning')
    warnings.warn('warning in worker2')
    logger.info('worker2 info')
    logger.debug('worker2 debug')


def test_logging():
    task1 = Process(target=worker1)
    task2 = mp.get_context('spawn').Process(target=worker2)  # fork
    task1.start()
    task2.start()
    task1.join()
    task2.join()


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    test_logging()

# Printout is like this:
#
# worker2 warning
# /home/docker-user/mpservice/tests/test_multiprocessing_logger.py:19: UserWarning: warning in worker2
#   warnings.warn('warning in worker2')
# WARNING:worker1:worker1 warning
# WARNING:py.warnings:/home/docker-user/mpservice/tests/test_multiprocessing_logger.py:11: UserWarning: warning in worker1
#   warnings.warn('warning in worker1')
#
# INFO:worker1:worker1 info
# DEBUG:worker1:worker1 debug
#
# That is, in the standard spawned process, logging is not configured, hence
# the printout is governed by the default in terms of both format and level.
