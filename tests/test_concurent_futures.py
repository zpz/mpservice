import logging

from mpservice.concurrent_futures import (
    ProcessPoolExecutor,
)

logger = logging.getLogger(__name__)


def cfworker():
    logging.getLogger('worker').error('worker error')
    logging.getLogger('worker.warn').warning('worker warning')

    logging.getLogger('worker.info').info('worker info')
    logging.getLogger('worker.debug').debug('worker debug')


def test_concurrent_futures_executor():
    # Observe the printout of logs in the worker processes
    logging.basicConfig(
        format='[%(asctime)s.%(msecs)02d; %(levelname)s; %(name)s; %(funcName)s, %(lineno)d] [%(processName)s]  %(message)s',
        level=logging.DEBUG,
    )
    # TODO: this setting may interfere with other test. How to do it better?
    # TODO: the 'debug' level did not take effect due to pytext setting.
    # A separate script will be better to test this.
    logger.error('main error')
    logger.info('main info')
    with ProcessPoolExecutor() as pool:
        t = pool.submit(cfworker)
        t.result()
    logger.warning('main warning')
    logger.debug('main debug')
