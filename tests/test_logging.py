import logging

from mpservice._logging import config_logger

logger = logging.getLogger(__name__)


# This test does not assert any result.
# It will pass as long as the code does not crash.
# To see the printout, run with `py.test -s`.
def test_logging():
    config_logger(level='debug')
    logger.debug('debug info')
    logger.info('some info')
    logger.warning('warning!')
    logger.error('something is wrong!')
    logger.critical('something terrible has happened!')
