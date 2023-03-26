import logging
from multiprocessing import Process

from mpservice.server_process import ServerProcess


class MyServerProcess(ServerProcess):
    def x(self, val):
        logger = logging.getLogger('server')
        logger.error('server error')
        logger.warning('server warning')
        logger.info('server info')
        logger.debug('server bug')
        return val


def worker(server):
    logger = logging.getLogger('worker')
    logger.error('worker error')
    logger.warning('worker warning')
    logger.info('worker info')
    logger.debug('worker bug')
    print('get 38:', server.x(38))


def main():
    server = MyServerProcess.start()
    p = Process(target=worker, args=(server,))
    p.start()
    p.join()


if __name__ == '__main__':
    logging.basicConfig(
        format='[%(asctime)s.%(msecs)02d; %(levelname)s; %(name)s; %(funcName)s, %(lineno)d] [%(processName)s]  %(message)s',
        level=logging.DEBUG,
    )
    main()
