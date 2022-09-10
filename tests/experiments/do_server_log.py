import logging
from mpservice.server_process import ServerProcess
from mpservice.util import SpawnProcess
from multiprocessing import Process


class MyServerProcess(ServerProcess):
    def x(self, val):
        logger = logging.getLogger('server')
        logger.error('worker error')
        logger.warning('worker warning')
        logger.info('worker info')
        logger.debug('worker bug')
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
    p = Process(target=worker, args=(server, ))
    p.start()
    p.join()


if __name__ == '__main__':
    logging.basicConfig(
        format='[%(asctime)s.%(msecs)02d; %(levelname)s; %(name)s; %(funcName)s, %(lineno)d] [%(processName)s]  %(message)s',
        level=logging.DEBUG,
    )
    main()
