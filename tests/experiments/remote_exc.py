from concurrent.futures import ProcessPoolExecutor
import multiprocessing as mp
from mpservice.util import SpawnProcess
import traceback


def gee():
    raise ValueError(3)


def foo():
    p = SpawnProcess(target=gee)
    return p.result()


def main():
    p = SpawnProcess(target=foo)
    # print(p.result())
    e = p.exception()
    print('repr')
    print(repr(e))
    print('')
    print('str')
    print(str(e))
    print('')
    print('args')
    print(e.args)
    print('')
    print('tb')
    print(e.__cause__.traceback)
    print('')
    # print('raise')
    # print('')
    # raise e
    print('__tb__')
    print(e.__traceback__)
    print(e.__context__)
    print(e.__dir__())
    print(e.__format__())


if __name__ == '__main__':
    main()
