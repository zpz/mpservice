import asyncio
import multiprocessing as mp
import pickle
from mpservice.remote_exception import RemoteException
from mpservice.mpserver import EnqueueTimeout

class MyError(Exception):
    pass


def foo(q):
    try:
        raise MyError('wrong')
    except Exception as e:
        y = RemoteException(e)
        q.put(y)


def main():
    q = mp.Queue()
    p = mp.Process(target=foo, args=(q,))
    p.start()
    p.join()

    y = q.get()

    try:
        raise y
    except Exception as e:
        #raise RemoteException(e)
        z = RemoteException(e)
        print(repr(z))
        print(str(z))
        print('')
        z.print()


if __name__ == '__main__':
    main()
