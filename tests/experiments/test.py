from mpservice.multiprocessing import Manager
from mpservice.threading import Thread
from mpservice.multiprocessing import Process
from time import sleep
import pickle


def main():
    m = Manager()
    e = m.Event()
    e = pickle.dumps(e)
    sleep(0.1)
    ee = pickle.loads(e)

    m.shutdown()
    print('shut down')


if __name__ == '__main__':
    main()
