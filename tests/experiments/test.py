from mpservice.multiprocessing import Manager
from mpservice.threading import Thread
from mpservice.multiprocessing import Process
from time import sleep

def worker(e):
    sleep(.1)
    return e.is_set()


def main():
    m = Manager()
    e = m.Event()
    t = Process(target=worker, args=(e,))
    t.start()

    t.join()
    m.shutdown()
    print('shut down')


if __name__ == '__main__':
    main()
