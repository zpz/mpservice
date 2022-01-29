from threading import Thread
from queue import Queue
from time import sleep


def keep_on():
    for i in range(100000):
        print('keep on', i)
        sleep(0.1)


def die_soon(cond):
    for i in range(10):
        print('die soon', i)
        sleep(0.08)
    print('to die')
    raise ValueError('die soon')


def main():
    t1 = Thread(target=keep_on)
    t2 = Thread(target=die_soon)
    t1.start()
    t2.start()
    while True:
        sleep(0.01)
        if not t2.is_alive():
            print('died')
            raise TypeError('run')
    t1.join()
    t2.join()


main()

