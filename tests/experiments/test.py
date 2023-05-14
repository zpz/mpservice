import ctypes
from mpservice.multiprocessing import SimpleQueue, Event, Value


def main():
    q = SimpleQueue()
    e = Value(ctypes.c_bool)
    print(e.value)
    q.put(e)
    print(q.get())


if __name__ == '__main__':
    main()
