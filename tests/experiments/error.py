# error.py
from mpservice.util import MP_SPAWN_CTX, RemoteException

def increment(qin, qout):
    while True:
        x = qin.get()
        if x is None:
            qout.put(None)
            return
        try:
            qout.put((x, x + 1))
        except Exception as e:
            qout.put((x, e))

def main():
    qin = MP_SPAWN_CTX.Queue()
    qout = MP_SPAWN_CTX.Queue()
    p = MP_SPAWN_CTX.Process(target=increment, args=(qin, qout))
    p.start()
    qin.put(1)
    qin.put(3)
    qin.put('a')
    qin.put(5)
    qin.put(None)
    p.join()
    while True:
        y = qout.get()
        if y is None:
            break
        if isinstance(y[1], BaseException):
            raise y[1]
        print(y)

if __name__ == '__main__':
    main()


# def worker(qin, qout):
#     q = MP_SPAWN_CTX.Queue()
#     p = MP_SPAWN_CTX.Process(target=increment, args=(qin, q))
#     p.start()
#     while True:
#         y = q.get()
#         if y is None:
#             qout.put(y)
#             break
#         # ... do other things ...
#         if isinstance(y[1], BaseException):
#             qout.put((y[0], RemoteException(y[1])))
#         else:
#             qout.put(y)
#     p.join()
