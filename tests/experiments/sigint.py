'''
This script is used to check the handling of Ctl-C.
Run this script, then hit Ctl-C.
'''
import time

from mpservice.mpserver import SimpleServer
from zpz.logging import config_logger

config_logger(with_process_name=True)



def double(x):
    time.sleep(0.001)
    return [v * 2 for v in x]



def main():
    service = SimpleServer(double, batch_size=10)
    with service:
        x = [1, 2, 3]
        y = service.call(x)
        print(x, ' --> ', y)
        x = [4, 5, 6]
        y = service.call(x)
        print(x, ' --> ', y)
        for i in range(1000):
            _ = service.call(i)
        print('stay on forever...')
        while True:
            time.sleep(1)


if __name__ == '__main__':
    main()
