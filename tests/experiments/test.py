from mpservice.multiprocessing import Manager


def main():
    m = Manager()
    e = m.Event()
    m.shutdown()


if __name__ == '__main__':
    main()
