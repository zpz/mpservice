from mpservice.streamer import Stream


def double(x):
    print('double', x)
    return x * 2


def shift(x):
    print('  shift', x)
    return x + 3


def add(batch):
    print('    add', batch)
    return sum(batch)



def main():
    s = Stream(range(20))
    s = s.transform(double).transform(shift).batch(3).transform(add)
    print(s.collect())


main()

