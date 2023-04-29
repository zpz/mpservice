
def gen():
    try:
        for x in range(100):
            yield x
    finally:
        print('exiting')

def main():
    g = gen()
    for _ in range(10):
        print(next(g))
    # for x in g:
    #     print(x)
    #     if x > 10:
    #         break

    print('done')

if __name__ == '__main__':
    main()
