def gen():
    for x in range(20):
        try:
            yield x
        except GeneratorExit:
            print('generator exit captured')
            raise


if __name__ == '__main__':
    for x in gen():
        print(x)
        # if x > 8:
        #     break
            # raise ValueError(x)

