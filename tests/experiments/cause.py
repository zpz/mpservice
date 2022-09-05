import traceback


def foo1():
    raise ValueError(3)


def foo2():
    try:
        return foo1()
    except Exception as e:
        print('\ntraceback\n')
        traceback.print_exception(type(e), e, e.__traceback__)
        print('')
        raise ZeroDivisionError from e


def main():
    foo2()


main()


