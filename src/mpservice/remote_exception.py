import sys
import traceback


# TODO
# check out
#   https://github.com/ionelmc/python-tblib
#   https://stackoverflow.com/questions/6126007/python-getting-a-traceback-from-a-multiprocessing-process
# about pickling Exceptions with tracebacks.

# See also: boltons.tbutils
# See also: package `eliot`: https://github.com/itamarst/eliot/blob/master/eliot/_traceback.py
# See also: package `pebble`


class RemoteException(Exception):
    '''
    This class is used to wrap a "regular" Exception object to add support
    for pickling the traceback (to some extent), so that when this object
    is passed from the originating process into another process and handled
    there, the printout (such as after being raised) contains useful traceback
    info.

    Example:
        try:
            a = 0 / 1
            return a
        except:
            return RemoteException()

        # In another process, this object, say `x`, is retrieved from a queue.
        # Then,

        if isinstance(x, RemoteException):
            raise x

    The printout looks as if the exception has occurred this way:

        raise RemoteException(...) from e

    where `e` is the original exception object as happened in the original
    process.

    One could `raise` this object in the original process (w/o any pickling).
    The printout is meaningful. However, this is not an intended use case.

    There are ways to make `RemoteException` unpickle to an object of the
    original exception type directly, and behave quite like the original
    object. However, there are scanarios where it is useful to recognize
    that an exception object is one from another process via the
    `RemoteException` mechanism. As a result, it's decided to keep the
    `RemoteException` class in both originating and receiving processes,
    and make it behave differently from the original class in a few places.

    To get the original exception object, reach for the `exc_value` attribute.
    '''

    def __init__(self, exc: BaseException = None, /):
        if exc is None:
            self.exc_type, self.exc_value, tb = sys.exc_info()
        else:
            if type(exc) is type:
                exc = exc()
            self.exc_type, self.exc_value, tb = type(
                exc), exc, exc.__traceback__
        self.tb_exc_value = traceback.TracebackException(
            self.exc_type, self.exc_value, tb
        )

        self.__cause__ = Exception(self.format())
        # This special attribute is not in `self.__dict__`
        # and will not be pickled. This attribute makes the `raise`
        # printout look like this:
        #
        #   ....
        #
        #   The above exception was the direct cause of the following exception:
        #
        #   ....

    @property
    def args(self):
        return self.exc_value, self.exc_value.args

    def __repr__(self):
        return '{}({!r})'.format(
            self.__class__.__name__, self.exc_value
        )

    def __str__(self):
        return f"{self.__class__.__name__}: {self.exc_value.__str__()}"

    def __setstate__(self, data):
        # TODO: I tried to customize `__getstate__` as well,
        # but for unknown reasons the method did not get called.
        super().__setstate__(data)
        self.__cause__ = Exception(self.format())

    @property
    def stack(self):
        # Return a `traceback.StackSummary` object
        return self.tb_exc_value.stack

    def format(self, *, chain=True):
        '''
        Return the traceback str of the original exception.
        If one needs further control over the format of printing,
        one may use `self.tb_exc_value.format` directly.
        '''
        z = ''.join(self.tb_exc_value.format(chain=chain))
        return z.strip('\n')

    def print(self, *, chain=True):
        print(self.format(chain=chain), file=sys.stderr)
