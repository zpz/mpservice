import importlib
import sys
import traceback
import warnings

from .util import full_class_name

# TODO
# check out
#   https://github.com/ionelmc/python-tblib
#   https://stackoverflow.com/questions/6126007/python-getting-a-traceback-from-a-multiprocessing-process
# about pickling Exceptions with tracebacks.

# See also: boltons.tbutils
# See also: package `eliot`: https://github.com/itamarst/eliot/blob/master/eliot/_traceback.py
# See also: package `pebble`

# TODO:
# checkout `concurrent.futures.process._ExceptionWithTraceback`.


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

        This is equivalent to

            try:
                a = 0 / 1
                return a
            except exception as e:
                return RemoteException(e)

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

    Efforts are made to support pickling; a pair of`to_dict/from_dict` methods
    further enable using other serializations.
    One peculiar case is that the producing environment used a custom
    exception class that is not available in the consuming environment;
    this is handled by the pickling support and `to_dict/from_dict`.
    '''

    def __init__(self, exc: BaseException = None, /):
        if exc is None:
            exc_type, exc_value, tb = sys.exc_info()
        else:
            if type(exc) is type:
                exc = exc()
            exc_type = type(exc)
            exc_value = exc
            tb = exc.__traceback__

        self._exc_name = exc_type.__name__
        self._exc_fullname = full_class_name(exc_type)
        self._exc_repr = repr(exc_value)
        self._exc_str = str(exc_value)
        self._exc_args = exc_value.args

        tb = traceback.TracebackException(exc_type, exc_value, tb)
        tb = ''.join(tb.format(chain=True)).strip('\n')
        tb = [tb]
        if issubclass(exc_type, RemoteException):
            tb = exc_value._exc_tb + tb
        self._exc_tb = tb

        # TODO: how to use the __cause__ attribute with a `self.exc_value`
        # that has a traceback property attached?

        # There's a special attribute __cause__, which is not in `self.__dict__`.
        # This attribute makes the `raise` printout look like this:
        #
        #   ....
        #
        #   The above exception was the direct cause of the following exception:
        #
        #   ....
        # We are not using this attribute. The difficulty lies in attaching
        # a traceback object to `self.exc_value`.
        # Instead, we directly control the printout via customizing sys.excepthook.

    def __repr__(self):
        return f"{self.__class__.__name__}({self._exc_repr})"

    def __str__(self):
        return f"{self._exc_name}: {self._exc_str}"

    @property
    def args(self):
        return (self.exc_value, )

    def to_dict(self):
        return self.__dict__

    @classmethod
    def from_dict(cls, state):
        obj = cls.__new__(cls)
        obj.__setstate__(state)
        return obj

    def __reduce__(self):
        return (Exception.__new__, (self.__class__,), self.to_dict())
        # Customize the first 2 elements to be sure `__init__` is not called
        # during unpickling, b/c the exception object expected by `__init__`
        # may be unavailable in the unpickling environment.
        # The 3rd element is the input to `__setstate__`.

        # `Exception.__reduce__` is defined, hence `__getstate__` is never called.
        # Don't try to define `__getstate__` in order to customize its pickling.

    @property
    def exc_type(self):
        fullname = self._exc_fullname
        try:
            if '.' not in fullname:
                exc_type = eval(fullname)
            else:
                names = fullname.split('.')
                if len(names) == 2 and names[0] == '__main__':
                    __main__ = sys.modules['__main__']
                    exc_type = getattr(__main__, names[1])
                else:
                    mod = '.'.join(names[:-1])
                    try:
                        m = sys.modules[mod]
                    except KeyError:
                        m = importlib.import_module(mod)
                    exc_type = getattr(m, names[-1])

        except (NameError, ModuleNotFoundError, AttributeError) as e:
            warnings.warn(str(e))
            exc_type = Exception

        return exc_type

    @property
    def exc_value(self):
        exc_type = self.exc_type
        if exc_type is Exception and self._exc_name != 'Exception':
            val = exc_type(self._exc_fullname, *self._exc_args)
        else:
            val = exc_type(*self._exc_args)
        return val

    def format(self) -> str:
        delim = "\n\nThe above exception was the direct cause of the following exception:\n\n"
        return delim.join(self._exc_tb)

    def print(self):
        print(self.format(), file=sys.stderr)


def exit_err_msg(obj, exc_type=None, exc_value=None, exc_tb=None):
    if exc_type is None:
        return
    if isinstance(exc_value, RemoteException):
        msg = "Exiting {} with exception: {}\n{}".format(
            obj.__class__.__name__, exc_type, exc_value,
        )
        msg = "{}\n\n{}\n\n{}".format(
            msg, exc_value.format(),
            "The above exception was the direct cause of the following exception:")
        msg = f"{msg}\n\n{''.join(traceback.format_tb(exc_tb))}"
        return msg


_excepthook_ = sys.excepthook


def _my_excepthook(type_, value, tb):
    # Customize the printout upon `raise RemoteException(...)`.
    # This works in the default Python console but may not work in other
    # interactive shells such as `ptpython`, `ipython`.
    #
    #  https://stackoverflow.com/q/1261668/6178706
    #
    # This is called only when the exception is NOT handled.
    if type_ is RemoteException:
        print("{}\n\n{}\n".format(
            value.format(),
            "The above exception was the direct cause of the following exception:"),
            file=sys.stderr)
    _excepthook_(type_, value, tb)


sys.excepthook = _my_excepthook
