from __future__ import annotations

import traceback
from types import TracebackType
from typing import Optional


# This class should be in the module `mpserver`.
# It is here because the class `RemoteException` needs to handle it.
# User should import it from `mpserver`.
class EnsembleError(RuntimeError):
    def __init__(self, results: dict):
        nerr = sum(
            1 if isinstance(v, (BaseException, RemoteException)) else 0
            for v in results['y']
        )
        errmsg = None
        for v in results['y']:
            if isinstance(v, (BaseException, RemoteException)):
                errmsg = repr(v)
                break
        msg = f"{results['n']}/{len(results['y'])} ensemble members finished, with {nerr} error{'s' if nerr > 1 else ''}; first error: {errmsg}"
        super().__init__(msg, results)
        # self.args[1] is the results

    def __repr__(self):
        return self.args[0]

    def __str__(self):
        return self.args[0]

    def __reduce__(self):
        return type(self), (self.args[1],)


def is_remote_exception(e) -> bool:
    """Return ``True`` if the exception ``e`` was created by :class:`RemoteException`, and ``False`` otherwise."""
    return isinstance(e, BaseException) and isinstance(e.__cause__, RemoteTraceback)


def get_remote_traceback(e) -> str:
    """
    ``e`` must have checked ``True`` by :func:`is_remote_exception`.
    Suppose an Exception object is wrapped by :class:`RemoteException` and sent to another process
    through a queue.
    Taken out of the queue in the other process, the object will check ``True``
    by :func:`is_remote_exception`. Then this function applied on the object
    will return the traceback info as a string.
    """
    return e.__cause__.tb


class RemoteTraceback(Exception):
    """
    This class is used by :class:`RemoteException`.
    End-user does not use this class directly.
    """

    def __init__(self, tb: str):
        self.tb = tb

    def __str__(self):
        return self.tb


def _rebuild_exception(exc: BaseException, tb: str):
    exc.__cause__ = RemoteTraceback(tb)

    return exc


class RemoteException:
    # fmt: off
    """
This class is a pickle helper for Exception objects to preserve some traceback info.
This is needed because directly calling ``pickle.dumps`` on an Exception object will lose
its ``__traceback__`` and ``__cause__`` attributes.

This is designed to be used to send an exception object to another process.
It can also be used to pickle-persist an Exception object for some time.

This class preserves some traceback info simply by keeping it as a formatted string during pickling.
Once unpickled, the object obtained, say ``obj``,
is **not** an instance of ``RemoteException``, but rather of the original Exception type.
``obj`` does not have the ``__traceback__`` attribute, which is impossible to reconstruct,
but rather has the ``__cause__`` attribute, which is a custom Exception
object (:class:`RemoteTraceback`) that contains the string-form traceback info, with proper line breaks.
Most (if not all) methods and attributes of ``obj`` behave the same
as the original Exception object, except that
``__traceback__`` is gone but ``__cause__`` is added.

.. note:: ``type(pickle.loads(pickle.dumps(RemoteException(e))))`` is not ``RemoteException``, but rather ``type(e)``.

This design is a compromise.
Since the object out of unpickling is not an instance of ``RemoteException``,
we "lose control of it", in the sense that we can't add methods in ``RemoteException`` to help on the use of that object.
A clear benefit, though, is that we can detect the type of the exception by ``isinstanceof`` or by the type list in
``try ... except <type_list>``, *without even knowing about* ``RemoteException``.

.. note:: ``RemoteException`` does not subclass ``BaseException``, hence you can't *raise* an instance of this class.

.. seealso:: :func:`is_remote_exception`, :func:`get_remote_traceback`.

The session below shows the basic behaviors of a ``RemoteException``.

>>> from mpservice.multiprocessing import RemoteException, is_remote_exception, get_remote_traceback
>>> import pickle
>>>
>>>
>>> def foo():
...     raise ValueError(38)
>>>
>>>
>>> def gee():
...     foo()
>>>
>>>
>>> gee()
Traceback (most recent call last):
    File "<stdin>", line 1, in <module>
    File "<stdin>", line 2, in gee
    File "<stdin>", line 2, in foo
ValueError: 38
.
38
>>>
>>> err = None
>>> try:
...     gee()
... except Exception as e:
...     err = e
>>>
>>> err
ValueError(38)
>>> err.__traceback__  # doctest: +ELLIPSIS
<traceback object at 0x7...>
>>> err.__cause__ is None
True
>>>
>>> e_remote = RemoteException(err)
>>> e_remote
RemoteException(ValueError(38))
>>> e_pickled = pickle.dumps(e_remote)
>>> e_unpickled = pickle.loads(e_pickled)
>>>
>>> e_unpickled
ValueError(38)
>>> type(e_unpickled)
<class 'ValueError'>
>>> e_unpickled.__traceback__ is None
True
>>> e_unpickled.__cause__  # doctest: +SKIP
RemoteTraceback('Traceback (most recent call last):
  File "<stdin>", line 2, in <module>
  File "<stdin>", line 2, in gee
  File "<stdin>", line 2, in foo
ValueError: 38
')
>>>
>>> is_remote_exception(e_unpickled)
True
>>> get_remote_traceback(e_unpickled)  # doctest: +SKIP
'Traceback (most recent call last):
  File "<stdin>", line 2, in <module>
  File "<stdin>", line 2, in gee
  File "<stdin>", line 2, in foo
  ValueError: 38
'
>>> print(get_remote_traceback(e_unpickled))  # doctest: +SKIP
Traceback (most recent call last):
    File "<stdin>", line 2, in <module>
    File "<stdin>", line 2, in gee
    File "<stdin>", line 2, in foo
ValueError: 38
>>>
>>>
>>> raise e_unpickled
Traceback (most recent call last):
    File "<stdin>", line 1, in <module>
ValueError: 38
.
38

Examples
--------
Let's use an example to demonstrate the use of ``RemoteException``.
First, create a script with the following content:

.. code-block:: python
    :linenos:

    # error.py
    from mpservice.multiprocessing import MP_SPAWN_CTX, RemoteException

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
            print(y)

    if __name__ == '__main__':
        main()

Run it::

    $ python error.py
    (1, 2)
    (3, 4)
    ('a', TypeError('can only concatenate str (not "int") to str'))
    (5, 6)

Everything is as expected. Now, instead, we want to stop the program upon errors, so we change
line 30 to

::

        if isinstance(y[1], BaseException):
            raise y[1]
        print(y)

Note the line numbers at the bottom increase a bit. Now ``raise y[1]`` is on line 32, while the ``main()`` call is line 37.
Run it again::

    $ python error.py
    (1, 2)
    (3, 4)
    Traceback (most recent call last):
    File "error.py", line 35, in <module>
        main()
    File "error.py", line 31, in main
        raise y[1]
    TypeError: can only concatenate str (not "int") to str

Looks good?

Not really. The traceback says the exception happened on line 32 with ``raise y[1]``.
That's not very useful. We get no info about where it **actually** happened.

What's the problem?
Well, on line 13, the ``TypeError`` object ``e`` gets put in a multiprocessing queue, pickled;
on line 27, the object gets taken out of the queue, unpickled. In the process,
``pickle.dumps(e)`` strips off the
attribute ``e.__traceback__`` because traceback is not picklable!

One solution is to change line 13 to ``qout.put((x, RemoteException(e)))``.
Now run it again,

::

    $ python error.py
    (1, 2)
    (3, 4)
    mpservice._remote_exception.RemoteTraceback: Traceback (most recent call last):
    File "/home/docker-user/mpservice/tests/experiments/error.py", line 11, in increment
        qout.put((x, x + 1))
    TypeError: can only concatenate str (not "int") to str


    The above exception was the direct cause of the following exception:

    Traceback (most recent call last):
    File "error.py", line 35, in <module>
        main()
    File "error.py", line 31, in main
        raise y[1]
    TypeError: can only concatenate str (not "int") to str

This time, we get the exact line number where the error actually happened in the child process.

If we need to pass an Exception object through multiple processes, we need to remember that
an ``RemoteException`` object pickled and then unpickled gives rise to an object of the original
``Exception`` class, *not* of ``RemoteException``.
For any Exception object, follow this rule:

.. note:: Before putting any ``Exception`` object in a multiprocessing Queue, wrap it by ``RemoteException``.

The script is revised to show this idea:

.. code-block:: python
    :linenos:

    # error.py
    from mpservice.multiprocessing import MP_SPAWN_CTX, RemoteException

    def increment(qin, qout):
        while True:
            x = qin.get()
            if x is None:
                qout.put(None)
                return
            try:
                qout.put((x, x + 1))
            except Exception as e:
                qout.put((x, RemoteException(e)))

    def worker(qin, qout):
        q = MP_SPAWN_CTX.Queue()
        p = MP_SPAWN_CTX.Process(target=increment, args=(qin, q))
        p.start()
        while True:
            y = q.get()
            if y is None:
                qout.put(y)
                break
            # ... do other things ...
            if isinstance(y[1], BaseException):
                qout.put((y[0], RemoteException(y[1])))
            else:
                qout.put(y)
        p.join()

    def main():
        qin = MP_SPAWN_CTX.Queue()
        qout = MP_SPAWN_CTX.Queue()
        p = MP_SPAWN_CTX.Process(target=worker, args=(qin, qout))
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

Run it::

    $ python error.py
    (1, 2)
    (3, 4)
    mpservice._remote_exception.RemoteTraceback: Traceback (most recent call last):
    File "/home/docker-user/mpservice/tests/experiments/error.py", line 11, in increment
        qout.put((x, x + 1))
    TypeError: can only concatenate str (not "int") to str


    The above exception was the direct cause of the following exception:

    Traceback (most recent call last):
    File "error.py", line 51, in <module>
        main()
    File "error.py", line 47, in main
        raise y[1]
    TypeError: can only concatenate str (not "int") to str
    """
    # fmt: on

    # This takes the idea of `concurrent.futures.process._ExceptionWithTraceback`
    # with slightly tweaked traceback printout.
    # `pebble.common` uses the same idea.

    # check out
    #   https://github.com/ionelmc/python-tblib
    #   https://stackoverflow.com/questions/6126007/python-getting-a-traceback-from-a-multiprocessing-process
    # about pickling Exceptions with tracebacks.
    #
    # See also: boltons.tbutils
    # See also: package `eliot`: https://github.com/itamarst/eliot/blob/master/eliot/_traceback.py
    #
    # Also check out `sys.excepthook`.

    def __init__(self, exc: BaseException, tb: Optional[TracebackType | str] = None):
        """
        Parameters
        ----------
        exc
            A BaseException object.
        tb
            Optional traceback info, if not already carried by ``exc``.
        """
        if isinstance(tb, str):
            pass
        elif isinstance(tb, TracebackType):
            tb = "".join(traceback.format_exception(type(exc), exc, tb))
        else:
            if tb is not None:
                raise ValueError(f"expecting no traceback but got: {tb}")

            if exc.__traceback__ is not None:
                # This is the most common use case---in an exception handler block:
                #
                #   try:
                #       ...
                #   except Exception as e:
                #       ...
                #       ee = RemoteException(e)
                #       ...
                #
                # This includes the case where `e` has come from another process via `RemoteException`
                # (hence `is_remote_exception(e)` is True) and is raised again, and because
                # we intend to pickle it again (e.g. paassing it to another process via a queue),
                # hence we put it in `RemoteException`.
                tb = "".join(
                    traceback.format_exception(type(exc), exc, exc.__traceback__)
                )
            else:
                # This use case is not in an "except" block, rather somehow there's an
                # exception object and we need to pickle it, so we put it in a
                # `RemoteException`.
                if is_remote_exception(exc):
                    tb = get_remote_traceback(exc)
                    # `exc.__cause__` will become `None` after pickle/unpickle.
                else:
                    raise ValueError(f"{repr(exc)} does not contain traceback info")
                    # In this case, don't use RemoteException. Pickle the exc object directly.

        if isinstance(exc, EnsembleError):
            # When an EnsembleError is just raised, all the exception objects in it
            # are RemoteException objects, so this block is no op.
            # But once an EnsembleError object has gone through pickling/unpickling,
            # the RemoteException objects contained in it will change back to
            # BaseException objects. If this object is put in RemoteException again
            # (prior to its being placed on a queue), this block will be useful.
            z = exc.args[1]['y']
            for i in range(len(z)):
                if isinstance(z[i], BaseException):
                    z[i] = self.__class__(z[i])

        self.exc = exc
        """
        This is still the original Exception object with traceback and everything.
        When you get a RemoteException object, it must have not gone through pickling
        (because a RemoteException object would not survive pickling!), hence you can
        use its ``exc`` attribute directly.
        """
        self.tb = tb

    def __repr__(self):
        return f"{self.__class__.__name__}({self.exc.__repr__()})"

    def __str__(self):
        return f"{self.__class__.__name__}('{self.exc.__str__()}')"

    def __reduce__(self):
        return _rebuild_exception, (self.exc, self.tb)
