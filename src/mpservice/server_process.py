"""A "server process" provides a server running in one process,
to be called from other processes for shared data or functionalities.

This module corresponds to the standard
`multiprocessing.managers <https://docs.python.org/3/library/multiprocessing.html#managers>`_ module
with simplified APIs for targeted use cases. The basic workflow  is as follows.

1. Register one or more classes with the :class:`Manager` class::

        class Doubler:
            def __init__(self, ...):
                ...

            def double(self, x):
                return x * 2

        class Tripler:
            def __init__(self, ...):
                ...

            def triple(self, x):
                return x * 3

        Manager.register(Doubler)
        Manager.register(Tripler)

2. Create a manager object and start it::

        manager = Manager()
        manager.start()

   You can also use a context manager::

        with Manager() as manager:
            ...

3. Create one or more proxies::

        doubler = manager.Doubler(...)
        tripler = manager.Tripler(...)

   A manager object has a "server process".
   The above causes corresponding class objects to be created
   in the server process; the returned objects are "proxies"
   for the real objects. These proxies can be passed to any other
   processes and used there.

   The arguments in the above calls are passed to the server process
   and used in the ``__init__`` methods of the corresponding classes.
   For this reason, the parameters to ``__init__`` of a registered class
   must all be pickle-able.

   Calling one registered class multiple times, like

   ::

        prox1 = manager.Doubler(...)
        prox2 = manager.Doubler(...)

   will create independent objects in the server process.

   Multiple manager objects will run their corresponding
   server processes independently.

4. Pass the proxy objects to any process and use them there.

   Public methods (minus "properties") defined by the registered classes
   can be invoked on a proxy with the same parameters and get the expected
   result. For example,

   ::

        prox1.double(3)

   will return 6. Inputs and output of the public method
   should all be pickle-able.

In each new thread or process, a proxy object will create a new
connection to the server process (see``multiprocessing.managers.Server.accepter``,
...,
``Server.accept_connection``,
and
``BaseProxy._connect``;
all in `Lib/multiprocessing/managers.py <https://github.com/python/cpython/blob/main/Lib/multiprocessing/managers.py>`_);
the server process then creates a new thread
to handle requests from this connection (see ``Server.serve_client``
also in `Lib/multiprocessing/managers.py <https://github.com/python/cpython/blob/main/Lib/multiprocessing/managers.py>`_).
"""
from __future__ import annotations

import multiprocessing.managers
import warnings
from typing import Callable

from .util import MP_SPAWN_CTX


# Overhead of Thread:
# sequentially creating/running/joining
# threads with a trivial worker function:
#   20000 took 1 sec.


class Manager(multiprocessing.managers.SyncManager):
    def __init__(self):
        super().__init__(ctx=MP_SPAWN_CTX)

        # `self._ctx` is `MP_SPAWN_CTX`

    @classmethod
    def register(cls, typeid_or_callable: str | Callable, /, **kwargs):
        """
        ``typeid_or_callable`` is usually a class object.
        This method should be called before a :class:`Manager` object is "started".
        """
        if isinstance(typeid_or_callable, str):
            # This form allows the full API of the base class.
            # I have not encountered a need for this.
            # This is allowed just in case for experiments and expansions.
            # You almost always should use the other form.
            typeid = typeid_or_callable
            callable_ = kwargs.pop("callable", None)
        else:
            assert callable(typeid_or_callable)
            # Usually, `typeid_or_callable` is a class object and the sole argument.
            typeid = typeid_or_callable.__name__
            callable_ = typeid_or_callable
        if typeid in cls._registry:
            warnings.warn(
                '"%s" was registered; the existing registry is overwritten.' % typeid
            )
        super().register(typeid, callable_, **kwargs)
