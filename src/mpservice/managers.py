'''
This module corresponds to the standard `multiprocessing.managers` module
with simplified APIs for targeted use cases.
'''
import multiprocessing.managers
from typing import Type

from .util import MP_SPAWN_CTX


# Overhead of Thread:
# sequentially creating/running/joining
# threads with a trivial worker function:
#   20000 took 1 sec.


class Manager(multiprocessing.managers.SyncManager):
    '''
    Usage:

        1. Register one or more classes with the Manager class:

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

        2. Create a manager object and start it:

                manager = Manager()
                manager.start()

        3. Create one or more proxies:

                doubler = manager.Doubler(...)
                tripler = manager.Tripler(...)

           A manager object has a "server process".
           The above causes corresponding class objects to be created
           in the server process; the returned objects are "proxies"
           for the real objects. These proxies can be passed to any other
           processes and used there.

           The arguments in the above calls are passed to the server process
           and used in the `__init__` methods of the corresponding classes.
           For this reason, the parameters to `__init__` of a registered class
           must all be pickle-able.

           Calling one registered class multiple times, like

                prox1 = manager.Doubler(...)
                prox2 = manager.Doubler(...)

           will create independent objects in the server process.

           Multiple manager objects will run their corresponding
           server processes independently.

        4. Pass the proxy objects to any process and use them there.

           Public methods defined by the registered classes can be
           invoked on a proxy with the same parameters and get the expected
           result. For example

                prox1.double(3)

           will return 6. Inputs and output of the public method
           should all be pickle-able.

        In a new thread or process, a proxy object will create a new
        connection to the server process (see `multiprocessing.managers.Server.accepter`,
        ..., `Server.accept_connection`); the server process creates a new thread
        to handle requests from this connection (see `Server.serve_client`).
    '''
    def __init__(self):
        super().__init__(ctx=MP_SPAWN_CTX)

        # `self._ctx` is `MP_SPAWN_CTX`

    @classmethod
    def register(cls, server_cls: Type, /):
        '''
        `server_cls` is a class object. Suppose this is actually the class `MyClass`,
        then on a started object `manager` of `Manager`,

            prox = manager.MyClass(...)

        will create an object of `MyClass` in the server process and return a proxy.
        '''
        super().register(server_cls.__name__, server_cls)
