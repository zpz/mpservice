'''`ServerProcess` provides a server running in one process,
to be called from other processes for shared data or functionalities.

User should subclass this class to implement the functionalities they need.

Usage:

  (1) Define a subclass,

      class MyServerProcess(ServerProcess):
          def do_x(self, x):
              ...
              y = ...
              return y

    The subclass can define `__init__` and helper methods as needed,
    plus public methods as service APIs.

  (2) in main process,

      obj = MyServerProcess.start(...)

  (3) pass `obj` to other processes

  (4) in other processes, cal public methods of `obj`, e.g.

      y = obj.do_x(123)

`obj` is NOT an instance of the class `MyServerProcess`.
It's a "proxy" object, which is like a reference to a
`MyServerProcess` object in the "server process".
All public methods of `MyServerProcess` can be used on this
proxy object from other processes.
Input and output should all be small, pickle-able objects.

When all references to this proxy object have
been garbage-collected, the server process is shut down.
Usually user doesn't need to worry about it.
In order to proactively shut down the server process,
delete (all references to) the proxy object.

`ServerProcess.start()` can be used multiple times
to create multiple server objects, which reside in diff
processes and are independent of each other.

Example use cases:

  (1) Worker processes need to call an external service with
      certain categorical input, i.e. some return values are
      repeated. Use a server process to call the external
      service and do some caching of the results; worker
      processes call this server process.

  (2) Worker processes all need to load a large dataset
      into memory for some look up. Instead of have a copy
      of this large dataset in each worker process, load
      a single copy in a server process, which provides
      lookup service for the worker processes.
'''

from multiprocessing.managers import BaseManager


class ServerProcess:

    @classmethod
    def start(cls, *args, ctx=None, **kwargs):
        '''
        `args` and `kwargs` are passed on to the `__init__`
        method of this class (implemented by a subclass as needed).
        The method `__init__` is executed in the process that hosts
        the real server object.
        '''
        BaseManager.register(
            cls.__name__,
            cls,
        )
        manager = BaseManager(ctx=ctx)
        manager.start()  # pylint: disable=consider-using-with
        obj = getattr(manager, cls.__name__)(*args, **kwargs)
        return obj
