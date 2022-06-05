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
All public methods (i.e. named without a leading '_')
of `MyServerProcess` can be used on this proxy object from other processes.
Input and output should all be picklable objects, since this cross-process
communiation.

When the proxy of one server object is passed to multiple processes,
the calls to its public methods (even the same method) from different processes
are concurrent. This will show benefit if the method is I/O bound.
I guess we can think of the method call in the server process as if it is
happening in separate threads.

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


class _MyManager(BaseManager):
    pass


class ServerProcess:
    # In one application, there should not be two subclasses
    # of `ServerProcess` with the same name,
    # because subclass registration is by the class name.

    @classmethod
    def start(cls, *args, ctx=None, **kwargs):
        '''
        `args` and `kwargs` are passed on to the `__init__`
        method of this class (implemented by a subclass as needed).
        The method `__init__` is executed in the process that hosts
        the real server object.
        '''
        if cls.__name__ not in _MyManager._registry:
            _MyManager.register(
                cls.__name__,
                cls,
            )
        manager = _MyManager(ctx=ctx)
        manager.start()  # pylint: disable=consider-using-with
        obj = getattr(manager, cls.__name__)(*args, **kwargs)
        return obj
