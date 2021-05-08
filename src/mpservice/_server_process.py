from multiprocessing.managers import BaseManager


class ServerProcess:
    # User should subclass this class to implement the
    # functionalities they need.
    # An instance of this class is created in a "server process"
    # and serves as shared data for other processes.
    #
    # Usage:
    #
    #   (1) in main process,
    #
    #       obj = ServerProcess.start(...)
    #
    #   (2) pass `obj` to other processes
    #
    #   (3) in other processes, cal public methods of `obj`.
    #
    # `obj` is NOT an instance of the class `ServerProcess`.
    # It's a "proxy" object, which is like a reference to a
    # `ServerProcess` object in the "server process".
    # All public methods of `ServerProcess` can be used on this
    # proxy object from other processes.
    # Input and output should all be small, pickle-able
    # objects.
    #
    # When all references to this proxy object have
    # been garbage-collected, the server process is shut down.
    # Usually user doesn't need to worry about it.
    # In order to proactively shut down the server process,
    # delete (all references to) the proxy object.
    #
    # `ServerProcess.start()` can be used multiple times
    # to have multiple shared objects, which reside in diff
    # processes and are independent of each other.

    @classmethod
    def start(cls, *args, **kwargs):
        BaseManager.register(
            cls.__name__,
            cls,
        )
        manager = BaseManager()
        manager.start()  # pylint: disable=consider-using-with
        obj = getattr(manager, cls.__name__)(*args, **kwargs)
        return obj
