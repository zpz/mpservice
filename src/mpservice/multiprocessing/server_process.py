"""
The standard `Managers <https://docs.python.org/3/library/multiprocessing.html#managers>`_
provide a mechanism to interactively
communicate with a "server" that runs in another process.

The keyword is "interactively"---just like a client to a HTTP server.
The interactivity is achieved by a "proxy": an object is created and *hosted* in a "server process";
the hosted object is represented by a "proxy", which is passed to and used in other processes
to communicate with the hosted object. The backbone of the communication is ``multiprocessing.connection.Connection``
with its ``send`` and ``recv`` methods.

The module ``mpservice.multiprocessing.server_process`` provides :class:`ServerProcess`,
with mainly several enhancements to the standard ``Manager`` from ``multiprocessing.managers``, namely:
    
    - If a proxy method fails in ther server, the same exception is raised on the client side with useful trackback.
    - Proxy methods that return proxies can be called without access to the "manager", i.e. can be called on a proxy that has been passed from the original process into another process.
    - Nested proxies via function `managed`.
    - Support for shared memory blocks.

    
If you do not make use of these enhancements, you may as well use the 
:class:`Manager` that is imported
from ``mpservice.multiprocessing``.


Basic workflow
==============

1. First, register one or more classes with the :class:`ServerProcess` class::

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

    ServerProcess.register('Doubler', Doubler)
    ServerProcess.register('Tripler', Tripler)

2. Then, start a "server process" object in a contextmanager::

    with ServerProcess() as server:
            ...

   By now, ``server`` has started a "server process". We interact with things
   running in the server process via methods that have been "registered" on ``server``.

3. Instances of the registered classes can be created within the server process::

        doubler = server.Doubler(...)
        tripler = server.Tripler(...)

   This causes corresponding class objects to be created
   in the server process; the returned objects are "proxies"
   for the real objects. These proxies can be passed to any other
   processes and used there.

   The arguments in the above calls are passed into the server process
   and used in the ``__init__`` methods of the corresponding classes.
   For this reason, the parameters to ``__init__`` of a registered class
   must all be pickle-able.

   Calling a registered class multiple times, like

   ::

        prox1 = server.Doubler(...)
        prox2 = server.Doubler(...)

   will create independent objects in the server process.

   Multiple ``ServerProcess`` objects will run their corresponding
   server processes independently.

4. Pass the proxy objects to any process and use them there.

   By default, public methods (minus "properties") defined by the registered classes
   can be invoked on a proxy and will return the expected
   result. For example,

   ::

        prox1.double(3)

   will return 6. Inputs and output of the method ``Doubler.double``
   must all be pickle-able.

   The computation of the proxy's methods happens in the server process.
   The proxy object only handles the communication and input/output transport.

   Between the server process and the proxy object in a particular client process or thread,
   a connection is established, which starts a new thread in the server process
   to handle all requests from that proxy object (in said client process or thread).
   These "requests" include calling all methods of the proxy, not just one particular method.

   For example,

   ::

        th = Thread(target=foo, args=(prox1,))
        th.start()
        ...

   Suppose in function ``foo`` the first parameter is called ``p`` (which is ``prox1`` passed in),
   then ``p`` will communicate with the ``Doubler`` object (which runs in the server process)
   via its new dedicated thread in the server process, separate from the connection thread for ``prox1``
   in the "current" thread.

   Consequently, calls on a particular method of the proxy from multiple processes or threads
   become multi-threaded concurrent calls in the server process.
   We can design a simple example to observe this effect::


        class Doubler:
            def do(self, x):
                time.sleep(0.5)
                return x + x

        ServerProcess.register('Doubler', Doubler)

        def main():
            with ServerProcess() as server:
                doubler = server.Doubler()

                ss = Stream(range(100)).parmap(doubler.do, executor='thread', num_workers=50)
                t0 = time.perf_counter()
                zz = list(ss)
                t1 = time.perf_counter()
                print(t1 - t0)
                assert zz == [x + x for x in range(100)]

        if __name__ == '__main__':
            main()

   If the calls to ``doubler.do`` were sequential, then 100 calls would take 50 seconds.
   With concurrent calls in 50 threads as above, it took 1.05 seconds in an experiment.

   It follows that, if the method mutates some shared state, you may need to use locks to guard things.

:class:`ServerProcess` inherits from the standard :class:`multiprocessing.managers.SyncManager`.
If you don't need the enhancements (except for using :class:`mpservice.multiprocessing.SpawnProcess`
in place of the standard :class:`multiprocessing.process.BaseProcess`), you can import ``Manager`` from
``mpservice.multiprocessing`` like this::

    from mpservice.multiprocessing import Manager

    with Manager() as manager:
        q = manager.Event()
        ...

Shared memory
=============

"Shared memory", introduced in Python 3.8, provides a way to use a block of memory across processes
with zero-copy. ``ServerProcess`` has a method :meth:`~mpservice.multiprocessing.server_process.ServerProcess.MemoryBlock`
that returns a :class:`~mpservice.multiprocessing.server_process.MemoryBlockProxy` object::

    with ServerProcess() as server:
        mem = server.MemoryBlock(1000)
        buffer = mem.buf  # memoryview
        # ... write data into `buffer`
        # pass `mem` to other processes and use its `.buf` again for the content.
        # Since it's a "shared" block of memory, any process can modify the data
        # via the memoryview.

Usually you will use the three methods 
:meth:`~mpservice.multiprocessing.server_process.MemoryBlockProxy.name`,
:meth:`~mpservice.multiprocessing.server_process.MemoryBlockProxy.size`,
and
:meth:`~mpservice.multiprocessing.server_process.MemoryBlockProxy.buf`,
of :class:`~mpservice.multiprocessing.server_process.MemoryBlockProxy`.
The property ``buf`` returns a ``memoryview`` of the shared memory.

The block of shared memory is released/destroyed once all references to the ``MemoryBlockProxy``
object have been garbage collected, or when the server process terminates.

Nested proxies
==============

The standard ``Manager`` allows "nested proxies" in a client process (not the "server process").
For example,

::

    with ServerProcess() as server:
        lst = server.list()
        dct = server.dict()
        dct['a'] = 3
        dct['b'] = 4
        lst.append(dct)

Now, the first element of ``lst`` is a dict proxy. You may pass ``lst`` to another process
and manipulate its first element; the changes are reflected in the dict that resides in the server process.

Return proxies from methods of hosted objects
=============================================

By default, when you call a method of a proxy, the returned value is plain old data passed from
the server process to the caller in the client process via pickling.
Once received, that value has no relation to the server process.

The standard ``Manager`` allows a method of a registered class to return a proxy
by instructions via the parameter ``method_to_typeid``.
However, the set up can be inconvenient, and the capability is limited.

:class:`~mpservice.multiprocessing.server_process.ServerProcess` supports returning
a mix of proxy and non-proxy data elements in convenient and powerful ways.
Suppose we have a class ``Worker``, which we will run in a server process;
further, a method of ``Worker`` will return a dict that are plain data in some parts and
proxies in some others. We can do something like this::

    from mpservice.multiprocessing.server_process import MemoryBlock, ServerProcess, managed_memoryblock, managed_list

    class Worker:
        def make_data(self):
            return {
                'name': 'tom',
                'hobbies': ['soccor', 'swim'],
                'mem': managed_memoryblock(MemoryBlock(100)),
                'jobs': ('IBM', 'APPLE', {'UK': managed_list(['ARM', 'Shell'])}),
            }

        def get_memory(self, size):
            return managed_memoryblock(MemoryBlock(size))

    ServerProcess.register('Worker', Worker)

Then we may use it like so::

    from mpservice.multiprocessing import Process

    def agent(data):
        assert data['name'] == 'tom'
        data['hobbies'].append('tennis')
        data['mem'].buf[3] = 8
        data['jobs][2]['UK'].append('BP')

    with ServerProcess() as server_process:
        worker = server_process.Worker()
        data = worker.make_data()
        p = Process(target=agent, args=(data, ))
        p.start()
        p.join()

        # data['hobbis'] is not hosted; it's modification in one process
        # will not be reflected in other processes.
        assert len(data['hobbies]) == 2

        assert data['mem'].buf[3] == 8
        assert data['jobs'][2]['UK'][2] == 'BP'

        mem = worker.get_memory(80)
        assert isinstance(mem, MomeryBlockProxy)
        assert mem.size == 80
"""
from __future__ import annotations

import functools
import multiprocessing
import multiprocessing.context
import multiprocessing.managers
import multiprocessing.pool
import multiprocessing.queues
import sys
import threading
import weakref
from multiprocessing import util
from multiprocessing.connection import XmlListener
from multiprocessing.managers import (
    Array,
    ArrayProxy,
    BaseProxy,
    DictProxy,
    ListProxy,
    Namespace,
    NamespaceProxy,
    Token,
    Value,
    ValueProxy,
    dispatch,
)
from traceback import format_exc

from ._context import SyncManager
from .remote_exception import RemoteException

__all__ = [
    'ServerProcess',
    'managed',
    'managed_list',
    'managed_dict',
    'managed_value',
    'managed_array',
    'managed_namespace',
]


class _ProcessServer(multiprocessing.managers.Server):
    # This is the cpython code in versions 3.7-3.12.
    # My fix is labeled "FIX".
    def serve_client(self, conn):
        """
        Handle requests from the proxies in a particular process/thread
        """
        util.debug(
            'starting server thread to service %r', threading.current_thread().name
        )

        recv = conn.recv
        send = conn.send
        id_to_obj = self.id_to_obj

        while not self.stop_event.is_set():
            try:
                methodname = obj = None
                request = recv()
                ident, methodname, args, kwds = request

                try:
                    obj, exposed, gettypeid = id_to_obj[ident]
                except KeyError as ke:
                    try:
                        obj, exposed, gettypeid = self.id_to_local_proxy_obj[ident]
                    except KeyError:
                        raise ke

                if methodname not in exposed:
                    raise AttributeError(
                        'method %r of %r object is not in exposed=%r'
                        % (methodname, type(obj), exposed)
                    )

                function = getattr(obj, methodname)
                # `function` carries a ref to ``obj``.

                try:
                    res = function(*args, **kwds)
                except Exception as e:
                    # FIX
                    # msg = ('#ERROR', e)
                    msg = ('#ERROR', RemoteException(e))

                else:
                    typeid = gettypeid and gettypeid.get(methodname, None)
                    if typeid:
                        # FIX:
                        # In the official version, the return received in `BaseProxy._callmethod`
                        # will need the `manager` object to assemble the proxy object.
                        # For this reason, such proxy-returning methods can not be called on a proxy
                        # that is passed to another process, b/c the original manager object is not available.
                        #
                        # The hacked version below works in this situation.

                        # rident, rexposed = self.create(conn, typeid, res)
                        # token = Token(typeid, self.address, rident)
                        # msg = ('#PROXY', (rexposed, token))
                        msg = ('#RETURN', managed(res, typeid=typeid))
                    else:
                        msg = ('#RETURN', res)

                    # FIX
                    # del res

                # FIX:
                # If no more request is coming, then `function` and `res` will stay around.
                # If `function` is a instance method of `obj`, then it carries a reference to `obj`.
                # Also, `res` is a refernce to the object that has been tracked in `self.id_to_obj`
                # and "returned" to the requester.
                # The extra reference to `obj` and `res` lingering here have no use, yet can cause
                # sutble bugs in applications that make use of their ref counts.
                # del function
                # del obj

            except AttributeError:
                if methodname is None:
                    msg = ('#TRACEBACK', format_exc())
                else:
                    try:
                        fallback_func = self.fallback_mapping[methodname]
                        result = fallback_func(self, conn, ident, obj, *args, **kwds)
                        msg = ('#RETURN', result)

                        del obj  # HACK
                    except Exception:
                        msg = ('#TRACEBACK', format_exc())

            except EOFError:
                util.debug(
                    'got EOF -- exiting thread serving %r',
                    threading.current_thread().name,
                )
                sys.exit(0)

            except Exception:
                msg = ('#TRACEBACK', format_exc())

            try:
                try:
                    send(msg)
                except Exception:
                    send(('#UNSERIALIZABLE', format_exc()))
            except Exception as e:
                util.info(
                    'exception in thread serving %r', threading.current_thread().name
                )
                util.info(' ... message was %r', msg)
                util.info(' ... exception was %r', e)
                conn.close()
                sys.exit(1)

    def shutdown(self, c):
        memoryblocks = getattr(self, '_memoryblocks_', None)
        if memoryblocks:
            # If there are `MemoryBlockProxy` objects alive when the manager exists its context manager,
            # the corresponding shared memory blocks will live beyond the server process, leading to
            # warnings like this:
            #
            #    UserWarning: resource_tracker: There appear to be 3 leaked shared_memory objects to clean up at shutdown
            #
            # Because we intend to use this server process as a central manager of the shared memory blocks,
            # there shouldn't be any user beyond the lifespan of this server, hence we release these memory blocks.
            #
            # If may be good practice to explicitly `del` `MemoryBlockProxy` objects before the manager shuts down.
            # However, this may be unpractical if there are many proxies, esp if they are nested in other objects.
            #
            # TODO: print some warning or info?
            for m in memoryblocks:
                m.__del__()  # `del m` may not be enough b/c its ref count could be > 1.

        super().shutdown(c)


class ServerProcess(SyncManager):
    # Overhead:
    #
    # I made a naive example, where the registered class just returns ``x + x``.
    # I was able to finish 13K calls per second.

    # ``ServerProcess`` instances can't be (naively) pickled, hence can't be passed to other processes.
    # As a result, with the official code, if you pass a proxy object to another process and call its methods in the other process,
    # the methods can't be ones that return proxy objects, because proxy objects would need a reference to
    # the "manager", while a proxy object does not carry its "manager" attribute during pickling.
    # However, this restriction is removed by our code hack in `_ProcessServer.serve_client`.

    _Server = _ProcessServer


"""
With ``BaseProxy``, whenever a proxy object is garbage collected, ``decref`` is called
on the hosted object in the server process. Once the refcount of the hosted object
drops to 0, the object is discarded.

If a proxy object is put in a queue and is removed from the code scope, then this proxy
object is garbage collected once ``pickle.dumps`` is finished in the queue. The "data-in-pickle-form"
of course does not contain a reference to the proxy object. This can lead to gabage collection
of the object hosted in the server process.

In some use cases, this may be undesirable. (See ``MemoryBlock`` and ``MemoryBlockProxy`` for an example.)
This hack makes pickled data count as one reference to the server-process-hosted object.
"""

_BaseProxy_reduce_orig = BaseProxy.__reduce__


def _picklethrough_reduce_(self):
    # The only sensible case of pickling this proxy object is for
    # transfering this object between processes, e.g., in a queue.
    # (This proxy object is never pickled for storage.)
    #
    # Using ``MemoryBlock`` for example,
    # it is possible that at some time the only "reference"
    # to a certain shared memory block is in pickled form in
    # a queue. For example,
    #
    #   # main process
    #   with ServerProcess() as server:
    #        q = server.Queue()
    #        w = Process(target=..., args=(q, ))
    #        w.start()
    #
    #        for _ in range(1000):
    #           mem = server.MemoryBlock(100)
    #           # ... computations ...
    #           # ... write data into `mem` ...
    #           q.put(mem)
    #           ...
    #
    # During the loop, `mem` goes out of scope and only lives on in the queue.
    # When `mem` gets re-assigned in the next iteration, the previous proxy object
    # is garbage collected, causing the target object to be deleted in the server
    # because its ref count drops to 0 (the pickled stuff in the queue is "dead wood").
    #
    # This revised `__reduct__` fixes this problem.

    # Inc ref here to represent the pickled object in the queue.
    # When unpickling, we do not inc ref again. In effect, we move the call
    # to ``incref`` earlier from ``pickle.loads`` into ``pickle.dumps``
    # for this object.

    conn = self._Client(self._token.address, authkey=self._authkey)
    dispatch(conn, None, 'incref', (self._id,))
    # TODO: not calling `self._incref` here b/c I don't understand the impact
    # of `self._idset`. Maybe we should use `self._incref`?

    func, args = _BaseProxy_reduce_orig(self)
    return rebuild_picklethrough_proxy, (func, args)


# Hack
BaseProxy.__reduce__ = _picklethrough_reduce_


def rebuild_picklethrough_proxy(func, args):
    obj = func(*args)

    # Counter the extra `incref` that's done in `BaseProxy.__init__`.
    kwds = args[-1]
    if kwds.get('incref', True) and not kwds.get('manager_owned', False):
        conn = obj._Client(obj._token.address, authkey=obj._authkey)
        dispatch(conn, None, 'decref', (obj._id,))

    return obj


def managed(obj, *, typeid: str = None):
    '''
    This function wraps part of a data structure, such as one value in a dict, as a proxy.
    '''
    server = getattr(multiprocessing.current_process(), '_manager_server', None)
    if not server:
        return obj

    if not typeid:
        typeid = type(obj).__name__
        callable, *_ = server.registry[typeid]  # If KeyError, it's user's fault
        assert callable is None
    rident, rexposed = server.create(None, typeid, obj)
    # This has called `incref` once. Ref count is 1.

    token = Token(typeid, server.address, rident)
    proxytype = server.registry[typeid][-1]
    serializer = 'xmlrpclib' if isinstance(server.listener, XmlListener) else 'pickle'

    proxy = proxytype(
        token,
        serializer=serializer,
        authkey=server.authkey,
        incref=True,
        exposed=rexposed,
        manager_owned=False,
    )
    # TODO:
    # `manager_owned=True` would suppress incref in proxy init; it also means there's no `decref` when the proxy dies.
    # I feel `manager_owned=False` is the right way to go.

    # This proxy object called `incref` once in its `__init__`. Ref count is 2.

    server.decref(None, rident)
    # Now ref count is 1.

    return proxy
    # When this object is sent back to the request:
    #  (1) pickling (i.e. `__reduce__`) inc ref count to 2;
    #  (2) this proxy object goes out of scope, its finalizer is triggered, ref count becomes 1;
    #  (3) unpickling by `rebuild_picklethrough_proxy` keeps ref count unchanged


ServerProcess.register(
    'ManagedList', callable=None, proxytype=ListProxy, create_method=False
)
ServerProcess.register(
    'ManagedDict', callable=None, proxytype=DictProxy, create_method=False
)
ServerProcess.register(
    'ManagedValue', callable=None, proxytype=ValueProxy, create_method=False
)
ServerProcess.register(
    'ManagedArray', callable=None, proxytype=ArrayProxy, create_method=False
)
ServerProcess.register(
    'ManagedNamespace', callable=None, proxytype=NamespaceProxy, create_method=False
)


managed_list = functools.partial(managed, typeid='ManagedList')
managed_dict = functools.partial(managed, typeid='ManagedDict')
managed_value = functools.partial(managed, typeid='ManagedValue')
managed_array = functools.partial(managed, typeid='ManagedArray')
managed_namespace = functools.partial(managed, typeid='ManagedNamespace')


# Think through ref count dynamics in these scenarios:
#
#   - output of `managed(...)` gets sent to client; the output proxy then goes away in server
#   - output of `managed(...)` is put in something (say a dict), which is sent to client as proxy
#   - a proxy out side of server gets added to a proxy-ed object in server, e.g. client calls
#
#           data['a'] = b
#
#     where `data` is a dict proxy, and `b` is some proxy.
#   - the `b` above gets passed to user again, i.e. client calls
#
#           z = data['a']


try:
    from multiprocessing.shared_memory import SharedMemory
except ImportError:
    pass
else:

    class MemoryBlock:
        """
        This class is used within the "server process" of a ``ServerProcess`` to
        create and track shared memory blocks.
        """

        def __init__(self, size: int):
            assert size > 0
            self._mem = SharedMemory(create=True, size=size)

            server = (
                multiprocessing.current_process()._manager_server
            )  # this attribute must exist
            all_blocks = getattr(server, '_memoryblocks_', None)
            if all_blocks is None:
                all_blocks = server._memoryblocks_ = weakref.WeakSet()
            all_blocks.add(self)

        def _name(self):
            # This is for use by ``MemoryBlockProxy``.
            return self._mem.name

        @property
        def name(self):
            return self._mem.name

        @property
        def size(self):
            # This could be slightly larger than the ``size`` passed in to :meth:`__init__`.
            return self._mem.size

        @property
        def buf(self):
            return self._mem.buf

        def _list_memory_blocks(self):
            """
            Return a list of the names of the shared memory blocks
            created and tracked by this class.
            This list includes the memory block created by the current
            ``MemoryBlock`` instance; in other words, the list
            includes ``self.name``.

            This is for use by ``MemoryBlockProxy``.
            This is mainly for debugging purposes.
            """
            return [
                m.name
                for m in multiprocessing.current_process()._manager_server._memoryblocks_
            ]

        def __del__(self):
            """
            Release the shared memory when this object is garbage collected.
            """
            self._mem.close()
            self._mem.unlink()

        def __repr__(self):
            return f'<{self.__class__.__name__} {self.name}, size {self.size}>'

    class MemoryBlockProxy(BaseProxy):
        _exposed_ = ('_list_memory_blocks', '_name')

        def __init__(
            self, *args, name: str = None, size: int = None, incref=True, **kwargs
        ):
            super().__init__(*args, incref=incref, **kwargs)
            self._name = name
            self._size = size
            self._mem = None

        def __reduce__(self):
            func, args = super().__reduce__()
            kwds = args[-1][-1]
            kwds['name'] = self._name
            kwds['size'] = self._size
            return func, args

        @property
        def name(self):
            """
            Return the name of the ``SharedMemory`` object.
            """
            if self._name is None:
                self._name = self._callmethod('_name')
            return self._name

        @property
        def size(self) -> int:
            """
            Return size of the memory block in bytes.
            """
            if self._mem is None:
                self._mem = SharedMemory(name=self.name, create=False)
            return self._mem.size

        @property
        def buf(self) -> memoryview:
            """
            Return a ``memoryview`` into the context of the memory block.
            """
            if self._mem is None:
                self._mem = SharedMemory(name=self.name, create=False)
            return self._mem.buf

        def _list_memory_blocks(self, *, include_self=True) -> list[str]:
            """
            Return names of shared memory blocks being
            tracked by the ``ServerProcess`` that "ownes" the current
            proxy object.

            You can call this method on any memoryblock proxy object.
            If for some reason there's no such proxy handy, you can create a tiny, temporary member block
            for this purpose:

                blocks = manager.MemoryBlock(1)._list_memory_blocks(include_self=False)
            """
            blocks = self._callmethod('_list_memory_blocks')
            if not include_self:
                blocks.remove(self.name)
            return blocks

        def __str__(self):
            return f"<{self.__class__.__name__} '{self.name}' at {self._id}, size {self.size}>"

    ServerProcess.register('MemoryBlock', MemoryBlock, proxytype=MemoryBlockProxy)
    ServerProcess.register(
        'ManagedMemoryBlock', None, proxytype=MemoryBlockProxy, create_method=False
    )

    managed_memoryblock = functools.partial(managed, typeid='ManagedMemoryBlock')

    __all__.extend(('MemoryBlock', 'MemoryBlockProxy', 'managed_memoryblock'))
