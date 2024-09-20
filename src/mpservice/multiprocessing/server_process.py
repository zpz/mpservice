"""
The module `mpservice.multiprocessing.server_process` contains some fixes and enhancements to the
standard module `multiprocessing.managers <https://docs.python.org/3/library/multiprocessing.html#managers>`_.

For general understanding of the standard module, see `this blog <https://zpz.github.io/blog/python-mp-manager-1>`_.
Most of the fixes are discussed in `this blog <https://zpz.github.io/blog/python-mp-manager-2>`_.
Most of the enhances are discussed in `this blog <https://zpz.github.io/blog/python-mp-manager-3>`_.

The most visible enhancements are:

    - If a proxy method fails in the server, the same exception is raised on the client side with useful traceback.
    - Server-side proxies via function `managed`.
    - Support for shared memory blocks.


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

   By now, ``server`` has started a "server process". We create things in the server process
   via methods that have been "registered" on ``server``. Then we interact with these "remote" things
   via their "proxies".

   Create instances of the registered classes within the server process::

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

   Multiple ``ServerProcess`` objects would run their corresponding
   server processes independently.

3. Pass the proxy objects to any process and use them there.

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
   to handle all requests from that proxy object.

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


Server-side proxies
===================

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

Then we may use it like this::

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
import os
import sys
import threading
import types
from multiprocessing import current_process, util
from multiprocessing.managers import (
    Array,
    BaseManager,
    Namespace,
    State,
    Token,
    Value,
    convert_to_error,
    dispatch,
    get_spawning_popen,
    listener_client,
    public_methods,
)
from multiprocessing.managers import (
    BaseProxy as _BaseProxy_,
)
from multiprocessing.managers import (
    Server as _Server_,
)
from traceback import format_exc

from .context import MP_SPAWN_CTX
from .remote_exception import RemoteException

__all__ = [
    'ServerProcess',
    'add_proxy_methods',
    'managed',
    'managed_list',
    'managed_dict',
    'managed_value',
    'managed_array',
    'managed_namespace',
    'managed_iterator',
    'BaseProxy',
    'AutoProxy',
]


def get_server(address=None):
    # If `None`, the caller is not running in the manager server process.
    # Otherwise, the return is the `Server` object that is running in the current process.
    server = getattr(current_process(), '_manager_server', None)
    if server is None:
        return None
    if address is None or server.address == address:
        return server
    return None


class Server(_Server_):
    def __init__(self, registry, address, authkey, serializer):
        super().__init__(
            registry=registry, address=address, authkey=authkey, serializer=serializer
        )
        self.serializer = serializer
        delattr(self, 'id_to_local_proxy_obj')  # disable this

    def _wrap_user_exc(self, exc):
        return RemoteException(exc)

    def _callmethod(self, conn, ident, methodname, args, kwds):
        obj, exposed, gettypeid = self.id_to_obj[ident]

        try:
            function = getattr(obj, methodname)
            # `function` carries a ref to ``obj``.
        except AttributeError:
            if methodname is None:
                msg = ('#TRACEBACK', format_exc())
            else:
                try:
                    fallback_func = self.fallback_mapping[methodname]
                    result = fallback_func(self, conn, ident, obj, *args, **kwds)
                    msg = ('#RETURN', result)
                except Exception:
                    msg = ('#TRACEBACK', format_exc())
            return msg

        try:
            res = function(*args, **kwds)
        except Exception as e:
            msg = ('#ERROR', self._wrap_user_exc(e))
            return msg

        typeid = gettypeid and gettypeid.get(methodname, None)
        if typeid:
            proxy = self.create(conn, typeid, res)
            msg = ('#PROXY', proxy)
        else:
            msg = ('#RETURN', res)

        return msg

    # This is the cpython code in versions 3.7-3.12, with my changes,
    # including factoring out a helper method `_callmethod`.
    def serve_client(self, conn):
        """
        Handle requests from the proxies in a particular process/thread
        """
        util.debug(
            'starting server thread to service %r', threading.current_thread().name
        )

        recv = conn.recv
        send = conn.send

        while not self.stop_event.is_set():
            try:
                methodname = None
                request = recv()
                ident, methodname, args, kwds = request
                msg = self._callmethod(conn, ident, methodname, args, kwds)

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

    def debug_info(self, c):
        with self.mutex:
            return [
                {
                    'id': k,
                    'refcount:': self.id_to_refcount[k],
                    'type': type(v[0]).__name__,
                    'preview': str(v[0])[:75],
                }
                for k, v in self.id_to_obj.items()
                if k != '0'
            ]  # in order of object creation

    def _make_proxy(self, typeid, proxytype, ident, exposed):
        token = Token(typeid, self.address, ident)

        if typeid == 'ManagedMemoryBlock':
            mem = self.id_to_obj[ident][0]
            proxy = proxytype(
                token,
                serializer=self.serializer,
                authkey=self.authkey,
                name=mem.name,
                size=mem.size,
            )
        else:
            try:
                is_cls = issubclass(proxytype, BaseProxy)
            except TypeError:
                is_cls = False
            if is_cls:
                proxy = proxytype(
                    token,
                    self.serializer,
                    authkey=self.authkey,
                )
            else:
                proxy = proxytype(
                    token,
                    self.serializer,
                    authkey=self.authkey,
                    exposed=exposed,
                )

        # This calls `incref` once

        return proxy

    # Changes to the original version:
    #   - return proxy object
    #   - do not use `exposed` in registry
    #   - do not call `incref` as proxy-initiation calls that
    def create(self, c, typeid, /, *args, **kwds):
        """
        Create a new shared object and return its id
        """
        with self.mutex:
            callable, exposed, method_to_typeid, proxytype = self.registry[typeid]

            if callable is None:
                if kwds or (len(args) != 1):
                    raise ValueError(
                        'Without callable, must have one non-keyword argument'
                    )
                obj = args[0]
            else:
                obj = callable(*args, **kwds)

            # `exposed` in registry is always None as this module has removed that
            # from registration.
            exposed = public_methods(obj)
            if method_to_typeid is not None:
                if not isinstance(method_to_typeid, dict):
                    raise TypeError(
                        'Method_to_typeid {0!r}: type {1!s}, not dict'.format(
                            method_to_typeid, type(method_to_typeid)
                        )
                    )
                exposed = list(exposed) + list(method_to_typeid)

            ident = '%x' % id(obj)  # convert to string because xmlrpclib
            # only has 32 bit signed integers
            util.debug('%r callable returned object with id %r', typeid, ident)

            self.id_to_obj[ident] = (obj, set(exposed), method_to_typeid)
            if ident not in self.id_to_refcount:
                self.id_to_refcount[ident] = 0

        return self._make_proxy(typeid, proxytype, ident, tuple(exposed))

    def incref(self, c, ident):
        with self.mutex:
            self.id_to_refcount[ident] += 1

    def decref(self, c, ident):
        assert (
            ident in self.id_to_refcount
        )  # disable the use of `self.id_to_local_proxy_obj`
        super().decref(c, ident)


class ServerProcess(BaseManager):
    # Overhead:
    #
    # I made a naive example, where the registered class just returns ``x + x``.
    # I was able to finish 13K calls per second.

    _Server = Server

    def __init__(
        self,
        address=None,
        authkey=None,
        serializer='pickle',
        ctx=None,
        *,
        name: str | None = None,
        cpu: int | list[int] | None = None,
        **kwargs,
    ):
        """
        Parameters
        ----------
        name
            Name of the server process. If ``None``, a default name will be created.
        cpu
            Specify CPU pinning for the server process.
        """
        super().__init__(
            address=address,
            authkey=authkey,
            serializer=serializer,
            ctx=ctx or MP_SPAWN_CTX,
            **kwargs,
        )
        self._name = name
        self._cpu = cpu

    def start(self, *args, **kwargs):
        z = super().start(*args, **kwargs)
        if self._name:
            self._process.name = self._name
        if self._cpu is not None:
            cpu = self._cpu
            if isinstance(cpu, int):
                cpu = [cpu]
            os.sched_setaffinity(self._process.pid, cpu)
        return z

    def __reduce__(self):
        if get_spawning_popen() is not None:
            auth = self._authkey
        else:
            auth = None
        return (
            self._rebuild_manager,
            (type(self), self._address, auth, self._serializer),
        )

    @staticmethod
    def _rebuild_manager(cls, address, authkey, serializer):
        # The rebuilt manager object should be used to create registered class instances in the server
        # and nothing else. Do not use its context manager and do not "start" it (as it's already running).
        # Its lifetime should be contained in the original object.
        #
        # It seems that changes to a class object within a function are not carried in pickles,
        # hence you can't register a custom class within a function and hope to use that class on a manager
        # out of unpickling, e.g., the following will not work:
        #
        #   def worker(manager):
        #       myclass = manager.MyClass()
        #       ....
        #
        #   def myfunc():
        #       ServerProcess.register('MyClass', MyClass)
        #       with ServerProcess() as manager:
        #           p = Process(target=worker, args=(manager,))
        #           p.start()
        #           p.join()
        #
        # Instead, do registration in the global scope.

        obj = cls(address, authkey, serializer)
        obj._ctx = None
        obj.connect()
        obj.start = None
        return obj

    def _create(self, *args, **kwargs):
        raise RuntimeError('the method `_create` has been removed')

    # Changes to the standard version:
    #   - use our custom `AutoProxy`
    #   - remove parameter `exposed`
    #   - simplify the created method
    @classmethod
    def register(
        cls,
        typeid,
        callable=None,
        proxytype=None,
        *,
        method_to_typeid=None,
        create_method=True,
    ):
        if '_registry' not in cls.__dict__:
            cls._registry = cls._registry.copy()

        # FIX: disallow overwriting existing registry.
        if typeid in cls._registry:
            raise ValueError(f"typeid '{typeid}' is already registered")
            # See method `unregister`.

        if proxytype is None:
            proxytype = AutoProxy

        if method_to_typeid:
            for key, value in list(method_to_typeid.items()):  # isinstance?
                assert type(key) is str, '%r is not a string' % key  # noqa: E721
                assert type(value) is str, '%r is not a string' % value  # noqa: E721

        cls._registry[typeid] = (callable, None, method_to_typeid, proxytype)

        if create_method:

            def temp(self, /, *args, **kwds):
                util.debug('requesting creation of a shared %r object', typeid)

                assert self._state.value == State.STARTED, 'server not yet started'
                conn = self._Client(self._address, authkey=self._authkey)
                try:
                    proxy = dispatch(conn, None, 'create', (typeid,) + args, kwds)
                finally:
                    conn.close()

                return proxy

            temp.__name__ = typeid
            setattr(cls, typeid, temp)

    @classmethod
    def unregister(cls, typeid):
        del cls._registry[typeid]


class BaseProxy(_BaseProxy_):
    # Changes to the original version:
    #    - remove parameter `manager_owned`--fixing it at False, because I don't want the `__init__`
    #      to skip `incref` when `manager_owned` would be True.
    #    - remove parameter `manager`.
    #    - remove parameter `exposed`.
    def __init__(self, token, serializer, authkey=None, incref=True):
        self._server = get_server(token.address)
        super().__init__(
            token,
            serializer,
            manager=None,
            authkey=authkey,
            exposed=None,
            incref=incref,
            manager_owned=False,
        )

    # Changes to the original version:
    #   - shortcut if running in server
    #   - simplify the `kind == 'PROXY'` case
    def _callmethod(self, methodname, args=(), kwds={}):
        """
        Try to call a method of the referent and return a copy of the result
        """

        server = self._server
        if server:
            kind, result = server._callmethod(
                None, self._token.id, methodname, args, kwds
            )
        else:
            try:
                conn = self._tls.connection
            except AttributeError:
                util.debug(
                    'thread %r does not own a connection',
                    threading.current_thread().name,
                )
                self._connect()
                conn = self._tls.connection

            conn.send((self._id, methodname, args, kwds))
            kind, result = conn.recv()

        if kind == '#RETURN':
            return result
        elif kind == '#PROXY':
            return result

        raise convert_to_error(kind, result)

    def _dispatch(self, methodname):
        conn = self._Client(self._token.address, authkey=self._authkey)
        dispatch(conn, None, methodname, (self._id,))

    # Changes to the original version:
    #   - do not check `self._owned_by_manager`
    #   - use shortcut if this is running inside the server process
    #   - do not use `self._manager`, hence the finalizer does not take `state`
    #   - the finalizer takes `server`
    def _incref(self):
        server = self._server
        if server:
            server.incref(None, self._token.id)
        else:
            self._dispatch('incref')

        self._idset.add(self._id)

        self._close = util.Finalize(
            self,
            type(self)._decref,
            args=(
                self._token,
                self._authkey,
                self._tls,
                self._idset,
                self._Client,
                self._server,
            ),
            # exitpriority=10,
        )

    # Changes to the original version:
    #   - no parameter `state`
    #   - use shortcut if this is running in server
    @staticmethod
    def _decref(token, authkey, tls, idset, _Client, server):
        idset.discard(token.id)

        if server:
            server.decref(None, token.id)
        else:
            try:
                util.debug('DECREF %r', token.id)
                conn = _Client(token.address, authkey=authkey)
                dispatch(conn, None, 'decref', (token.id,))
            except Exception as e:
                util.debug('... decref failed %s', e)

        # check whether we can close this thread's connection because
        # the process owns no more references to objects for this manager
        if not idset and hasattr(tls, 'connection'):
            util.debug(
                'thread %r has no more proxies so closing conn',
                threading.current_thread().name,
            )
            tls.connection.close()
            del tls.connection

    # Changes to the standard version:
    #   1. call `incref` before returning
    #   2. use our custom `RebuildProxy` and `AutoProxy` in place of the standard versions
    #   3. changes about returning authkey
    def __reduce__(self):
        # Inc refcount in case the remote object is gone before unpickling happens.
        server = self._server
        if server:
            server.incref(None, self._token.id)
        else:
            conn = self._Client(self._token.address, authkey=self._authkey)
            dispatch(conn, None, 'incref', (self._id,))

        kwds = {}
        if get_spawning_popen() is not None:
            kwds['authkey'] = self._authkey
        elif self._server:
            kwds = {'authkey': bytes(self._authkey)}
            # Bypass a security check of `multiprocessing.process.AuthenticationString`,
            # because returning a proxy from the server is safe.

        if getattr(self, '_isauto', False):
            kwds['exposed'] = self._exposed_
            return (RebuildProxy, (AutoProxy, self._token, self._serializer, kwds))
        else:
            return (RebuildProxy, (type(self), self._token, self._serializer, kwds))


#
# Function used for unpickling
#


# Changes to the standard version:
#  1. do not add 'manager_owned' to `kwds`
#  2. call `decref` after object creation
def RebuildProxy(func, token, serializer, kwds):
    """
    Function used for unpickling proxy objects.
    """
    incref = kwds.pop('incref', True) and not getattr(
        current_process(), '_inheriting', False
    )
    obj = func(token, serializer, incref=incref, **kwds)
    # `func` is either `AutoProxy` or a subclass of `BaseProxy`.
    # TODO: it appears `incref` is True some times and False some others, affecting by the '_inheriting` condition.
    # Understand the `'_inheriting'` thing.

    if incref:
        # Counter the extra `incref` that's done in `BaseProxy.__init__`.
        server = obj._server
        if server:
            server.decref(None, token.id)
        else:
            obj._dispatch('decref')

    return obj


#
# Functions to create proxies and proxy types
#


def add_proxy_methods(*method_names: str):
    """
    Custom BaseProxy subclasses can use this as a class decorator
    to add methods that simply call `_callmethod`. The subclass may
    have other methods that address special concerns (besides calling
    `_callmethod`, if applicable).
    """

    def decorator(cls):
        for meth in method_names:
            dic = {}
            exec(
                """def %s(self, /, *args, **kwargs):
                return self._callmethod(%r, args, kwargs)"""
                % (meth, meth),
                dic,
            )
            setattr(cls, meth, dic[meth])

        return cls

    return decorator


# Changes to the standard version:
#   1. use our custom `make_proxy_type`
#   2. remove parameter `manager_owned`
def AutoProxy(token, serializer, authkey=None, exposed=None, incref=True):
    """
    This function creates an instance of a BaseProxy subclass that is
    dynamically defined inside this function.

    This function is used when the argument `proxytype` to `BaseManager.register` or `ServerProcess.register`
    is not provided.
    """
    _Client = listener_client[serializer][1]
    server = get_server(token.address)

    if exposed is None:  # TODO: does this ever happen?
        if server:
            exposed = server.get_methods(None, token)
        else:
            conn = _Client(token.address, authkey=authkey)
            try:
                exposed = dispatch(conn, None, 'get_methods', (token,))
            finally:
                conn.close()

    if authkey is None:
        authkey = current_process().authkey

    def make_proxy_type(name, exposed, _cache={}):
        """
        Return a proxy type whose methods are given by `exposed`
        """
        exposed = tuple(exposed)
        try:
            return _cache[(name, exposed)]
        except KeyError:
            pass

        ProxyType = add_proxy_methods(*exposed)(type(name, (BaseProxy,), {}))
        _cache[(name, exposed)] = ProxyType
        return ProxyType

    ProxyType = make_proxy_type('AutoProxy[%s]' % token.typeid, exposed)

    proxy = ProxyType(token, serializer, authkey=authkey, incref=incref)

    proxy._isauto = True
    proxy._exposed_ = exposed
    return proxy


#
# Functions for nested proxies
#


def managed(obj, *, typeid: str = None):
    """
    This function is used within a server process.
    It creates a proxy for the input `obj`.
    The resultant proxy can be sent back to a requester outside of the server process.
    The proxy may also be saved in another data structure (either residing in the server or sent back to
    the client), e.g. as an element in a managed list or dict.
    """
    server = get_server()
    if not server:
        return obj

    if not typeid:
        typeid = type(obj).__name__
        try:
            callable, *_, proxytype = server.registry[typeid]
        except KeyError:
            typeid = 'Managed' + typeid.title()
            try:
                callable, *_ = server.registry[typeid]
                if callable is not None:
                    raise ValueError(
                        '`typeid` is not provided and a suitable registry is not found'
                    )
            except KeyError:
                server.registry[typeid] = (None, None, None, AutoProxy)
                # TODO: delete after this single use?
        else:
            if callable is not None:
                typeid = 'Managed' + typeid.title()
                try:
                    callable, *_ = server.registry[typeid]
                    if callable is not None:
                        raise ValueError(
                            '`typeid` is not provided and a suitable registry is not found'
                        )
                except KeyError:
                    server.registry[typeid] = (None, None, None, proxytype)
                    # TODO: delete after this single use?

    proxy = server.create(None, typeid, obj)

    return proxy
    # When this object is sent back to the request:
    #  (1) pickling (i.e. `__reduce__`) inc ref count to 2;
    #  (2) this proxy object goes out of scope, its finalizer is triggered, ref count becomes 1;
    #  (3) unpickling by `RebuildProxy` keeps ref count unchanged


#
# Define and register some common classes
#
# Redefine the proxy classes to use our custom base class.
#
# Can not monkey-patch the proxy class objects in the standard lib, as that would
# cause errors like this:
#
#   _pickle.PicklingError: Can't pickle <class 'multiprocessing.managers.DictProxy'>: it's not the same object as multiprocessing.managers.DictProxy
#
# Do not register Lock, Queue, Event, etc. I don't see advantages of them compared to the standard solution from `multiprocessing` directly.


class IteratorProxy(BaseProxy):
    def __iter__(self):
        return self

    def __next__(self, *args):
        return self._callmethod('__next__', args)

    def send(self, *args):
        return self._callmethod('send', args)

    def throw(self, *args):
        return self._callmethod('throw', args)

    def close(self, *args):
        return self._callmethod('close', args)


@add_proxy_methods('__getattribute__')
class NamespaceProxy(BaseProxy):
    def __getattr__(self, key):
        if key[0] == '_':
            return object.__getattribute__(self, key)
        callmethod = object.__getattribute__(self, '_callmethod')
        return callmethod('__getattribute__', (key,))

    def __setattr__(self, key, value):
        if key[0] == '_':
            return object.__setattr__(self, key, value)
        callmethod = object.__getattribute__(self, '_callmethod')
        return callmethod('__setattr__', (key, value))

    def __delattr__(self, key):
        if key[0] == '_':
            return object.__delattr__(self, key)
        callmethod = object.__getattribute__(self, '_callmethod')
        return callmethod('__delattr__', (key,))


class ValueProxy(BaseProxy):
    def get(self):
        return self._callmethod('get')

    def set(self, value):
        return self._callmethod('set', (value,))

    value = property(get, set)

    __class_getitem__ = classmethod(types.GenericAlias)


@add_proxy_methods(
    '__add__',
    '__contains__',
    '__delitem__',
    '__getitem__',
    '__len__',
    '__mul__',
    '__reversed__',
    '__rmul__',
    '__setitem__',
    'append',
    'count',
    'extend',
    'index',
    'insert',
    'pop',
    'remove',
    'reverse',
    'sort',
    '__imul__',
)
class ListProxy(BaseProxy):
    def __iadd__(self, value):
        self._callmethod('extend', (value,))
        return self

    def __imul__(self, value):
        self._callmethod('__imul__', (value,))
        return self


# Changes to the standard version:
#   - remove method `__iter__`
@add_proxy_methods(
    '__contains__',
    '__delitem__',
    '__getitem__',
    '__len__',
    '__setitem__',
    'clear',
    'copy',
    'get',
    'items',
    'keys',
    'pop',
    'popitem',
    'setdefault',
    'update',
    'values',
)
class DictProxy(BaseProxy):
    pass


@add_proxy_methods('__len__', '__getitem__', '__setitem__')
class ArrayProxy(BaseProxy):
    pass


ServerProcess.register('list', callable=list, proxytype=ListProxy)
ServerProcess.register('dict', callable=dict, proxytype=DictProxy)
ServerProcess.register('Value', callable=Value, proxytype=ValueProxy)
ServerProcess.register('Array', callable=Array, proxytype=ArrayProxy)
ServerProcess.register('Namespace', callable=Namespace, proxytype=NamespaceProxy)

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
ServerProcess.register(
    'ManagedIterator', callable=None, proxytype=IteratorProxy, create_method=False
)

managed_list = functools.partial(managed, typeid='ManagedList')
managed_dict = functools.partial(managed, typeid='ManagedDict')
managed_value = functools.partial(managed, typeid='ManagedValue')
managed_array = functools.partial(managed, typeid='ManagedArray')
managed_namespace = functools.partial(managed, typeid='ManagedNamespace')
managed_iterator = functools.partial(managed, typeid='ManagedIterator')


#
# Support shared memory
#

try:
    from multiprocessing.shared_memory import SharedMemory
except ImportError:
    pass
else:

    class MemoryBlock:
        """
        This class provides a simple tracker for a block of shared memory.

        User is expected to use this class ONLY within a server process.
        They should wrap this object with `managed_memoryblock`.

        Outside of a server process, use `manager.MemoryBlock`.
        """

        def __init__(self, size: int):
            assert size > 0
            self._mem = SharedMemory(create=True, size=size)
            self.release = util.Finalize(self, type(self)._release, args=(self._mem,))

        def _name(self):
            # This is for use by ``MemoryBlockProxy``.
            return self._mem.name

        @property
        def name(self):
            return self._mem.name

        @property
        def size(self):
            return self._mem.size

        @property
        def buf(self):
            return self._mem.buf

        @staticmethod
        def _release(mem):
            mem.close()
            mem.unlink()

        def __del__(self):
            """
            Release the shared memory when this object is garbage collected.
            """
            self.release()

        def __reduce__(self):
            # Block pickling to prevent user from accidentally using this class w/o
            # wrapping it by `managed_memoryblock`.
            raise NotImplementedError(
                f"cannot pickle instances of type '{self.__class__.__name__}"
            )

        def __repr__(self):
            return (
                f"<{self.__class__.__name__} '{self._mem.name}', size {self._mem.size}>"
            )

    class MemoryBlockProxy(BaseProxy):
        def __init__(self, *args, name: str = None, size: int = None, **kwargs):
            super().__init__(*args, **kwargs)
            self._name = name
            self._size = size
            self._mem = None

        def __reduce__(self):
            func, args = super().__reduce__()
            kwds = args[-1]
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
            if self._size is None:
                if self._mem is None:
                    self._mem = SharedMemory(name=self.name, create=False)
                self._size = self._mem.size
            return self._size

        @property
        def buf(self) -> memoryview:
            """
            Return a ``memoryview`` into the context of the memory block.
            """
            if self._mem is None:
                self._mem = SharedMemory(name=self.name, create=False)
            return self._mem.buf

        def __str__(self):
            return f"<{self.__class__.__name__} '{self.name}' at {self._id}, size {self.size}>"

    ServerProcess.register(
        'MemoryBlock', callable=MemoryBlock, proxytype=MemoryBlockProxy
    )
    ServerProcess.register(
        'ManagedMemoryBlock',
        callable=None,
        proxytype=MemoryBlockProxy,
        create_method=False,
    )

    managed_memoryblock = functools.partial(managed, typeid='ManagedMemoryBlock')

    __all__.extend(('MemoryBlock', 'managed_memoryblock'))
