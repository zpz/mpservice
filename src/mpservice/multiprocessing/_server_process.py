'''
The :class:`Manager` in ``multiprocessing`` provides a mechanism interactively
communicate with a "server" that runs in another process.
In contrast, a :class:`Queue` is used in a streaming fashion, that is, we retrieve
the next data element from a queue if it exists. We can not use it "interactively" like:
request certain data and wait for the response.

The interactivity is achieved by a "proxy": an object is created in a "server process";
the hosted object is represented by a "proxy", which is passed to and used in other processes
to communicate with the hosted object. The backbone of the communication is ``multiprocessing.connection.Connection``
with its ``send`` and ``recv`` methods.

This module provides :class:`ServerProcess`, which contains some customization to the standard
``Manager``. There are several scenarios in its use.

Basic workflow
==============

1. First, we need to register one or more classes with the :class:`ServerProcess` class::

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

    Calling one registered class multiple times, like

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

    Between the server process and the proxy object in a particular process or thread,
    a connection is established, which starts a new thread in the server process
    to handle all requests from that proxy object.
    These "requests" include calling all methods of the proxy, not just one particular method.

    For example,

    ::

        th = Thread(target=foo, args=(prox1,))
        th.start()
        ...

    Suppose in function ``foo`` the first parameter is called ``p`` (which is ``prox1`` passed in),
    then ``p`` will communicate with the ``Doubler`` object (which runs in the server process)
    via its own, new thread in the server process, separate from the connection thread for ``prox1``.

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
    With concurrent calls in 50 threads as above, it took 1.05 seconds in one experiment.

    It follows that, if the method mutates some shared state, it needs to guard things by locks.

The class ``ServerProcess`` delegates most work to :class:`multiprocessing.managers.BaseManager``,
but is dedicated to the use case where user needs to design and register a custom class
to be used in a "server process" on the same machine. The interface of ``ServerProcess``
intends to stay simpler than the standard ``BaseManager``.

If you don't need a custom class, but rather just need to use one of the standard classes,
for example, ``Event``, you can use that like this::

    from mpservice.multiprocessing import Manager

    with Manager() as manager:
        q = manager.Event()
        ...

        
Shared memory
=============

In an environment that supports shared memory, ``ServerProcess`` has two other methods:
:meth:`MemoryBlock` and :meth:`list_memory_blocks`. A simple example::

    with ServerProcess() as server:
        mem = server.MemoryBlock(1000)
        buffer = mem.buf  # memoryview
        # ... write data into `buffer`
        # pass `mem` to other processes and use its `.buf` again for the content.
        # Since it's a "shared" block of memory, any process can modify the data
        # via the memoryview.

        del mem

To release (or "destroy") the memory block, just make sure all references to ``mem``
in this "originating" and other "consuming" processes are cleared.
In the originating process (where the ServerProcess object is started), the ``del mem``
in the example above can be omitted.

See :class:`MemoryBlock`, :class:`MemoryBlockProxy`, :func:`list_memory_blocks` for more
details.

Nested proxies
==============

Return proxies from hosted objects
==================================

Usually, methods of a hosted object simply returns regular data.
The data obviously goes through pickling coming out of the server process into the user process.
The server process does not keep of copy of or reference to the data; it's sent out and gone.

A method may want to create an object, let it stay within the server process, and return a proxy to it.
For example,

::

    class MyClass:
        def make_memory(self, size):
            ...
            mem = MemoryBlock(size)
            ... # write data into the memory block
            return mem

If we simply return the ``MemoryBlock`` object,
it is tricky to manage the lifetime and release of this memory block.
Instead, this module provides facilities to manage this within the server process.
To make use of that, we need to keep the original ``MemoryBlock`` object in the server process,
and return a proxy of it. This is achieved by wrapping the returned object in ``hosted``::

    class MyClass:
        def make_memory(self, size):
            ...
            mem = MemoryBlock(size)
            ... # write data into the memory block
            return hosted(mem)

Correspondingly, the class registration needs to be revised::

            
    ServerProcess.register(
        'MyClass',
        MyClass,
        method_to_typeid={'make_memory': 'hosted'},
    )

It is quite limited if we could only return a proxy object.
In this example, we may prefer to return a dict that contains a ``MemoryBlock``, along with
a few other elements of auxiliary info.
Not a problem. We can do something like this::

    class MyClass:
        def make_memory(self, size):
            ...
            mem = MemoryBlock(size)
            ... # write data into the memory block
            return {'size': size, 'user': 'Tom', 'block': hosted(mem)}

The class registration is the same as above.
'''
from __future__ import annotations

import multiprocessing
import multiprocessing.connection
import multiprocessing.context
import multiprocessing.managers
import multiprocessing.queues
import queue
import sys
import threading
import traceback
import warnings
import weakref
from multiprocessing import util
from multiprocessing.managers import (
    BaseManager,
    BaseProxy,
    Token,
    convert_to_error,
    dispatch,
)
from traceback import format_exc

from deprecation import deprecated

from ._multiprocessing import MP_SPAWN_CTX

# In a few cases I saw ``BrokenPipeError: [Errno 32] Broken pipe``.
# A workaround is described here:
# https://stackoverflow.com/q/3649458/6178706


# This is based on the 3.10 version.
def _callmethod(self, methodname, args=(), kwds={}):
    '''
    Try to call a method of the referent and return a copy of the result
    '''
    for tries in (0, 1):
        try:
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
            break
        except BrokenPipeError as e:
            if tries == 0:
                addr = self._token.address
                if addr in self._address_to_local:
                    del self._address_to_local[addr][0].connection
                    traceback.print_exc()
                    print(f'Retry once on "{e!r}"')
                    continue
            raise

    kind, result = conn.recv()

    if kind == '#RETURN':
        return result
    elif kind == '#PROXY':
        exposed, token = result
        if token.typeid == 'hosted':
            return HostedProxy(
                address=self._token.address,
                serializer=self._serializer,
                manager=self._manager,
                conn=self._Client(self._token.address, authkey=self._authkey),
                authkey=self._authkey,
                data=exposed,
            )

        proxytype = self._manager._registry[token.typeid][-1]
        token.address = self._token.address
        proxy = proxytype(
            token,
            self._serializer,
            manager=self._manager,
            authkey=self._authkey,
            exposed=exposed,
        )
        conn = self._Client(token.address, authkey=self._authkey)
        dispatch(conn, None, 'decref', (token.id,))
        return proxy
    raise convert_to_error(kind, result)


BaseProxy._callmethod = _callmethod


class PickleThroughProxy(BaseProxy):
    '''
    With ``BaseProxy``, whenever a proxy object is garbage collected, ``decref`` is called
    on the hosted object in the server process. Once the refcount of the hosted object
    drops to 0, the object is discarded.

    If a proxy object is put in a queue and is removed from the code scope, then this proxy
    object is garbage collected once ``pickle.dumps`` is finished in the queue. The "data-in-pickle-form"
    of course does not contain a reference to the proxy object. This can lead to gabage collection
    of the object hosted in the server process.

    In some use cases, this may be undesirable. (See ``MemoryBlock`` and ``MemoryBlockProxy`` for an example.)
    This proxy classs is designed for this use case. It makes pickled data count as one reference
    to the server-process-hosted object.
    '''

    def __reduce__(self):
        # The only sensible case of pickling this proxy object is for
        # transfering this object between processes in a queue.
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
        # A main design goal is to prevent the ``MemoryBlock`` object in the server process
        # from being garbage collected. This means we need to do some ref-counting hacks.

        # Inc ref here to represent the pickled object in the queue.
        # When unpickling, we do not inc ref again. In effect, we move the call
        # to ``incref`` earlier from ``pickle.loads`` into ``pickle.dumps``
        # for this object.

        conn = self._Client(self._token.address, authkey=self._authkey)
        dispatch(conn, None, 'incref', (self._id,))
        # NOTE:
        # calling ``self._incref()`` would not work because it adds another `_decref`;
        # I don't totally understand that part.

        func, args = super().__reduce__()
        args[-1]['incref'] = False  # do not inc ref again during unpickling.
        return RebuildPickleThroughProxy, (func, args)


def RebuildPickleThroughProxy(func, args):
    obj = func(*args)

    # We have called ``incref`` in ``PickleThroughProxy.__reduce__``,
    # i.e. prior to pickling,
    # hence we don't call ``incref`` when
    # reconstructing the proxy here during unpickling.
    # However, we still need to set up calling of ``decref``
    # when this reconstructed proxy is garbage collected.

    # The code below is part of ``BaseProxy._incref``.
    # ``BaseProxy.__init__`` does not do this.

    obj._idset.add(obj._id)
    state = obj._manager and obj._manager._state
    obj._close = util.Finalize(
        obj,
        BaseProxy._decref,
        args=(obj._token, obj._authkey, state, obj._tls, obj._idset, obj._Client),
        exitpriority=10,
    )

    return obj


def HostedProxy(*, address, serializer, manager, conn, authkey=None, data):
    def make_hosted(value: hosted):
        ident, exposed = value.value
        typeid = value.typeid

        tok = Token(typeid, address, ident)
        proxytype = manager._registry[typeid][-1]
        proxy = proxytype(
            tok, serializer, manager=manager, authkey=authkey, exposed=exposed
        )
        dispatch(conn, None, 'decref', (ident,))
        return proxy

    def make_one(value):
        if isinstance(value, hosted):
            return make_hosted(value)
        if isinstance(value, list):
            return make_list(value)
        if isinstance(value, dict):
            return make_dict(value)
        if isinstance(value, tuple):
            return make_tuple(value)
        return value

    def make_list(value: list):
        return [make_one(v) for v in value]

    def make_dict(value: dict):
        return {k: make_one(v) for k, v in value.items()}

    def make_tuple(value: tuple):
        return tuple(make_one(v) for v in value)

    return make_one(data)


class hosted:
    def __init__(self, value, typeid: str = None):
        self.value = value
        self.typeid = typeid or type(value).__name__


class _ProcessServer(multiprocessing.managers.Server):

    # This is the cpython code in versions 3.7-3.12.
    # My fix is at the end labeled "FIX".
    def serve_client(self, conn):
        '''
        Handle requests from the proxies in a particular process/thread
        '''
        util.debug(
            'starting server thread to service %r', threading.current_thread().name
        )

        recv = conn.recv
        send = conn.send
        id_to_obj = self.id_to_obj

        Token = multiprocessing.managers.Token

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
                    del function  # HACK
                except Exception as e:
                    msg = ('#ERROR', e)
                else:
                    typeid = gettypeid and gettypeid.get(methodname, None)
                    if typeid:
                        rident, rexposed = self.create(conn, typeid, res)
                        token = Token(typeid, self.address, rident)
                        msg = ('#PROXY', (rexposed, token))
                    else:
                        msg = ('#RETURN', res)

                del obj  # HACK
                del res
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

            # FIX:
            # If no more request is coming, then `function` and `res` will stay around.
            # If `function` is a instance method of `obj`, then it carries a reference to `obj`.
            # Also, `res` is a refernce to the object that has been tracked in `self.id_to_obj`
            # and "returned" to the requester.
            # The extra reference to `obj` and `res` lingering here have no use, yet can cause
            # sutble bugs in applications that make use of their ref counts, such as ``MemoryBlock``..

    def _create_hosted(self, c, data):
        super_create = super().create

        def convert_hosted(value):
            value, typeid = value.value, value.typeid
            typeid_nocall = f"{typeid}-nocall"
            if typeid_nocall not in self.registry:
                self.registry[typeid_nocall] = (
                    None,
                    *self.registry[typeid][1:],
                )  # replace `callable` by `None`
                print(self.registry[typeid_nocall])

            ident, exposed = super_create(c, typeid_nocall, value)
            # By now `value` has been referenced in ``self._id_to_obj``.
            # print('id: %x' % id(value))
            # print('ident:', ident, 'exposed:', exposed)
            return hosted((ident, exposed), typeid)

        def convert_one(value):
            if isinstance(value, hosted):
                return convert_hosted(value)
            if isinstance(value, list):
                return convert_list(value)
            if isinstance(value, dict):
                return convert_dict(value)
            if isinstance(value, tuple):
                return convert_tuple(value)
            return value  # any other type is returned as is, not hosted

        def convert_list(value):
            return [convert_one(v) for v in value]

        def convert_dict(value):
            return {k: convert_one(v) for k, v in value.items()}

        def convert_tuple(value):
            return tuple(convert_one(v) for v in value)

        return 0, convert_one(data)
        # The first value, `0`, is in the spot of ``ident`` but is just a placeholder.
        # The second value is the actual data and takes the spot of ``exposed``.
        # Refer to ``Server.serve_client``.
        # This data is received by ``BaseProxy._call_method``.

    def create(self, c, typeid, /, *args, **kwds):
        if typeid == 'hosted':
            assert not kwds
            assert len(args) == 1
            return self._create_hosted(c, args[0])
        return super().create(c, typeid, *args, **kwds)


class _ProcessManager(BaseManager):

    _Server = _ProcessServer

    def __init__(self):
        super().__init__(ctx=MP_SPAWN_CTX)
        # 3.11 got parameter `shutdown_timeout`, which may be useful.

    def __enter__(self):
        super().__enter__()
        self._memoryblock_proxies = weakref.WeakValueDictionary()
        return self

    def __exit__(self, *args):
        for prox in self._memoryblock_proxies.values():  # valuerefs():
            prox._close()
        # This takes care of dangling references to ``MemoryBlockProxy`` objects
        # in such situations (via ``ServerProcess``):
        #
        #   with ServerProcess() as server:
        #       mem = server.MemoryBlock(30)
        #       ...
        #       # the name ``mem`` is not "deleted" when exiting the context
        #
        # In this case, if the treatment above is not in place, we'll get such warning:
        #
        #   /usr/lib/python3.10/multiprocessing/resource_tracker.py:224: UserWarning: resource_tracker: There appear to be 1 leaked shared_memory objects to clean up at shutdown
        #
        # See ``MemoryBlockProxy.__init__()`` for how the object is registered in
        # ``self._memoryblock_proxies``.

        super().__exit__(*args)


class ServerProcess:
    # In each new thread or process, a proxy object will create a new
    # connection to the server process (see``multiprocessing.managers.Server.accepter``,
    # ...,
    # ``Server.accept_connection``,
    # and
    # ``BaseProxy._connect``;
    # all in `Lib/multiprocessing/managers.py <https://github.com/python/cpython/blob/main/Lib/multiprocessing/managers.py>`_);
    # the server process then creates a new thread
    # to handle requests from this connection (see ``Server.serve_client``
    # also in `Lib/multiprocessing/managers.py <https://github.com/python/cpython/blob/main/Lib/multiprocessing/managers.py>`_).

    # Overhead:
    #
    # I made a naive example, where the registered class just returns ``x + x``.
    # I was able to finish 13K calls per second.

    _registry = set()

    @classmethod
    def register(
        cls,
        typeid: str,
        callable=None,
        **kwargs,
    ):
        """
        Typically, ``callable`` is a class object (not class instance) and ``typeid`` is the calss's name.

        Suppose ``Worker`` is a class, then registering ``Worker`` will add a method
        named "Worker" to the class ``ServerProcess``. Later on a running ServerProcess object
        ``server``, calling::

            server.Worker(*args, **kwargs)

        will run the callable ``Worker`` inside the server process, taking ``args`` and ``kwargs``
        as it normally would (since ``Worker`` is a class, treating it as a callable amounts to
        calling its ``__init__``). The object resulted from that call will stay in the server process.
        (In this, the call ``Worker(...)`` will result in an *instance* of the ``Worker`` class.)
        The call ``server.Worker(...)`` returns a "proxy" to that object; the proxy is going to be used
        from other processes or threads to communicate with the real object residing inside the server process.

        .. versionchanged:: 0.13.1
            Now the signature is consistent with that of :meth:`multiprocessing.managers.BaseManager.register`.

        .. note:: This method must be called before a :class:`ServerProcess` object is "started".
        """
        if not isinstance(typeid, str):
            assert callable is None
            assert not kwargs
            callable = typeid
            typeid = callable.__name__
            warnings.warn(
                "the signature of ``register`` has changed; now the first argument should be ``typeid``, which is a str",
                DeprecationWarning,
                stacklevel=2,
            )

        if typeid in cls._registry:
            warnings.warn(
                '"%s" is already registered; the existing registry is being overwritten.'
                % typeid
            )
        else:
            cls._registry.add(typeid)
        _ProcessManager.register(typeid=typeid, callable=callable, **kwargs)

    def __init__(
        self,
        *,
        cpu: int | list[int] | None = None,
        name: str | None = None,
    ):
        '''
        Parameters
        ----------
        name
            Name of the server process. If ``None``, a default name will be created.
        cpu
            Ignored in 0.13.1; will be removed soon.
        '''
        self._manager = _ProcessManager()
        self._name = name

    def __enter__(self):
        self._manager.__enter__()
        if self._name:
            self._manager._process.name = self._name
        return self

    def __exit__(self, *args):
        self._manager.__exit__(*args)

    def __del__(self):
        try:
            self.__exit__(None, None, None)
        except Exception:
            traceback.print_exc()

    @deprecated(
        deprecated_in='0.13.1',
        removed_in='0.13.5',
        details="Use context manager instead.",
    )
    def start(self):
        # :meth:`start` and :meth:`shutdown` are provided mainly for
        # compatibility. It's recommended to use the context manager.
        self.__enter__()

    @deprecated(
        deprecated_in='0.13.1',
        removed_in='0.13.5',
        details="Use context manager instead.",
    )
    def shutdown(self):
        self.__exit__()

    def __getattr__(self, name):
        # The main method names are the names of the classes that have been reigstered.
        if name in self._registry:
            return getattr(self._manager, name)
        raise AttributeError(
            f"'{self.__class__.__name__}' object has no attribute '{name}'"
        )


ServerProcess.register(
    'hosted', callable=None, proxytype=HostedProxy, create_method=False
)


# Register most commonly used classes.
# I don't make ``ServerProcess`` subclass ``SyncManger`` because I don't want to
# expose the full API of ``SyncManager`` or ``BaseManager``.
# If you need more that's not registered here, just register in your own code.

ServerProcess.register('Queue', queue.Queue)
ServerProcess.register(
    'Event', threading.Event, proxytype=multiprocessing.managers.EventProxy
)
ServerProcess.register(
    'Lock', threading.Lock, proxytype=multiprocessing.managers.AcquirerProxy
)
ServerProcess.register(
    'RLock', threading.RLock, proxytype=multiprocessing.managers.AcquirerProxy
)
ServerProcess.register(
    'Semaphore', threading.Semaphore, proxytype=multiprocessing.managers.AcquirerProxy
)
ServerProcess.register(
    'Condition', threading.Condition, proxytype=multiprocessing.managers.ConditionProxy
)
ServerProcess.register(
    'list', threading.Condition, proxytype=multiprocessing.managers.ListProxy
)
ServerProcess.register(
    'dict', threading.Condition, proxytype=multiprocessing.managers.DictProxy
)


try:
    from multiprocessing.shared_memory import SharedMemory
except ImportError:
    pass
else:

    class MemoryBlock:
        '''
        This class is used within the "server process" of a ``ServerProcess`` to
        create and track shared memory blocks.

        The design of this class is largely dictated by the need of its corresponding "proxy" class.
        '''

        _blocks_ = set()

        def __init__(self, size: int):
            self._mem = SharedMemory(create=True, size=size)
            self.__class__._blocks_.add(self._mem.name)

        def name(self):
            return self._mem.name

        def size(self):
            # This could be slightly larger than the ``size`` passed in to :meth:`__init__`.
            return self._mem.size

        @property
        def buf(self):
            return self._mem.buf

        def list_memory_blocks(self):
            '''
            Return a list of the names of the shared memory blocks
            created and tracked by this class.
            This list includes the memory block created by the current
            ``MemoryBlock`` instance; in other words, the list
            includes ``self.name``.
            '''
            return list(self._blocks_)

        def __del__(self):
            '''
            The garbage collection of this object happens when its refcount
            reaches 0. Unless this object is "attached" to anything within
            the server process, it is garbage collected once all references to its
            corresponding proxy object (outside of the server process) have been
            removed.

            The current object creates a shared memory block and references it
            by a ``SharedMemory`` object. In addition, each proxy object creates
            a ``SharedMemory`` object pointing to the same memory block for
            reading/writing.
            According to the class ``multiprocessing.shared_memory.SharedMemory``,
            a ``SharedMemory`` object calls ``.close`` upon its garbage collection,
            hence when all the proxy objects goes away, all the ``SharedMemory`` objects
            they have created have called ``.close``.
            At that point, ``self._mem``, residing in the server process, is the only
            reference to the shared memory block, and it is safe to "destroy" the memory block.

            Therefore, this ``__del__`` destroys that memory block.
            '''
            name = self._mem.name
            self._mem.close()
            self._mem.unlink()
            self.__class__._blocks_.remove(name)

        def __repr__(self):
            return f"<{self.__class__.__name__} {self.name()}, size {self.size()}>"

    class MemoryBlockProxy(PickleThroughProxy):
        _exposed_ = ('list_memory_blocks', 'name')

        def __init__(self, *args, incref=True, manager=None, **kwargs):
            super().__init__(*args, incref=incref, manager=manager, **kwargs)
            self._name = None
            self._size = None
            self._mem = None

            if incref:
                # Since ``incref`` is ``True``,
                # ``self`` is being constructed out of the return of either
                # ``ServerProcess.MemoryBlock`` or a method of some proxy (
                # for some object running within a server process).
                # ``self`` is not being constructed due to unpickling.
                #
                # We only place such objects under this tracking, and don't place
                # unpickled objects (in other processes) under this tracking, because
                # those objects in other processes should explicitly reach end of life.
                if self._manager is not None:
                    self._manager._memoryblock_proxies[self._id] = self

        def __reduce__(self):
            func, args = super().__reduce__()
            return _rebuild_memory_block_proxy, (
                func,
                args,
                self._name,
                self._size,
                self._mem,
            )

        @property
        def name(self):
            '''
            Return the name of the ``SharedMemory`` object.
            '''
            if self._name is None:
                self._name = self._callmethod('name')
            return self._name

        @property
        def size(self) -> int:
            '''
            Return size of the memory block in bytes.
            '''
            if self._mem is None:
                self._mem = SharedMemory(name=self.name, create=False)
            return self._mem.size

        @property
        def buf(self) -> memoryview:
            '''
            Return a ``memoryview`` into the context of the memory block.
            '''
            if self._mem is None:
                self._mem = SharedMemory(name=self.name, create=False)
            return self._mem.buf

        def list_memory_blocks(self) -> list[str]:
            '''
            Return names of shared memory blocks being
            tracked by the ``ServerProcess`` that "ownes" the current
            proxy object.
            '''
            return self._callmethod('list_memory_blocks')

        def __str__(self):
            return f"<{self.__class__.__name__} '{self.name}' at {self._id}, size {self.size}>"

    def _rebuild_memory_block_proxy(func, args, name, size, mem):
        obj = func(*args)
        obj._name, obj._size, obj._mem = name, size, mem
        return obj

    ServerProcess.register('MemoryBlock', MemoryBlock, proxytype=MemoryBlockProxy)

    def list_memory_blocks(self):
        '''
        List names of MemoryBlock objects being tracked.
        '''
        m = self.MemoryBlock(1)
        blocks = m.list_memory_blocks()
        blocks.remove(m.name)
        return blocks

    ServerProcess.list_memory_blocks = list_memory_blocks
