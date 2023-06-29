'''
The :class:`Manager` in ``multiprocessing`` provides a mechanism to interactively
communicate with a "server" that runs in another process.

The keyword is "interactively"---just like a client to a HTTP server, the client process
can make a request to a "proxy" that is returned by the "manager" and wait for the response.
There may be multiple types of data to be requested, just like calling multiple "end-points" 
of a HTTP server.

The other inter-process data sharing facilities, e.g. a :class:`Queue`, work more in a passive fashion.
The client code checks the next data element in the queue, and obtaines whatever is there.

The interactivity is achieved by a "proxy": an object is created and *hosted* in a "server process";
the hosted object is represented by a "proxy", which is passed to and used in other processes
to communicate with the hosted object. The backbone of the communication is ``multiprocessing.connection.Connection``
with its ``send`` and ``recv`` methods.

The module ``mpservice.multiprocessing.server_process`` provides :class:`ServerProcess`,
with mainly two enhancements to the standard ``Manager``:
one concerns "shared memory",
and the other concerns returning a mix of hosted and non-hosted data from a proxy.


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
object have been garbage collected.
There are two special considerations to be aware of. First, in the "original" process
in the code block above, if ``mem`` is not deleted, it will be when ``server.__exit__``
executes.

Regarding the second consideration, suppose ``mem`` is placed in a Queue to be picked up
in another process; further suppose that the reference ``mem`` is gone after it has been placed
in the Queue. This happens, for example, ``mem` is a temporary reference in a loop.
In the standard behavior, the pickled data does not contain a reference to the ``MemoryBlockProxy``,
therefore there may very well be a moment when there is no reference to the shared memory outside
of the server process. However, the intention is clearly to use the shared memory once
the pickled ``MemoryBlockProxy`` is taken out of the Queue in another process.
For this reason, ``MemoryBlockProxy`` does some trick to count as a reference in pickled form.

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

Now, the first element of ``lst`` is a dict proxy. You may pass ``lst`` to a nother process
and manipulate its first element; the changes are reflected in the dict that resides in the server process.

Return proxies from methods of hosted objects
=============================================

By default, when you call a method of a proxy, the returned value is plain old data passed from
the server process to the caller in the client process via pickling.
Once received, that value has no relation to the server process.

The standard ``Manager`` allows a method of a registered class to return a proxy
by instructions via the parameter ``method_to_typeid`` of :meth:`~mpservice.multiprocessing.server_process.ServerProcess.register`.
However, the set up can be inconvenient, and the capability is limited.

:class:`~mpservice.multiprocessing.server_process.ServerProcess` supports returning
a mix of proxy and non-proxy data elements in convenient and powerful ways.
Suppose we have a class ``Worker``, which we will run in a server process;
further, a method of ``Worker`` will return a dict that are plain data in some parts and
proxies in some others. We can do something like this::

    from mpservice.multiprocessing.server_process import hosted, MemoryBlock, ServerProcess

    class Worker:
        def make_data(self):
            return {
                'name': 'tom',
                'hobbies': ['soccor', 'swim'],
                'mem': hosted(MemoryBlock(100)),
                'jobs': ('IBM', 'APPLE', {'UK': hosted(['ARM', 'Shell'])}),
            }

        def get_memory(self, size):
            return hosted(MemoryBlock(size))

    ServerProcess.register('Worker', Worker, method_to_typeid={'make_data': 'hosted'})

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

Any value that is wrapped by ``hosted`` must be a registered class, e.g. the return of ``Worker.get_memory``.
When the entire return value is not wrapped by ``hosted``, the value must be a tuple, list, or dict.
Any element of a tuple or a list, or a value (not a key) in a dict, can be ``hosted``,
as long as that value is a ``register``\ed class. This goes recursively to any depth.
The search will stop at any value that is not a tuple, list, or dict; such a value will simply be pickled.
For example,

::

    data = {
        'a': [1, 2, [3, hosted(MemoryBlock)], ClassA()],
        'b': {'x': ['sfo', ('y', hosted([1, 2]))]},
    }
    return data

In this example, ``data['a'][2]`` is a list, hence we check each of its elements,
and find ``data['a'][2][1]`` to be a ``hosted`` object, but ``data['a'][2][2]``
is not a tuple, list, or dict, and not ``hosted``, hence it is returned as is.
Similarly, ``data['b']['x'][0]`` is a string and returned as is.
On the other hand, ``data['b']['x'][1]`` is a tuple, hence each of its elements is examined,
and it is found that it contains a plain string and a ``hosted`` list.
'''
from __future__ import annotations

import multiprocessing
import multiprocessing.connection
import multiprocessing.context
import multiprocessing.managers
import multiprocessing.pool
import multiprocessing.queues
import sys
import threading
import traceback
import warnings
import weakref
from multiprocessing import util
from multiprocessing.managers import (
    BaseProxy,
    Token,
    convert_to_error,
    dispatch,
)
from traceback import format_exc

from .context import MP_SPAWN_CTX
from .util import CpuAffinity

__all__ = [
    'ServerProcess',
    'MemoryBlock',
    'MemoryBlockProxy',
    'hosted',
]


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
            # In a few cases I saw ``BrokenPipeError: [Errno 32] Broken pipe``.
            # A workaround is described here:
            # https://stackoverflow.com/q/3649458/6178706

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
                Client=self._Client,
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


def HostedProxy(*, address, serializer, manager, Client, authkey=None, data):
    def make_hosted(value: hosted):
        ident, exposed = value.value
        typeid = value.typeid

        tok = Token(typeid, address, ident)
        proxytype = manager._registry[typeid][-1]
        proxy = proxytype(
            tok, serializer, manager=manager, authkey=authkey, exposed=exposed
        )
        conn = Client(address, authkey=authkey)
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
    # My fix is labeled "FIX".
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

                # FIX:
                # If no more request is coming, then `function` and `res` will stay around.
                # If `function` is a instance method of `obj`, then it carries a reference to `obj`.
                # Also, `res` is a refernce to the object that has been tracked in `self.id_to_obj`
                # and "returned" to the requester.
                # The extra reference to `obj` and `res` lingering here have no use, yet can cause
                # sutble bugs in applications that make use of their ref counts, such as ``MemoryBlock``..
                del function
                del obj
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

    def _create_hosted(self, c, data):
        # Construct the return value of a method that has been registered in
        # `method_to_typeid` as 'hosted'.

        def convert_hosted(value):
            value, typeid = value.value, value.typeid
            typeid_nocall = f"{typeid}-nocall"
            if typeid_nocall not in self.registry:
                self.registry[typeid_nocall] = (
                    None,
                    *self.registry[typeid][1:],
                )  # replace `callable` by `None`

            ident, exposed = self.create(c, typeid_nocall, value)
            # By now `value` has been referenced in ``self._id_to_obj``.
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


class ServerProcess(multiprocessing.managers.SyncManager):
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

    # Restrictions:
    # ``ServerProcess`` instances can't be pickled, hence can't be passed to other processes.
    # As a result, if you pass a proxy object to another process and call its methods in the other process,
    # the methods can't be ones that return proxy objects, because proxy objects would need a reference to
    # the "manager", while a proxy object does not carry its "manager" attribute during pickling.

    _Server = _ProcessServer

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
        super().register(typeid=typeid, callable=callable, **kwargs)

    def __init__(
        self,
        *,
        cpu: int | list[int] | None = None,
        name: str | None = None,
        **kwargs,
    ):
        '''
        Parameters
        ----------
        name
            Name of the server process. If ``None``, a default name will be created.
        cpu
            Specify CPU pinning for the server process.
        '''
        super().__init__(ctx=MP_SPAWN_CTX, **kwargs)
        self._name = name
        self._cpu = cpu

    def __enter__(self):
        super().__enter__()
        if self._name:
            self._process.name = self._name

        if self._cpu is not None:
            CpuAffinity(self._cpu).set(pid=self._process.pid)

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


ServerProcess.register(
    'hosted', callable=None, proxytype=HostedProxy, create_method=False
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
        '''

        _blocks_ = set()

        def __init__(self, size: int):
            assert size > 0
            self._mem = SharedMemory(create=True, size=size)
            self.__class__._blocks_.add(self._mem.name)

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
            '''
            Return a list of the names of the shared memory blocks
            created and tracked by this class.
            This list includes the memory block created by the current
            ``MemoryBlock`` instance; in other words, the list
            includes ``self.name``.

            This is for use by ``MemoryBlockProxy``.
            This is mainly for debugging purposes.
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
        _exposed_ = ('_list_memory_blocks', '_name')

        def __init__(self, *args, incref=True, manager=None, **kwargs):
            super().__init__(*args, incref=incref, manager=manager, **kwargs)
            self._name = None
            self._size = None
            self._mem = None

            if incref:
                # Since ``incref`` is ``True``,
                # ``self`` is being constructed out of the return of either
                # ``ServerProcess.MemoryBlock`` or a method of some proxy (
                # and the method's return value contains a ``MemoryBlock``).
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
                self._name = self._callmethod('_name')
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

        def _list_memory_blocks(self) -> list[str]:
            '''
            Return names of shared memory blocks being
            tracked by the ``ServerProcess`` that "ownes" the current
            proxy object.
            '''
            return self._callmethod('_list_memory_blocks')

        def __str__(self):
            return f"<{self.__class__.__name__} '{self.name}' at {self._id}, size {self.size}>"

    def _rebuild_memory_block_proxy(func, args, name, size, mem):
        obj = func(*args)
        obj._name, obj._size, obj._mem = name, size, mem
        return obj

    ServerProcess.register('MemoryBlock', MemoryBlock, proxytype=MemoryBlockProxy)

    def list_memory_blocks(self):
        '''
        List names of MemoryBlock objects being tracked in the server process.
        '''
        m = self.MemoryBlock(1)
        blocks = m._list_memory_blocks()
        blocks.remove(m.name)
        return blocks

    ServerProcess.list_memory_blocks = list_memory_blocks
