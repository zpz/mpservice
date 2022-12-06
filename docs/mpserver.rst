========
mpserver
========

``mpservice.mpserver`` provides classes that use ``multiprocessing`` to perform CPU-bound operations
taking advantage of all the CPUs (i.e. cores) on the machine.

Using threads to perform IO-bound operations is also supported, although that is not the focus.

There are four levels of constructs.

1. On the lowest level is ``Worker``. This defines operations on a single input item
   or a batch of items in usual sync code. This is supposed to run in its own process
   and use that single process only.

2. A ``Servlet`` manages a ``Worker``, specifying how many processes are to be created,
   each executing one instance of said ``Worker`` independently. Optionally, it can specify exactly
   which CPU(s) each worker process uses.

3. Servlets can compose a ``SequentialServlet`` or an ``EnsembleServlet``. In a ``SequentialServlet``,
   one servlet's output becomes the next servlet's input.
   In an ``EnsembleServlet``, each input item is processed by all the constituent servlets, and their
   results are collected and combined. Interestingly, both ``SequentialServlet`` and ``EnsembleServlet``
   are also ``Servlet``\s, hence they can participate in composing other ``SequentialServlet``\s
   and ``EnsembleServlet``\s.

4. On the top level is ``Server``. A ``Server``
   handles interfacing with the outside world, while passing the "real work" to
   a ``Servlet`` and relays the latter's result to the requester.

Reference: `Service Batching from Scratch, Again <https://zpz.github.io/blog/batched-service-redesign/>`_.
This article describes roughly version 0.7.2.


Workers
=======

.. autoclass:: mpservice.mpserver.Worker

.. autoclass:: mpservice.mpserver.ProcessWorker


.. autoclass:: mpservice.mpserver.ThreadWorker


.. autofunction:: mpservice.mpserver.make_threadworker


.. autodata:: mpservice.mpserver.PassThrough


Servlets
========

A "servlet" manages the execution of a ``Worker``.
To be precise, a servlet manages one or more instances of one ``Worker`` subclass.
The servlet runs in the "main" process, whereas the ``Worker`` instances run in other processes or threads.

We make a distinction between "simple" servlets, including ``ProcessServlet`` and ``ThreadServlet``,
and "compound" servlets, including ``SequentialServlet`` and ``EnsembleServlet``.

In the case of a simple servlet, each input item is passed to and processed by
exactly one worker process or thread.

.. autoclass:: mpservice.mpserver.CpuAffinity

.. autoclass:: mpservice.mpserver.ProcessServlet

.. autoclass:: mpservice.mpserver.ThreadServlet


Servlets can be composed in ``SequentialServlet``'s or ``EnsembleServlet``'s. In a ```SequentialServlet``,
the first servlet's output becomes the second servlet's input, and so on.
In an ``EnsembleServlet``, each input item is processed by all the servlets, and their
results are collected in a list.

Great power comes from the fact that both `SequentialServlet` and `EnsembleServlet`
also follow the `Servlet` API, hence both can be constituents of other compositions.
In principle, you can freely compose and nest them.
For example, suppose `W1`, `W2`,..., are `Worker` subclasses,
then you may design such a workflow,

::

    s = SequentialServlet(
            ProcessServlet(W1),
            EnsembleServlet(
                ThreadServlet(W2),
                SequentialServlet(ProcessServlet(W3), ThreadServlet(W4)),
                ),
            EnsembleServlet(
                Sequetial(ProcessServlet(W5), ProcessServlet(W6)),
                Sequetial(ProcessServlet(W7), ThreadServlet(W8), ProcessServlet(W9)),
                ),
        )

In sum, `ProcessServlet`, `ThreadServlet`, `SequentialServlet`, `EnsembleServlet` are collectively
referred to and used as `Servlet`.

.. autoclass:: mpservice.mpserver.SequentialServlet

.. autoclass:: mpservice.mpserver.EnsembleServlet

.. autoattribute:: mpservice.mpserver.Servlet


Server
======


The "interface" and "scheduling" code of `Server` runs in the "main process".
Two usage patterns are supported, namely making (concurrent) individual
calls to the service to get individual results, or flowing
a potentially unlimited stream of data through the service
to get a stream of results. The first usage supports a sync API and an async API.


On the top level is `Server`. Pass a `Servlet`, or `SequentialServlet` or `EnsembleServlet`
into a `Server`, which handles scheduling as well as interfacing with the outside
world::

    server = Server(s)
    with server:
        y = server.call(38)
        z = await server.async_call('abc')

        for x, y in server.stream(data, return_x=True):
            print(x, y)


Code in the "workers" should raise exceptions as it normally does, without handling them,
if it considers the situation to be non-recoverable, e.g. input is of wrong type.
The exceptions will be funneled through the pipelines and raised to the end-user
with useful traceback info.


The user's main work is implementing the operations in the "workers".
Another task (of some trial and error) by the user is experimenting with
CPU allocations among workers to achieve best performance.

.. autoclass:: mpservice.mpserver.Server