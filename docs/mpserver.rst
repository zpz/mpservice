========
mpserver
========

.. automodule:: mpservice.mpserver
    :no-members:
    :no-undoc-members:
    :no-special-members:


Workers
=======

.. autoclass:: mpservice.mpserver.FastQueue

.. autoclass:: mpservice.mpserver.SimpleQueue

.. autoclass:: mpservice.mpserver.Worker

.. autoclass:: mpservice.mpserver.ProcessWorker

.. autoclass:: mpservice.mpserver.ThreadWorker

.. autofunction:: mpservice.mpserver.make_threadworker

.. autodata:: mpservice.mpserver.PassThrough


Servlets
========

A "servlet" manages the execution of a ``Worker`` or ``Worker``\s.
We make a distinction between "simple" servlets, including ``ProcessServlet`` and ``ThreadServlet``,
and "compound" servlets, including ``SequentialServlet`` and ``EnsembleServlet``.

A simple servlet arranges to execute one ``Worker`` in requested number of processes (threads).
Optionally, it can specify exactly which CPU(s) each worker process uses.
Each input item is passed to and processed by exactly one worker process (thread).

A compound servlet arranges to execute multiple ``Servlet``\s as a sequence or an ensemble.
There's a flavor of recursion in this definition in that a member servlet can very well be
a compound servlet.

Great power comes from this recursive definition.
In principle, we can freely compose and nest the ``Servlet`` types.
For example, suppose `W1`, `W2`,..., are `Worker` subclasses,
then we may design such a workflow,

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

In precise language, the type ``Servlet`` is an alias to
``Union[ProcessServlet, ThreadServlet, SequentialServlet, EnsembleServlet]``.


.. autoclass:: mpservice.mpserver.CpuAffinity

.. autoclass:: mpservice.mpserver.ProcessServlet

.. autoclass:: mpservice.mpserver.ThreadServlet

.. autoclass:: mpservice.mpserver.SequentialServlet

.. autoclass:: mpservice.mpserver.EnsembleServlet

.. autodata:: mpservice.mpserver.Sequential

.. autodata:: mpservice.mpserver.Ensemble


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


Reference: `Service Batching from Scratch, Again <https://zpz.github.io/blog/batched-service-redesign/>`_.
This article describes roughly version 0.7.2. Things have changed a lot.
