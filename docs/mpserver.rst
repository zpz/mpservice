===================================
Flexible service using ``mpserver``
===================================

.. automodule:: mpservice.mpserver
    :no-members:
    :no-special-members:


Example
=======

Let's make up an interesting problem that involves several expensive steps that demand a lot of computing power.
We decided to use `multiprocessing`_ to speed things up.
First, define the few operations that will take place in separate processes::

    from time import sleep
    from mpservice.mpserver import Worker, make_worker, ProcessServlet, ThreadServlet, SequentialServlet, EnsembleServlet


    class GetHead(Worker):
        def call(self, x):
            sleep(0.01)
            return x[0]


    class GetTail(Worker):
        def call(self, x):
            sleep(0.011)
            return x[-1]


    class GetLen(Worker):
        def call(self, x):
            sleep(0.012)
            return len(x)


    class Solute(Worker):
        def call(self, x):
            return f"Hello, {x}!"

This is what they do:

- Given a sequence, ``GetHead`` returns the first element, ``GetTail`` returns the last element, ``GetLen`` returns the length of the sequence.
- Given something, ``Solute`` returns a welcome message.


Second, specify how these operations work together.
In other words, define a *flow* that any input value will go through.

::

    servlet = SequentialServlet(
        EnsembleServlet(
            ProcessServlet(GetHead, cpus=[0]),
            ProcessServlet(GetTail, cpus=[0, 1]),
            ProcessServlet(GetLen, cpus=[1]),
            ),
        ThreadServlet(make_worker(lambda x: (x[0] + x[1]) * x[2])),
        ProcessServlet(Solute),
        )

In words, given input ``x``, it goes through such a flow of operations:

1. On the high level, the flow is a "sequence" of three components.
   The first is an :class:`EnsembleServlet`, the second is a :class:`ThreadServlet`,
   and the third is a :class:`ProcessServlet`.

   The input ``x`` enters the EnsembleServlet, the output of which
   enters the ThreadServlet, the output of which enters the ProcessServlet,
   the output of which is the final result.
   
2. The :class:`EnsembleServlet` arranges ``GetHead``, ``GetTail``, and ``GetLen``
   to run in separate processes because they are compute-intensive.

   For precise control, we have specified which CPUs each component should run on.
   This also shows how many processes each component creates and runs in. For example,
   ``GetTail`` uses two processes running on the first and the second CPU, respectively.

   Being an *ensemble* servlet, each of its components ``GetHead``, ``GetTail``, and ``GetLen``
   will get the input ``x`` and produce its output.
   The three outputs will form a list (respecting the order of three operators), which
   is the output of the :class:`EnsembleServlet`.
   In other words, the output is the list ``[first_elem, last_elem, len]``.

3. On the output of the EnsembleServlet, we apply a simple function, which adds up
   the first two elements and multiply the sum by the third element.
   This is a light-weight operation, so we do it in a thread instead of a process.

   We could have defined a subclass of :class:`Worker` and then wrap it in a :class:`ThreadServlet`.
   For demonstration, we chose to use :func:`make_worker` to dynamically define and return such
   a class.

4. The output of the ThreadServlet becomes the input to ``Solute``.
   This is again a heavy computation, hence we run it in another process.
   This process can use any CPU because we did not provide the ``cpus`` argument.

   The output of ``Solute`` is the final result of the :class:`SequentialServlet`.

All the :class:`Worker`, :class:`ProcessServlet`, :class:`ThreadServlet`, :class:`EnsembleServlet`,
and :class:`SequentialServlet` (and :class:`SwitchServlet` not shown above) are just "spec" of the flow. 
They do not run by themselves.
There needs to be a "driver" that starts them, connects them to the "outside world", 
passes input to them, and collects output from them.
That's the job of a :class:`Server`.
To be precise, a Server does not interact with all of "them";
it *directly* interacts with only one :class:`Servlet`; in the example above, that's
the :class:`SequentialServlet`::


    from mpservice.mpserver import Server

    server = Server(servlet)

All this code is in a script named "test.py".
Here's the remaining content of the script::

    def main():

        with server:
            x = 'world'
            y = server.call(x)
            print(y)
            
            x = [1, 2, 3, 4]
            y = server.call(x)
            print(y)


    if __name__ == '__main__':
        main()
        
Before continuing, can you figure out what will be printed?


Workers
=======

.. autoclass:: mpservice.mpserver._SimpleProcessQueue

.. autoclass:: mpservice.mpserver._SimpleThreadQueue

.. autoclass:: mpservice.mpserver.Worker

.. autofunction:: mpservice.mpserver.make_worker

.. autodata:: mpservice.mpserver.PassThrough


Servlets
========

A :class:`Servlet` manages the execution of one or more :class:`Worker`.
We make a distinction between "simple" servlets, including :class:`ProcessServlet` and :class:`ThreadServlet`,
and "compound" servlets, including :class:`SequentialServlet`, :class:`EnsembleServlet`,
and :class:`SwitchServlet`.

A simple servlet arranges to execute one :class:`Worker` in requested number of processes (or threads).
Optionally, it can specify exactly which CPU(s) each worker process should use.
Each input item is passed to and processed by exactly one of the processes (or threads).

A compound servlet arranges to execute multiple :class:`Servlet`\s as a sequence or an ensemble.
In addition, there is :class:`SwitchServlet` that acts as a "switch"
in front of a set of Servlets.
There's a flavor of recursion in this definition in that a member servlet can very well be
a compound servlet.

Great power comes from this recursive definition.
In principle, we can freely compose and nest the :class:`Servlet` types.
For example, suppose `W1`, `W2`,..., are :class:`Worker` subclasses,
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



.. autoclass:: mpservice.mpserver.Servlet

.. autoclass:: mpservice.mpserver.ProcessServlet

.. autoclass:: mpservice.mpserver.ThreadServlet

.. autoclass:: mpservice.mpserver.SequentialServlet

.. autoclass:: mpservice.mpserver.EnsembleServlet

.. autoclass:: mpservice.mpserver.SwitchServlet


Server
======


The "interfacing" and "scheduling" code of :class:`Server`
runs in the "main process".
Two usage patterns are supported, namely making individual
calls to the service to get individual results, or flowing
a (potentially unlimited) stream of data through the service
to get a stream of results.

A typical setup looks like this::

    server = Server(servlet)
    with server:
        z = server.call('abc')

        for x, y in server.stream(data, return_x=True):
            print(x, y)


Code in the "workers" should raise exceptions as it normally does, without handling them,
if it considers the situation to be non-recoverable, e.g. input is of wrong type.
The exceptions will be funneled through the pipelines and raised to the end-user
with useful traceback info.

The user's main work is implementing the operations in the "workers".
Another task (of some trial and error) by the user is experimenting with
CPU allocations among workers to achieve best performance.

:class:`Server` has an async counterpart named :class:`AsyncServer`.

.. autoexception:: mpservice.mpserver.ServerBacklogFull

.. autoclass:: mpservice.mpserver.Server

.. autoclass:: mpservice.mpserver.AsyncServer


.. **Reference**: `Service Batching from Scratch, Again <https://zpz.github.io/blog/batched-service-redesign/>`_.
.. This article describes roughly version 0.7.2. Things have evolved a lot.

**Answer to the Example challenge**: when we run the script, this is the printout::

    $ python test.py 
    Hello, wdwdwdwdwd!
    Hello, 20!
