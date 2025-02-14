# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).


## [0.16.8] - 2025-01-15

- New class `mpservice.queue.ResponsiveQueue`.
- `IterableQueue` was moved from `mpservice.streamer` into `mpservice.queue`; its implementation makes use of `ResponsiveQueue`.


## [0.16.7] - 2024-12-18

- Make `StopRequested` inherit from `BaseException` instead of `Exception`.
- Finetune printout of exception info in process and threads.


## [0.16.6] - 2024-12-17

- Removed module `mpservice.rate_limiter` (moved to package `cloudly`).
- Bug fix regarding `mpservice.mpserver.Worker.preprocess`--now Exception objects are not sent to `preprocess`.


## [0.16.5] - 2024-12-14

- `mpservice.streamer.Parmapper` gets new parameter `preprocessor`, which is passed on to `fifo_stream`.
- `ProcessRunner` and `ProcessRunee` are moved from `mpservice.streamer` into `mpservice.multiprocessing.runner`.
- `RateLimiter` and `AsyncRateLimiter` are moved from `mpservice.streamer` into `mpservice.rate_limiter`.


## [0.16.4] - 2024-12-14

- Classes `Tailor` and `AsyncTailor` were renamed to `Tailer` and `AsyncTailer`.
- `mpservice.streamer.{fifo_stream, async_fifo_stream}` get new parameter `preprocessor`.
- `mpservice.mpserver.{Server, AsyncServer}.stream` parameter `preprocess` was renamed to `preprocessor`,
  and they are now simply passed on to `fifo_stream` or `async_fifo_stream`.
- New class `mpservice.streamer.AsyncRateLimiter`.


## [0.16.3] - 2024-11-20

- New class `mpservice.streamer.RateLimiter`.


## [0.16.2] - 2024-11-17

- Further separate "async_streamer" from "streamer" because I doubt "async_streamer" has a strong use case.
- Update documentation for "streamer".
- Requires Python 3.10.


## [0.16.1] - 2024-11-10

- Removed async capabilities from `Stream`.
- New class `AsyncStream`, providing the async capabilities of the previous `Stream`.
  This new class is not available in `mpservice.streamer`; it's only in `mpservice._streamer` because I'm still not sure
  whether async streaming is useful.


## [0.16.0] - 2024-10-27

- `IterableQueue.put` gets keyword arg `timeout`.
- `IterableQueue.get` becomes `_get`.
- New functions `mpservice.streamer.{fifo_stream, async_fifo_stream}`.
- Simplified implementations of classes `mpservice._streamer.*Parmapper*` and methods `mpservice.mpserver.{Server, AsyncServer}.stream` using these functions.
- `mpservice.streamer.Stream.parmapper` parameter `num_workers` was renamed `concurrency` because the meaning of the latter is more correct in async contexts.
- `mpservice.mpserver.Server.stream` and `mpservice.mpserver.AsyncServer.stream` lost parameter `to_stop`.


## [0.15.9] - 2024-09-28


- Bug fix in `IterableQueue` when using a multiprocessing SimpleQueue.
- Refinements and enhancements to `IterableQueue`.
- `mpservice.mpserver.{Server, AsyncServer}.stream` gets a new parameter `preprocess`.
- New classes `ProcessRunner` and `ProcessRunnee` in `mpservice._streamer`.


## [0.15.8] - 2024-0921

- Added new class `mpservice.streamer.IterableQueue`.
- The directory structure under `mpservice.multiprocessing` better mirrors that of the standard lib.
- `Parmapper` and `Server.stream` get new parameter `to_stop` to allow the stream to be stopped by external forces.


## [0.15.7] - 2024-09-15

- Finetune `mpservice.multiprocessing.SpawnProcess` regarding termination, background threads, and error handling.
- `mpservice.multiprocessing.Queue` gets property `maxsize`, making it aligned with `queue.Queue`, which has attribute `maxsize`.


## [0.15.6] - 2024-06-02

- Finetune `mpservice.multiprocessing.server_process`: 
    - support pickling of `ServerProcess` objects.
    - buf fix regarding custom authkey to `ServerProcess`


## [0.15.5] - 2024-05-28

- Minor improvement to `mpservice.multiprocessing.server_process`.
- Upgraded `uvicorn` to 0.30.0; revised `mpservice._http` accordingly.


## [0.15.4] - 2024-05-25

- Refactored and enhanced `mpservice.multiprocessing.server_process`.


## [0.15.3] - 2024-05-17

- `mpservice.multiprocessing.server_process.ServerProcess` enhancement: if a proxy function raises exception in the server process, a `mpservice.multiprocessing.remote_exception.RemoteException` is returned to the request and then raised to the user, hence the user (outside of the server process) gets more useful traceback info.
- `mpservice.multiprocessing.server_process` is heavily refactored for enhancements and simplifications.


## [0.15.2] - 2024-05-05

- `mpservice.mpserver.Worker` gets new method `stream` for some special use cases.


## [0.15.1] - 2024-05-03

- `mpservice.mpserver.Worker.__init__` loses parameter `batch_size_log_cadence` and gets new parameter `cpu_affinity`,
  which also becomes an attribute of the worker instance.


## [0.15.0] - 2024-04-15

- Removed some deprecated code.
- `mpservice.mpserver.Worker` gets new method `cleanup`.


## [0.14.9] - 2024-03-22

- Bug fix in `mpservice.multiprocessing.Pool`.
- Finetune about shutdown of the background threads supporting `mpservice.multiprocessing.SpawnProcess`.


## [0.14.8] - 2024-03-17

- Minor tweak on `mpserver` detail.

  It happens in one application that some requests get stuck in the server "backlog", whereas by design
  this should never happen (in reasonable time every input unit flows through the system and comes out of
  the output queue, at which time clears its bookkeeping entry in the backlog). The tweak hopefully helps
  debugging this situation.


## [0.14.7] - 2024-03-08

- Bug fix in ``ServerBacklogFull`` class definition.


## [0.14.6] - 2024-03-02

- Export ``isiterable`` and ``isasynciterable`` in ``mpservice.streamer``.
- ``mpservice.threading.Thread.raise_exc`` was renamed to ``throw``.
- Bug fix in ``mpservice.multiprocessing.SimpleQueue``.
- Bug fix and fine-tuning related to stopping `uvicorn` and `mpservice.mpserver` servers.


## [0.14.5] - 2024-01-18

- Customize ``Process`` and ``Thread`` classes in ``mpservice.mpserver`` to log unhandled BaseException, if any.


## [0.14.4] - 2023-11-11

- Bug fix in ``mpservice.experimental.streamer.EagerBatcher``.
- `EagerBatcher` is moved out of `experimental` into `mpservice.streamer`.


## [0.14.3] - 2023-09-20

- Further finetune of importing paths of things, esp in ``mpservice.multiprocessing``.
- Some improvements to documentation.


## [0.14.2] - 2023-09-10

- Adjustment to the importing of things in ``mpservice.multiprocessing``. Some changes are breaking.
  ``mpservice.multiprocessing.context`` has become ``mpservice.multiprocessing._context``.
  ``mpservice.multiprocessing.util`` has been removed.


## [0.14.1] - 2023-09-04

- New http serving code in ``mpservice.http`` designed for multiple worker processes
  managed by ``uvicorn`` (as opposed to multiple worker processes managed by ``mpservice.mpserver``).
- Removed dependency on ``asgiref``.
- ``Stream.groupby`` now behaves like the standard ``itertools.groupby`` in that the subgroups it yields are generators rather than lists.
- ``Stream.{map, filter}`` now can take async ``func`` arguments.
- New method ``Stream.shuffle``.


## [0.14.0] - 2023-08-22


- Bug fix.
- Remove previously deprecated code.


## [0.13.9] - 2023-08-20

- Deprecated ``mpservice.multiprocessing.util.CpuAffinity``.
- Refinement to ``get_shared_thread_pool`` and ``get_shared_process_pool``.
- Removed the constant ``MAX_THREADS`` from ``mpservice.threading``.


## [0.13.8] - 2023-08-15

- Removed "IterableQueue" and variants, as well as ``StreamServer``, from ``mpservice.experimental``.
- Revised ``mpservice.experimental.streamer.EagerBatcher`` to not use ``IterableQueue``. This makes it simpler and more usable.


## [0.13.7] - 2023-08-07

- Finetune exit status handling of ``SpawnProcess``.
- ``mpservice.mpserver.StreamServer`` was moved into ``mpservice.experimental.mpserver``.
- Parameter ``main_cpu`` to a few functions in ``mpservice.mpserver`` was removed.
- Default value of parameter ``timeout`` to ``mpservice.mpserver.Server.stream`` and ``mpservice.mpserver.AsyncServer.stream``
  were changed from 600 to 3600, so that user almost never need to specify this parameter.
- in ``mpservice.mpserver``, if a Worker fails to start, the exception will propagate through ``Servlet.start`` into ``Server.__enter__``.


## [0.13.6] - 2023-07-18

- ``mpservice.mpserver.{ProcessServlet, ThreadServlet}`` get new parameter ``worker_name``.
- Added trivial subclasses of the standard ``queue.{Queue, SimpleQueue}``, ``asyncio.Queue``, ``multiprocessing.queues.{Queue, SimpleQueue}``; the subclasses are generic, hence can take annotations for the type of the elements contained in the queues.
- New subpackage `mpservice.experimental`; moved all the few types of ``IterableQueue`` as well as ``mpservice.streamer.EagerBatcher`` into it.
- Import some subpackages in ``__init__.py``.


## [0.13.5] - 2023-07-03

- New class ``mpservice.streamer.EagerBatcher``.
- New class ``mpservice.mpserver.StreamServer``. This intends to eventually replace and deprecate ``mpservice.mpserver.Server.stream``,
  but the current version has not shown speed benefits yet, despite being simpler.
- Speed improvements to ``mpserver`` and ``streamer``.
- ``mpservice._streamer.{IterableQueue, IterableProcessQueue, AsyncIterableQueue}`` were moved into
  ``mpservice.queue``, ``mpservice.multiprocessing.queues``, ``mpservice.asyncio`` respectively and all renamed to ``IterableQueue``.
- A few utility classes in ``mpservice._streamer`` are now exposed in ``mpservice.streamer``, including ``Batcher``, ``Unbatcher``, ``Buffer``.


## [0.13.4] - 2023-06-18

- ``IterableQueue``, ``IterableProcessQueue``, ``AsyncIterableQueue`` became generic with an element type parameter.
- Parameter ``cpus`` to ``ProcessServlet.__init__`` now accepts an int, indicating number of unpinned processes.
- Use more reliable way to infer whether a ``Stream`` object is sync or async iterable.
- Fixed issues in documentation generation.


## [0.13.3] - 2023-06-11

- New methods ``mpservice.threading.Thread.{raise_exc, terminate}``.
- New functions ``mpservice.threading.{wait, as_completed}``.
- Enhancements to ``mpservice.multiprocessing.server_process`` esp regarding "hosted" data.
- Do not raise exception if a ``mpservice.multiprocessing.context.SpawnProcess`` was terminated by ``.terminate()``.
  The previous behavior tends to raise exception when a ``ServerProcess`` shuts down.
- ``mpservice.mpserver.Worker`` adds support for ``preprocess``.
- Revised implementation of ``mpservice.multiprocessing.context.SpawnProcess`` to use a Future to support ``wait`` and ``as_completed``.
- New functions ``mpservice.multiprocessing.{wait, as_completed}``.
- Renamed ``mpservice._streamer.{IterQueue, IterProcessQueue, AsyncIterQueue}`` to ``{IterableQueue, IterableProcessQueue, AsyncIterableQueue}``.
- Finetune implementation of ``mpservice._stramer.{IterableQueue, IterableProcessQueue, AsyncIterableQueue}``.
- Made ``mpservice._streamer.Stream`` generic to allow type-annotating ``Stream[T]``.


## [0.13.2] - 2023-06-07

- Re-orged ``mpservice.multiprocessing`` into a sub-package as the module has grown considerably and may grow further.
- ``mpservice.mpserver.{Server, AsyncServer}`` bug related to "gather-output" and "notify" requesters.
- Enhancements to ``mpservice.mpserver.multiprocessing.ServerProcess`` (in progress).


## [0.13.1] - 2023-06-04

- Finetune to ``mpservice.multiprocessing.ServerProcess`` and its shared-memory facilities.
- Fix a bug in ``mpservice.mpserver.{Server, AsyncServer}`` related to input buffer.
- Finetune ``mpservice.mpserver.{Server, AsyncServer}`` internals.


## [0.13.0] - 2023-05-31

- Breaking changes to ``mpservice.mpserver.Server`` API: the class is split into two classes: ``Server`` and ``AsyncServer``.
- ``mpservice.mpserver.Server.call`` got new parameter ``backpressure`` (previously only the async call has this parameter).
- Finetuned waiting and sleeping logic in ``mpservice.mpserver.{Server, AsyncServer}``; use ``Condition`` to replace sleeping.
- Made sure (or confirmed) that ``mpservice.mpserver.Server.call`` and ``mpservice.mpserver.Server.stream` are thread-safe.
- ``mpservice.streamer.Stream.peek`` finetune of printing; got new parameter ``prefix`` and ``suffix``.
- Refinements to classes ``mpservice.streamer.{IterQueue, IterProcessQueue, AsyncIterQueue}``.
- Refinements to ``mpservice.multiprocessing.ServerProcess``: further diverge from the standard class.
- Initial support for "shared memory" in the class ``mpservice.multiprocessing.ServerProcess``.


## [0.12.9] - 2023-05-23

- New function ``mpservice.streamer.tee``.
- New class ``mpservice.streamer.IterProcessQueue``.
- Removed the "cancellation" "Event" mechanism in ``mpservice.mpserver.Server`` that was introduced in 0.12.7.
  There are two main reasons for the removal: (1) the ``mpservice.multiprocessing.manager.Event`` that is checked
  by every worker seems to have considerable overhead although I did not measure it; (2) there is
  difficulty in maintaining a reference to the ``Event`` object in the event of cancellation to
  ensure any worker that tries to access it will do so before it is gone in the manager process;
  this issue manifests as ``KeyError`` during unpickling when the object is being retrieved from
  a multiprocessing queue.


## [0.12.8] - 2023-05-17

- Removed ``mpservice.mpserver.{ProcessWorker, ThreadWorker}``; just use ``Worker``.
- Renamed ``mpservice.mpserver.make_threadworker`` to ``mpservice.mpserver.make_worker``.
- ``mpservice.mpserver.Server`` got new method ``async_stream``.
- New classes ``mpservice.streamer.IterQueue``, ``mpservice.streamer.AsyncIterQueue``.
- Minor tuning of ``mpservice.multiprocessing.ServerProcess``.


## [0.12.7] - 2023-05-07

### Removed

- Methods ``mpservice.streamer.Stream.{async_parmap, parmap_async}`` are dropped and merged into ``parmap``.
- Function ``mpservice.http.run_app``.

### Changed

- ``mpservice.streamer.Stream.parmap``: parameter ``executor`` became named only.
- ``mpservice.multiprocessing.Manager`` was renamed ``ServerProcess``.
- Parameter ``backlog`` to ``mpservice.mpserver.Server.__init__`` was renamed to ``capacity``.

### Added

- ``mpservice.streamer.Stream`` added extensive support for async.
- Methods ``mpservice.streamer.Stream.{to_sync, to_async, __aiter__}``.
- Method ``mpservice.mpserver.Server.full`` and properties ``mpservice.mpserver.Server.{capacity, backlog}``.
- Added capabilities to cancel a item submitted to ``mpservice.mpserver.Server`` and halt its processing in the pipeline as soon as practical.
- Made ``mpservice.mpservice.multiprocessing`` more close to the standard ``multiprocessing`` in terms of what can be imported from it.
- New parameter ``name`` to ``SpawnContext.Manager``.
- ``SpawnProcess`` captures warnings to logging.


## [0.12.6] - 2023-04-28

### Added

- New method ``mpservice.streamer.Stream.async_parmap`` with corresponding class ``AsyncParmapper``.

### Improved

- The cleanup or "finalize" logic of ``mpservice.streamer.{Buffer, Parmapper, AsyncParmapper, ParmapperAsync}``.


## [0.12.5] - 2023-04-24

### Added

- New method ``mpservice.streamer.Stream.parmap_async``, taking an async worker function.

### Fixed

- Bug in deprecation warning in ``util`` and ``server_process``.


## [0.12.4] - 2023-04-22

### Refactor

- Refactored ``util`` to split it into modules ``mpservice.multiprocessing``,
  ``mpservice.threading``, ``mpservice.concurrent.futures`` to have some imports
  correspond to those in the standard libs. ``util`` is deprecated.

- ``server_process`` was merged into ``mpservice.multiprocessing``.

### Removed

- ``ProcessServlet`` and ``ThreadServlet`` lost parameter ``name`` to ``__init__``.
- class ``ProcessLogger``.

### Changed

- The modules ``mpserver``, ``multiprocessing``, ``threading`` each defines its own ``TimeoutError``
  exception class.

## Added

- ``mpservice.mpserver.Worker`` got new parameter ``worker_index`` to ``__init__``, which is
  automatically provided by the parent ``ProcessServlet`` or ``ThreadServlet``.
  Subclasses of ``Worker`` should be sure to accept this parameter in their ``__init__``.
- function ``mpservice.multiprocessing.get_context``.
- ``mpservice.multiprocessing.Manager`` gets two init parameters ``process_name`` and ``process_cpu``.
- ``mpservice.concurrent.futures.ProcessPoolExecutor`` gets parameter ``mp_context`` to be compatible
  with the standard lib, but with a different default that is ``mpservice.multiprocessing.MP_SPAWN_CTX``.
  
## Enhanced

- ``mpservice.multiprocessing.SpawnProcess`` finetune on ``join`` and finalization cleanup.


## [0.12.3] - 2023-04-14

### Removed

- Remove dependency on ``overrides``.

### Added

- ``mpservice.util.{Process, Thread}`` have customized method ``join`` that will raise the exception raised in the child process or thread.

### Enhanced or changed

- ``mpservice.util.{Process, Thread}`` finetune related to exceptions.
- ``mpservice.util.{Process, Thread}``: parameter ``loud_exception`` moved from ``__init__`` to ``submit``.
- ``streamer`` finetune related to exception printout in worker threads/processes.


## [0.12.2] - 2023-03-31

### Changed

- ``SpawnProcess`` does not forward logs to the main process if the root logger has any handler configured.

### Fixed

- ``SpawnProcess`` and ``Thread`` in "loud_exception" mode do not print exception info if the exception
  is ``SystemExit(0)``. This is the case when a "server process" exits.

### Improved

- ``mpservice.mpserver.Server.stream`` retries on enqueue timeout.
- Finetune to waiting times in `Server`.


## [0.12.1] - 2023-03-26

### Added

- ``EnsembleServlet`` gets new parameter ``fail_fast`` to control behavior when ensemble members return exceptions.
- New exception class ``EnsembleError``.
- Added ``mpservice.util.Process``, which is an alias to ``util.SpawnProcess``.
- Refinements to ``mpservice.util.SpawnProcessPoolExecutor``.
- Added ``mpservice.util.ProcessPoolExecutor``, which is an alias to ``mpservice.util.SpawnProcessPoolExecutor``.
- New class ``mpservice.util.ThreadPoolExecutor``.
- New class ``mpservice.mpserver.SwitchServlet``.

### Fixed

- ``mpservice.util.{Thread, SpawnProcess}`` print out tracback upon exception, making errors in concurrent code more
  discoverable. This functionality was there previously but it was buggy.
- Fixed a deadlock situation during the shutdown of streamer ``parmap``.


## [0.12.0] - 2023-03-10

### Bug fixes

- Bug in `mpservice.mpserver.EnsembleServlet`.


## [0.11.9] - 2022-02-25

### Removed

- Deprecated context manager on `Streamer`. Instead, use the object directly.
- Deprecated function `mpservice.util.is_exception`.

### Changed

- `Streamer.peek` parameter `interval`: default changed to 1 from 1000.
- Class `Streamer` is renamed `Stream`; the old class `Stream` was removed.

### Added or enhanced

- `mpservice.streamer.Parmapper.__init__` takes two new arguments `executor_initializer`
  and `executor_init_args`.
- Simplifications to the implementation of `streamer.py`, making use of `GeneratorExit` and removing class `Stream`.
- New utility functions `mpservice.util.get_shared_thread_pool`, `mpservice.util.get_shared_process_pool`.


## [0.11.8] - 2022-12-21

The two largest efforts of this release are documentation and "streamer" refactor.

### Removed

- `mpservice.streamer.Streamer.{drop_first_n, peek_random, drop_if, keep_if}`, and corresponding classes
  `Dropper`.
- `mpservice.streamer.Streamer.drop_exceptions`.

### Changed

- `mpservice.streamer.Streamer.transform`: parameter `concurrency` used to default to 1 (i.e. no concurrency), now defaults to higher numbers (i.e. with concurrency).
- `mpservice.mpserver.{Sequential, Ensemble}` were renamed to `SequentialServlet` and `EnsembleServlet` respectively.
- `mpservice.streamer.Streamer.drain`: return count of elements processed, instead of the tuple of element count and exception count.
- `mpservice.streamer.Streamer.peek` was refactored.
- `mpservice.streamer.Streamer.transform` was renamed to `parmap`.
- Relaxed the requirement for using context manager with `Streamer`.
- `Streamer.parmap` uses processes by default, instead of threads.

### Added or enhanced

- Enhanced documentation. Started to host generated doc on Read the Docs.
- New class `mpservice.mpserver.CpuAffinity`.
- New method on `mpservice.streamer.Streamer` and corresponding classes:
  `filter` and `Filter`, `tail` and `Tailor`, `map` and `Mapper`, `groupby` and `Grouper`.
- New method `mpservice.streamer.Streamer.filter_exceptions`, `mpservice.streamer.Streamer.accumulate`.


## [0.11.7.post1] - 2022-10-21

- Upgrade for a breaking change in `uvicorn` 0.19.0.


## [0.11.7] - 2022-10-15

- `Streamer` implementation finetune, mainly about worker thread/process finalization.
- `Streamer` removes methods that are trivial (so user can implement them if needed) and unnecessary or not very needed: `collect`, `drop_nones`, `keep_every_nth`, `keep_random`, `log_every_nth`.
- `Streamer.log_exceptions` was renamed `peek_exceptions` with minor changes.
- Parameter `shed_load` to `mpservice.mpserver.Server.async_call` is renamed to `backpressure`.


## [0.11.6] - 2022-10-07

- `mpserver` wait-time fine tunning.
- `mpservice.mpserver.Server.async_call` gets new parameter `shed_load` with default `True`.
- New exception class `PipelineFull` in `mpserver`.


## [0.11.5] - 2022-09-22

- `RemoteException` is re-written with much simplifications; the class is moved from `remote_exception` to `util`; the module `remote_exception` is removed.
- Enhancements to `SpawnProcess`.
- Improvements to util.ProcessLogger`.
- The new constant `mpservice.util.MP_SPAWN_CTX` is a customization to the standard spawn
  context that uses `SpawnProcess` instead of `Process`.
- Use spawn method or `SpawnProcess` exclusively or by default in various places in the code.
- `Streamer.transform` gets new parameter `executor` to support multiprocessing for CPU-bound operators.
- The module `server_process` is re-written.
- The module `named_pipe` is renamed `pipe`.


## [0.11.4] - 2022-09-01

- `mpservice.util.ProcessLogger` gets context manager methods.
- New class `mpservice.util.SpawnProcess`.


## [0.11.3] - 2022-06-24

- Add dependency `asgiref`.
  Previously we've relied on getting `asgiref` from `uvicorn` dependency,
  which is a bad idea. Recently, `uvicorn` removed its dependency on `asgiref`.
- Reduce the default frequency of resource utilization logs.


## [0.11.2] - 2022-06-05

- Refinement and simplification to `streamer`.
- Refinement to `server_process`.


## [0.11.1] - 2022-05-31

- Added `mpservice.mpserver.ThreadWorker` and `ThreadServlet`.
- Simplified `mpserver` parameter for CPU pinning spec.
- Added log on worker process CPU/memory utilization in `mpserver`.


## [0.11.0] - 2022-05-27

- Refactor to `mpserver` with API changes.
  New design allows flexible composition of sequential and ensemble setups,
  leading to considerable enhancements in capability and flexibility.
  There are also considerable improvements to the implementation
  (in terms of simplicity, elegance, robustness).
- Replaced all uses of `time.monotonic` by `time.perf_counter`, which has much
  higher resolution.
- Added module `named_pipe`.


## [0.10.9] - 2022-05-21

- Added (or brought back) parameter `backlog` to `MPServer`.
- Implimentation improvements: simplified utitlity queues; removed error pipe of MPServer.


## [0.10.8] - 2022-05-18

- Finetune to `EnsembleServer`: ensemble elements could be `RemoteException`
  objects, i.e. failure of one ensemble component will not halt or invalidate
  the other components.


## [0.10.7] - 2022-05-15

- By default, `MPServer` uses the new, custom queue type `Unique` for faster
  buildup of batches for servlets. Removed `ZeroQueue` and `BasicQueue`.
- Simplified timeout in `mpserver`. `EnqueueTimeout` is gone; `TotalTimeout` is renamed
  `Timeout`. The parameters `enqueue_timeout` and `total_timeout` are combined
  into `timeout`.
- `ServerProcess` gets new parameter `ctx` for multiprocessing context.


## [0.10.6] - 2022-04-29

- Added alternative multiprocessing queues, namely `BasicQueue`, `FastQueue` and `ZeroQueue`,
  in an attempt to improve service speed, expecially batching.
  This topic is still under experimentation. `ZeroQueue` is not ready for use.
- Changed socket encoder from 'orjson' to 'pickle'.
- Removed `max_queue_size` parameter of `MPServer`; use unbounded queues.
- `MPServer` parameter `cpus` renamed to `main_cpu` with default value `0`.


## [0.10.5] - 2022-04-08

- Minor fine-tuning and documentation touch-ups.


## [0.10.4] - 2022-04-03

- Rewrote `socket` to be fully based on `asyncio.streams`.
- Refactored socket server side to make its usage similar to a web app.
- Refactored `_streamer` for much improved robustness and simplicity.
- Streamer API changes, mainly:
  1. use context manager;
  2. operations modify the hosting object in-place, hence eliminating the need to assign
     intermediate transforms to new names.
- `Streamer.transform` now accepts both sync and async functions as the "operator".
- Improved `MPServer.stream` and `SocketClient.stream`; removed `MPServer.stream2`.
- Rewrote `RemoteException`, especially for pickle-safety.
- `SocketServer` shutdown handling.
- Removed `MPServer.{start, stop}`; use `__enter__/__exit__`.
- Removed some contents of `util`.


## [0.10.3.post1, post2, post3, post4, post5] - 2022-03-25

- Add async-based `MPServer.stream2`.
- Improve printing of RemoteException in `__exit__`.
- SocketServer creates directory path as needed at startup.
- Small improvements.


## [0.10.3] - 2022-03-19

- Removed async streamer.
- Simplified implementation of the `transform` method of streamer.


## [0.10.2] - 2022-03-19

- Simplify `MPServer.stream`; remove `MPServer.async_stream`; the functionality
  of `async_stream` is provided by `stream`.
- Make more utility functions available in `util`.
- Simplify `_async_streamer`.


## [0.10.1] - 2022-03-17

- `mpservice.http_server` was renamed to `mpservice.http`.
- Initial implementation of socket client/server.


## [0.10.0] - 2022-03-13

- `mpserver` fine tune, esp about graceful shutdown.
- Use `overrides` to help keep sanity checks on class inheritance.
- Bug fixes in streamer.


## [0.9.9.post1] - 2022-03-11

- Handle logging in multiprocessing.


## [0.9.9] - 2022-03-10

- Use 'spawn' method for process creation.
- Refactored and simplified test/build process.
- Removed `mpservice.http_server.run_local_app`.
- Minor bug fixes.


## [0.9.8.post2] - 2022-01-31

- Improvements to the utilities in `mpservice.http_server`, esp regarding service shutdown.


## [0.9.8.post1] - 2022-01-31

- Requirement on `uvicorn` changes to `uvicorn[standard]`, which uses `uvloop`.


## [0.9.8] - 2022-01-30

- Reworked error propagation in streamer; fixed bugs therein.
- Renamed `exception` to `remote_exception`.
- Corner-case bug fixes in `MPServer`.
- Increase queue size in `ConcurrentTransformer`.


## [0.9.7] - 2022-01-16

- Refactor the `MPError` class, with a renaming to `RemoteException`.


## [0.9.6] - 2022-01-09

- Refactor the `MPError` class.


## [0.9.5.post3] - 2021-12-31

- Relax version requirements on dependencies.


## [0.9.5.post2] - 2021-12-05

- `BackgroundTask` refinements, esp about support for asyncio.


## [0.9.5.post1] - 2021-12-05 [YANKED]

- `BackgroundTask` bug fix.


## [0.9.5] - 2021-12-05 [YANKED]

- `BackgroundTask` refactor.


## [0.9.4] - 2021-12-05

- `MPServer.start` starts the servlets sequentially by default.


## [0.9.3] - 2021-11-16

- Bug fix in `mpservice.streamer.{Stream, AsyncStream}.batch`.
- Change Python version requirement from 3.7 to 3.8, due to the use of
  parameter `name` in `asyncio.create_task`.


## [0.9.2] - 2021-10-31

- Revise `background_task` API.


## [0.9.1] - 2021-10-31

- Rewrite `streamer` and `async_streamer` to avoid queues in simple "pass-through" ops, such as `drop_if`, `log_exceptions`.
- Minor improvements to `http_server`.
- Possible bug fix related to `total_timeout`.
- Added new module `background_task`.


## [0.9.0] - 2021-08-28

- Bug fix.
- `mpservice.mpserver.Servlet.__call__` is renamed to `call`.


## [0.8.9] - 2021-08-16

- Add `mpservice.mpserver.EnsembleServer` to implement ensembles; rename `Server` to `SequentialServer`.
- Add `mpservice.mpserver.SimpleServer`.
- Revise `cpu` specification in `mpserver`.


## [0.8.8] - 2021-08-10

- `mpservice.mpserver.Server` gets a sync API, in addition to the existing async API.
- `mpservice.mpserver.Server` gets sync and async stream methods.


## [0.8.7] - 2021-08-05

Streamer API fine-tuning and bug fixes.


## [0.8.6] - 2021-08-02

Added sync streamer.


## [0.8.5] - 2021-08-01

Refactor the async streamer API and tests.


## [0.8.4] - 2021-07-28

Fine tuning on `streamer`.


## [0.8.3] - 2021-07-27

Added:

- `mpservice.streamer.{transform, unordered_transform}` get new parameter `return_exceptions`.
  Similarly, `drain` gets new parameter `ignore_exceptions`.


## [0.8.2] - 2021-07-25

Added:

- `streamer`


## [0.8.1] - 2021-07-24

Changed:

- `Servlet.process` is renamed to `Servlet.__call__`.
- `mpservice._http` renamed to `mpservice.http_server` with enhancements.
- `mpservice._server_process` renamed to `mpservice.server_process`.


## [0.8.0] - 2021-05-19

Changed:

- Replaced machine learning terminologies ('model', 'predict', etc) in namings
  by generic names. This broke public APIs.
- Guarantee worker initiation (running in other processes) before service startup
  finishes.


## [0.7.3] - 2021-05-09

Added:

- `ModelService.a_predict` gains new parameters `enqueue_timeout` and `total_timeout`,
  with default values 10 and 100 seconds, respectively.
