mpservice
=========

The package ``mpservice`` provides a few groups of facilities. The first group helps ochestrating an operation 
with multiple stages/components in threads and processes, such as a machine learning model;
the second group concerns stream processing, optionally involving threads or processes for concurrency;
the third group provides customization and enhancements to the standard packages ``multiprocessing`` and ``threading``.
The central theme of ``mpservice`` is concurrency by threads or processes.

Read the `documentation <https://mpservice.readthedocs.io/en/latest/>`_.

To install, do

::
    
    python3 -m pip install mpservice


Status
------

Production ready. Under active development.


Python version
--------------

Development and testing were conducted in Python 3.8 until version 0.12.0.
Starting with 0.12.1, development and testing happen in Python 3.10.
Code continues to NOT intentionally use features beyond Python 3.8.
I expect to require 3.10 at some pont in 2024.
