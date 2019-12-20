# Change Log

## Nov 2019
In this update, we fixed several bugs and improved  performance.

A big thank you to the following folks that helped to make PMwCAS even better.

- Wojciech Golab and his students from the University of Waterloo found a linearizability bug, which may cause spurious abort in some cases.
- Ziqi Wang from Carnegie Mellon University found an optimization that can improve the PMwCAS by reducing persistent memory flushes.
- Shaonan Ma from Tsinghua University located a bug in PMDK integration that a crash during allocation may cause a memory leak.
- Ao LI from the University of Toronto fixed an incorrect `inline` which caused the code failed to build with gcc. 

## June 2019

Added PMDK backend, bug fixes, and general improvements on code base.

Note that the PMDK integration assumes a Linux environment and might not work well on Windows machines.
