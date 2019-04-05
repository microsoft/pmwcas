# Persistent Multi-Word Compare-and-Swap (PMwCAS) for NVRAM

![Windows Build Status](https://justinlevandoski.visualstudio.com/_apis/public/build/definitions/c59a8e03-b063-4da5-8b4b-b0092d61c7cb/3/badge "Windows Build Status")

PMwCAS is a library that allows atomically changing multiple 8-byte words on non-volatile memory in a lock-free manner. It allows developers to easily build lock-free data structures for non-volatile memory and requires no custom recovery logic from the application. More details are described in the following [slide deck](http://www.cs.sfu.ca/~tzwang/pmwcas-slides.pdf), [full paper](http://justinlevandoski.org/papers/ICDE18_mwcas.pdf) and [extended abstract](http://www.cs.sfu.ca/~tzwang/pmwcas-nvmw.pdf):

```
Easy Lock-Free Indexing in Non-Volatile Memory.
Tianzheng Wang, Justin Levandoski and Paul Larson.
ICDE 2018.
```
```
Easy Lock-Free Programming in Non-Volatile Memory.
Tianzheng Wang, Justin Levandoski and Paul Larson.
NVMW 2019.
Finalist for Memorable Paper Award.
```

## Environment and Variants

The current code supports both Windows (CMake + Visual Studio) and Linux (CMake + gcc/clang) and four variants with different persistence modes:

1. Persistence using Intel [PMDK](https://pmem.io)
2. Persistence by emulation with DRAM
3. Volatile (no persistence support)
4. Volatile with Intel TSX (using hardware transaction memory to install descriptors, for `Volatile` only)

A persistence mode must be specified at build time through the `PMEM_BACKEND` option in CMake (default PMDK). See below for how to pass this option to CMake.

## Build (for Linux)
Suppose we build in a separate directory "build" under the source directory.

To build PMwCAS without TSX:
```
$ mkdir build
$ cd build
$ cmake -DPMEM_BACKEND=[PMDK/Volatile/Emu] -DCMAKE_BUILD_TYPE=[Debug/Release/RelWithDebInfo] ..
$ make -jN
```

To build a **volatile** PMwCAS variant that uses TSX to install descriptor pointers:
```
$ mkdir build
$ cd build
$ cmake -DPMEM_BACKEND=Volatile -DWITH_RTM=1 -DCMAKE_BUILD_TYPE=[Debug/Release/RelWithDebInfo] ..
$ make -jN
```

#### Descriptor size
By default each descriptor can hold up to four words. This can be adjusted at compile-time by specifying the `DESC_CAP` parameter to CMake, for example the following will allow up to 8 words per descriptor:
```
$ cmake -DDESC_CAP=8 -DPMEM_BACKEND=Volatile -DWITH_RTM=1 -DCMAKE_BUILD_TYPE=[Debug/Release/RelWithDebInfo] ..
```

#### Huge pages and memlock limits (for Linux)

Under Linux the `volatile` and `emu` variants use a simple thread-local allocator that uses huge pages. Make sure the system has enough huge pages:
```
sudo sh -c 'echo [x pages] > /proc/sys/vm/nr_hugepages'
```
By default the allocator needs ~10GB per socket, defined by `kNumaMemorySize` in [src/environment/environment_linux.h](./src/environment/environment_linux.h).

On Linux `mwcas_shm_server` (see below) requires a proper value for memlock limits. Add the following to `/etc/security/limits.conf` (replace "[user]" with your login) to make it unlimited (need **re-login** to apply):
```
[user] soft memlock unlimited
[user] hard memlock unlimited
```

#### To build on Windows (to be tested for PMDK):

```
$ md build
$ cd build
$ cmake -G [generator] ..
```

`[generator]` should match the version of Visual Studio installed and specify `Win64`, e.g., `Visual Studio 14 2015 Win64`. Use `cmake -G` to get a full list.

Then either opening and building pmwcas.sln in Visual Studio, or use:

```
$ msbuild pmwcas.sln /p:Configuration=[Release/Debug]
```

## NVRAM Emulation

For runs without real NVRAM device (e.g., NVDIMM or Intel 3D-XPoint), we provide a shared-memory interface for emulation. 

The basic idea is to start a dedicated process which creates a shared memory segment for the application (another process) to attach to, so that data will remain intact when the application crashes (the dedicated shared memory process is alive).

The shared memory process is implemented in [src/benchmarks/mwcas_shm_server.cc](./src/benchmarks/mwcas_shm_server.cc). It needs to start before the application.

## APIs

The central concept of PMwCAS is desrciptors. The typical steps to change multiple 8-byte words are:
1. Allocate a descriptor;
2. Fill in the descriptor with the expected and new values of each target word;
3. Issue the PMwCAS command to actually conduct the operation.

The target words often are pointers to dynamically allocated memory blocks. PMwCAS allows transparent handling of dynamically allocated memory depending on user-specified policy. For example, one can specify to allocate a memory block and use it as the 'new value' of a target word, and specify that this memory block be deallocated if the PMwCAS failed.

See [APIs.md](./APIs.md) for a list of useful APIs and memory related polices and examples.

## Thread Model

It is important to note that PMwCAS uses C++11 thread_local variables which must be reset if the descriptor pool is destructed and/or a thread is (in rare cases) re-purposed to use a different descriptor pool later. The library provides a `Thread` abstraction that extends `std::thread` for this purpose. The user application is expected to use the `Thread` class whenever `std::thread` is needed. `Thread` has the exactly same APIs as `std::thread` with an overloaded join() interface that clears its own thread-local variables upon exit. The `Thread` class also provides a static `ClearRegistry()` function that allows the user application to clear all thread local variables. All the user application needs is to invoke this function upon changing/destroying a descriptor pool.

For example, the below pattern is often used in our test cases:

```
DescriptorPool pool_1 = new DescriptorPool(...);  // Create a descriptor pool
... use pool_1 ...
delete pool_1;
Thread::ClearRegistry();  // Reset the TLS variables, after this it is safe to use another pool

DescriptorPool *pool_2 = new DescriptorPool(...);
... use pool_2 ...
Thread::ClearRegistry();
```

## Benchmarks

To use PMwCAS, simply link the built library with your application. See the [ICDE paper](http://justinlevandoski.org/papers/ICDE18_mwcas.pdf) for a complete list of APIs. The project provides two examples:

### Array benchmark
This benchmark atomically changes a random number of entries in a fixed-sized array (code in [src/benchmarks/mwcas_benchmark.cc](./src/benchmarks/mwcas_benchmark.cc)). 

A sample 10-second, 2-thread, 100-entry array run that changes four words:

```
$ mwcas_shm_server -shm_segment "mwcas" # start the shared memory process
$ mwcas_benchmark -shm_segment "mwcas" -threads 2 -seconds 10 -array_size 100 -word_count 4
```
See the source file for  a complete list of parameters.

### Doubly-linked list
Inside [src/double-linked-list](./src/double-linked-list) are implementations of  lock-free doubly-linked lists using single-word CAS and PMwCAS. A benchmark is implemented in [src/benchmarks/doubly_linked_list_benchmark.cc](./src/benchmarks/doubly_linked_list_benchmark.cc).

# Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.microsoft.com.

When you submit a pull request, a CLA-bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., label, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

## License

Copyright (c) Microsoft Corporation. All rights reserved.

Licensed under the MIT License.
