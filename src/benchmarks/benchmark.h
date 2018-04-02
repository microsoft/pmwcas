// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#ifndef WIN32
#include "benchmark_linux.h"
#else

#include <cstdint>
#include <atomic>
#include <vector>
#include <deque>
#include <thread>

#include "common/environment_internal.h"
#include "util/macros.h"

namespace pmwcas {
namespace benchmark {

/// Glue to make running multi - threaded benchmarks easier.Classes subclass
/// this and provide a Main(), Setup(), Teardown(), and Dump() method.
/// Main() is called in parallel by a requested number of the threads.There
/// is some magic in there to start and stop timing precisely during the
/// experiment.
class Benchmark {
 public:
  Benchmark()
    : threads_ready_{ 0 }
    , threads_finished_{ 0 }
    , start_running_{ false }
    , run_seconds_{}
    , is_shutdown_{ false }
    , dump_count_{} {
  }

  /// Run in parallel by the number of the threads specified to Run().
  /// \a thread_index an integer between 0 and the number of threads run - 1.
  /// Important: Main() must call WaitForStart() once it is ready to start
  /// the benchmark.This gives Main() a chance to set up its stack without
  /// setup overhead being measured. Missing this call in a thread will
  /// result in deadlock.
  virtual void Main(size_t thread_index) = 0;

  /// Called during Run() before threads call Main(). Useful to initialize
  /// any state accessed during the test.
  virtual void Setup(size_t thread_count) = 0;

  /// Called during Run() after the benchmark is complete. Useful to
  /// uninitialize any state accessed during the test.
  virtual void Teardown() = 0;

  /// Returns the total operation count for the benchmark aggregated from all
  /// worker threads. This function is not threadsafe and should be called only
  /// after the workload completes.
  virtual uint64_t GetOperationCount() = 0;

  virtual void Dump(size_t thread_count, uint64_t run_ticks,
                    uint64_t dump_id, bool final_dump) {
    MARK_UNREFERENCED(thread_count);
    MARK_UNREFERENCED(run_ticks);
    printf("> Benchmark %llu Dump %llu\n", dump_id,
           final_dump ? ~0llu : dump_count_);
    ++dump_count_;
  }

  /// Run \a threadCount threads running Entry() and measure how long the
  /// run takes.Afterward, GetLastRunSeconds() can be used to retrieve how
  /// long the execution took.This method will call Setup() and Teardown()
  /// respectively before and after the executions of Entry().
  void Run(size_t thread_count, uint64_t seconds_to_run,
           AffinityPattern affinity, uint64_t dump_interval) {
    threads_finished_ = 0;
    threads_ready_ = 0;
    start_running_ = false;

    Setup(thread_count);

    LARGE_INTEGER frequency;
    QueryPerformanceFrequency(&frequency);
    uint64_t ticks_per_second = frequency.QuadPart;

    // Start threads
    std::deque<std::thread> threads;
    for(size_t i = 0; i < thread_count; ++i) {
      threads.emplace_back(&Benchmark::entry, this, i, thread_count, affinity);
    }

    // Wait for threads to be ready
    while(threads_ready_.load() < thread_count);

    uint64_t unique_dump_id = __rdtsc();
    if(dump_interval > 0) {
      Dump(thread_count, 0, unique_dump_id, false);
    }

    VLOG(1) << "Starting benchmark.";

    // Start the benchmark
    LARGE_INTEGER counter;
    QueryPerformanceCounter(&counter);
    uint64_t start = counter.QuadPart;
    start_running_.store(true, std::memory_order_release);

    const int64_t end = start + ticks_per_second * seconds_to_run;
    if(dump_interval > 0) {
      uint64_t next_dump_ticks = ticks_per_second + start;
      uint64_t next_dump_seconds = 1;
      while(threads_finished_.load() < thread_count) {
        Sleep(10);
        QueryPerformanceCounter(&counter);
        uint64_t now = counter.QuadPart;
        if(end <= counter.QuadPart) {
          is_shutdown_.store(true, std::memory_order_release);
          break;
        }
        if(now < next_dump_ticks) continue;

        // Collect metrics after the experiment end.
        unique_dump_id = __rdtsc();
        Dump(thread_count, now - start, unique_dump_id, false);

        // 'Schedule' next dump.
        next_dump_seconds += dump_interval;
        next_dump_ticks = next_dump_seconds * ticks_per_second + start;
      }
    } else {
      // Sleep the required amount of time before setting the shutdown flag.
      ::Sleep((DWORD)seconds_to_run * 1000);
      is_shutdown_.store(true, std::memory_order_release);

      // Wait for all threads to finish their workload
      while(threads_finished_.load() < thread_count) {
        ::Sleep(10);
      }
    }

    for(auto& thread : threads) {
      thread.join();
    }

    while(0 == end_.load());
    unique_dump_id = __rdtsc();

    VLOG(1) << "Benchmark stopped.";

    Dump(thread_count, end_ - start, unique_dump_id, true);

    run_seconds_ = double(end_ - start) / double(ticks_per_second);

    Teardown();
  }

  /// Must be called in Entry() after initial setup and before the real
  /// work of the experiment starts.This acts as a barrier; the main thread
  /// waits for all threads running as part of the measurement to reach this
  /// point and then releases them to start to body of the experiment as
  /// closely synchronized as possible.Forgetting to call this in Entry()
  /// will result in an infinite loop.
  void WaitForStart() {
    threads_ready_.fetch_add(1, std::memory_order_acq_rel);
    while(!start_running_.load(std::memory_order_acquire));
  }

  /// Must be called in Entry() after initial setup.Used by the worker threads
  /// to test for the shutdown flag after #m_nRuntTimeBeforeShutdown seconds
  /// have passed.
  bool IsShutdown() {
    return is_shutdown_.load(std::memory_order_acquire);
  }

  /// Return the number of seconds Entry() ran on any of the threads starting
  /// just after WaitForStart() up through the end of Entry().
  double GetRunSeconds() {
    return run_seconds_;
  }

 private:
  /// Internal springboard used to automatically notify the main thread of
  /// Entry() completion.The last thread to finish the calls stuffs a snapshot
  /// of the ending time so the main thread can accurately determine how long
  /// the run took without polling aggressively and wasting a core.
  void entry(size_t thread_index, size_t thread_count,
             AffinityPattern affinity) {
    if(affinity != AffinityPattern::OSScheduled) {
      Environment::Get()->SetThreadAffinity(thread_index, affinity);
    }
    Main(thread_index);
    size_t previous =
      threads_finished_.fetch_add(1, std::memory_order_acq_rel);
    if(previous + 1 == thread_count) {
      LARGE_INTEGER counter;
      QueryPerformanceCounter(&counter);
      uint64_t end = counter.QuadPart;
      end_.store(end);
    }
  }

 private:
  /// Number of threads that have reached WaitForStart(). Used by the main
  /// thread to synchronize the start of the experiment.
  std::atomic<size_t> threads_ready_;

  /// Number of threads that have exited Entry(). Used by the main thread
  /// to determine when the experiment has completed.
  std::atomic<size_t> threads_finished_;

  /// The main thread sets this to true once #m_threadReady equals the
  /// number of threads that are part of the experiment. This releases all
  /// the threads to run the remainder of Entry() that follows the call
  /// to WaitForStart().
  std::atomic<bool> start_running_;

  /// Tracks how long in seconds each collective run of Entry() took for
  /// a set of threads. Used in GetAllRunSeconds() and GetLastRunSeconds().
  std::atomic<double> run_seconds_;

  /// Time in QueryPerformanceCounter ticks that the last thread finished
  /// its work.
  std::atomic<uint64_t> end_;

  /// Shutdown flag for timed tests.
  std::atomic<bool> is_shutdown_;

  /// Number of metrics dumps that have been done so far
  uint64_t dump_count_;
};

}
} // namespace pmwcas::benchmark
#endif  // WIN32
