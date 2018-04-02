// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <cstdint>
#include <atomic>
#include <vector>
#include <deque>
#include <memory>

#include "include/environment.h"
#include "common/environment_internal.h"
#include "util/macros.h"

namespace pmwcas {

/// Run-time check to ensure a narrowing conversion is safe.
template <class Narrow, class Wide>
Narrow narrow(const Wide& wide) {
  Narrow narrow = static_cast<Narrow>(wide);
  return narrow;
}

/// An aid to reduce the boilerplate of writing multi-threaded performance or
/// smoke tests. To use this class, subclass it directly in your unit test.
/// Declare any shared data that all thread will operate on as class members.
/// Provide an Entry() implementation and call run() to have several threads
/// run the code (one variant of run() runs with a varying number of threads
/// as well). After the test executes GetLastRunSeconds() and
/// GetAllRunSeconds() can be used to find out how long the test took.
/// The Setup() and Teardown() methods can also be overridden to reset things
/// before/after each set of threads enters Entry().
///
/// One note on implementing Entry(): the body of Entry() must call
/// WaitForStart(). This notifies the main thread that the thread is ready
/// to run the experiment; it blocks until all participating threads have
/// called the method. This makes the timing of the tests more reliable
/// (it guarantees that we aren't measuring thread creation/destruction), and
/// it gives threads a chance to initialize useful and potentially heavyweight
/// stuff on their stack without messing up the measurements.
class PerformanceTest {

 protected:
  /// Structure to record thread-local statistics. Cache line sized in order
  /// to prevent false cache line sharing across threads.
  struct alignas(64) WorkerStatistics {
    WorkerStatistics()
  : operation_count{} {
  }

  uint64_t operation_count;
  uint64_t ____filler[7];
  };
  static_assert(64 == sizeof(WorkerStatistics),
                "WorkerStatistics not cacheline size");

 public:
  PerformanceTest()
    :
    threads_ready_{ 0 },
    threads_finished_{ 0 },
    start_running_{ false },
    run_seconds_{},
    is_shutdown_{ false },
    use_worker_statistics_ { false },
    runtime_before_shutdown_{} {
  }

  /// Provided by derived classes; contains that each thread should run during
  /// the test. Somewhere in this method WaitForStart() must be called,
  /// which acts as a barrier to synchronize the start of all the threads
  /// in the test and the start of timing in the main thread.
  virtual void Entry(size_t thread_index) {
    MARK_UNREFERENCED(thread_index);
  }

  /// Provided by the derived classes; Same as single-argument Entry function
  /// defined above with the addition of a reference to a thread-exclusive
  /// statistics struct use to record worker-specific statistics (e.g.,
  /// operations completed). Statistics are aggregated by the master thread
  /// after the workload completes.
  virtual void Entry(size_t thread_index, WorkerStatistics* statistics) {
    MARK_UNREFERENCED(thread_index);
    MARK_UNREFERENCED(statistics);
  }

  /// Can be overridden by derived classes to reset member variables into
  /// the appropriate state before a set of threads enters Entry().
  virtual void Setup() {}

  /// Can be overridden by derived classes to reset member variables into
  /// the appropriate state after a set of threads finishes running
  /// Entry(). This may be a good place to dump out statistics that are
  /// being tracked in member variables as well.
  virtual void Teardown() {}

  /// Sets the amount of time the test should run.The shutdown flag for the
  /// test will be set after \a nTime seconds.Worker threads can check for the
  /// flag using the IsShutdown() function.
  void SetRunTime(uint64_t time) {
    runtime_before_shutdown_ = time;
  }

  /// Sets a flag for whether to pass the Entry function a pointer to
  /// thread-local structure to record workload statistics.
  void UseWorkerStatistics() {
    use_worker_statistics_ = true;
  }

  /// Run \a threadCount threads running Entry() and measure how long the
  /// run takes.Afterward, GetLastRunSeconds() can be used to retrieve how
  /// long the execution took.This method will call Setup() and Teardown()
  /// respectively before and after the executions of Entry().
  void Run(size_t threadCount) {
    threads_finished_ = 0;
    threads_ready_ = 0;
    start_running_ = false;

    Setup();

    if(use_worker_statistics_) {
      worker_statistics_.reserve(threadCount);
    }

    // Start threads
    std::deque<Thread> threads;
    for(size_t i = 0; i < threadCount; ++i) {
      if(use_worker_statistics_) {
        worker_statistics_.emplace_back();
      }
      threads.emplace_back(&PerformanceTest::entry, this, i, threadCount);
    }

    // Wait for threads to be ready
    while(threads_ready_.load() < threadCount);

    // Start the experiment
    uint64_t start = Environment::Get()->NowMicros();
    start_running_.store(true, std::memory_order_release);

    if(runtime_before_shutdown_ > 0) {
      // Sleep the required amount of time before setting the shutdown flag.
      Environment::Get()->Sleep(runtime_before_shutdown_ * 1000);
      is_shutdown_.store(true, std::memory_order_release);
    }

    // Wait for all threads to finish their workload
    while(threads_finished_.load() < threadCount) {
      Environment::Get()->Sleep(10);
    }

    for(auto& thread : threads) {
      thread.join();
    }

    // Record the run time in seconds
    run_seconds_.push_back((double)(end_ - start) / 1000);

    Teardown();
  }

  /// Repeatedly run Entry() first starting with \a startingThreadCount
  /// and increasing by one until Entry() has been run with \a endThreadCount
  /// threads.Afterward, GetAllRunSeconds() can be used to retrieve how
  /// long each of the executions took.This method will call Setup()
  /// just before running each new round with a different number of threads
  /// through Entry().Similarly, Teardown() will be called after each
  /// set of threads finishes Entry(), before the next number of threads
  /// is run through Entry().
  void Run(size_t startThreadCount, size_t endThreadCount) {
    for(size_t threadCount = startThreadCount; threadCount <= endThreadCount;
        ++threadCount) {
      Run(threadCount);
    }
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
  double GetLastRunSeconds() {
    return run_seconds_.back();
  }

  /// Return the number of seconds Entry() ran on any of the threads starting
  /// just after WaitForStart() up through the end of Entry() for all of the
  /// run of varying sets of threads.
  std::vector<double> GetAllRunSeconds() {
    return run_seconds_;
  }

  /// Returns the total operation count for the benchmark aggregated from all
  /// worker threads. This function is not threadsafe and should be called only
  /// after the workload completes.
  uint64_t GetTotalOperationCount() {
    if(!use_worker_statistics_) return 0;
    uint64_t total{};
    for(auto iter = worker_statistics_.begin();
        iter < worker_statistics_.end(); ++iter) {
      total += iter->operation_count;
    }
    return total;
  }

 private:
  /// Internal springboard used to automatically notify the main thread of
  /// Entry() completion.The last thread to finish the calls stuffs a snapshot
  /// of the ending time so the main thread can accurately determine how long
  /// the run took without polling aggressively and wasting a core.
  void entry(size_t threadIndex, size_t threadCount) {
    if(use_worker_statistics_) {
      Entry(threadIndex, &worker_statistics_[threadIndex]);
    } else {
      Entry(threadIndex);
    }
    size_t previous =
      threads_finished_.fetch_add(1, std::memory_order_acq_rel);
    if(previous + 1 == threadCount) {
      end_ = Environment::Get()->NowMicros();
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
  std::vector<double> run_seconds_;

  /// Time in QueryPerformanceCounter ticks that the last thread finished
  /// its work.
  uint64_t end_;

  /// Shutdown flag for timed tests.
  std::atomic<bool> is_shutdown_;

  /// Number of seconds a test will run before the #is_shutdown_ flag is set.
  uint64_t runtime_before_shutdown_;

  /// Whether the workload will have workers record statistics.
  bool use_worker_statistics_;

  /// Vector of workload statistics to hand out to each worker. Only allocated
  /// if use_worker_statistics_ is true.
  std::vector<WorkerStatistics> worker_statistics_;
};

} // namespace pmwcas

