// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once
#include <iostream>
#include "util/core_local.h"

namespace pmwcas {
class DescriptorPool;

// A singleton (not a real one but works like it) for all MwCAS-related stats.
// The user must call Initialize() first, each individual thread then should
// call ThreadInitialize() before start.
struct MwCASMetrics {
  friend class DescriptorPool;
 public:
  MwCASMetrics& operator+=(const MwCASMetrics& other) {
    succeeded_update_count += other.succeeded_update_count;
    failed_update_count += other.failed_update_count;
    read_count += other.read_count;
    descriptor_scavenge_count += other.descriptor_scavenge_count;

    help_attempt_count += other.help_attempt_count;
    bailed_help_count += other.bailed_help_count;

    descriptor_alloc_count += other.descriptor_alloc_count;
    return *this;
  }

  MwCASMetrics& operator-=(const MwCASMetrics& other) {
    succeeded_update_count -= other.succeeded_update_count;
    failed_update_count -= other.failed_update_count;
    read_count -= other.read_count;
    descriptor_scavenge_count -= other.descriptor_scavenge_count;

    help_attempt_count -= other.help_attempt_count;
    bailed_help_count -= other.bailed_help_count;

    descriptor_alloc_count -= other.descriptor_alloc_count;
    return *this;
  }

  friend MwCASMetrics operator+(MwCASMetrics left, const MwCASMetrics& right) {
    left += right;
    return left;
  }

  friend MwCASMetrics operator-(MwCASMetrics left, const MwCASMetrics& right) {
    left -= right;
    return left;
  }

  MwCASMetrics()
      : succeeded_update_count(0),
        failed_update_count(0),
        read_count(0),
        descriptor_scavenge_count(0),
        help_attempt_count(0),
        bailed_help_count(0),
        descriptor_alloc_count(0) {
  }

  uint64_t GetUpdateAttemptCount() {
    return succeeded_update_count + failed_update_count;
  }

  inline void Print() {
    if(!enabled) return;
    auto update_attempts = GetUpdateAttemptCount();
    std::cout << "> UpdateAttempts " << update_attempts
              << " (success " << succeeded_update_count
              << " failure " << failed_update_count << ")" << std::endl;
    printf("> UpdateFailurePercent %2f\n",
      (double)failed_update_count / (double)update_attempts * 100);
    std::cout << "> Reads " << read_count << std::endl;
    std::cout << "> DescriptorScavenges " <<
      descriptor_scavenge_count << std::endl;
    std::cout << "> HelpAttempts " << help_attempt_count << std::endl;
    std::cout << "> BailedHelpAttempts " << bailed_help_count << std::endl;
    std::cout << "> DecsriptorAllocations " <<
      descriptor_alloc_count << std::endl;
  }

  // Initialize the global CoreLocal container that encapsulates an array
  // of MwCASMetrics, one per thread. Call this exactly once upon startup.
  static Status Initialize() {
    if(enabled) MwCASMetrics::instance.Initialize();
    return Status::OK();
  }

  static Status Uninitialize() {
    if(enabled) return MwCASMetrics::instance.Uninitialize();
    return Status::OK();
  }

  static Status ThreadInitialize() {
    if(enabled) {
      auto *tls_metrics = reinterpret_cast<MwCASMetrics *>(
        Allocator::Get()->Allocate(sizeof(MwCASMetrics)));
      if (!tls_metrics) {
        return Status::OutOfMemory();
      }
      new (tls_metrics) MwCASMetrics();
      *instance.MyObject() = tls_metrics;
    }
    return Status::OK();
  }

  inline static void AddRead() {
    if(enabled) ++MyMetric()->read_count;
  };

  inline static void AddSucceededUpdate() {
    if(enabled) ++MyMetric()->succeeded_update_count;
  }

  inline static void AddFailedUpdate() {
    if (enabled) ++MyMetric()->failed_update_count;
  }

  inline static void AddDescriptorScavenge() {
    if(enabled) ++MyMetric()->descriptor_scavenge_count;
  }

  inline static void AddHelpAttempt() {
    if(enabled) ++MyMetric()->help_attempt_count;
  }

  inline static void AddBailedHelp() {
    if (enabled) ++MyMetric()->bailed_help_count;
  }

  inline static void AddDescriptorAlloc() {
    if (enabled) ++MyMetric()->descriptor_alloc_count;
  }

  inline static void Sum(MwCASMetrics &sum) {
    for (uint32_t i = 0; (i < instance.NumberOfObjects()) && enabled; ++i) {
      auto *thread_metric = *instance.GetObject(i);
      sum += *thread_metric;
    }
  }

 private:
  inline static MwCASMetrics* MyMetric() {
    RAW_CHECK(enabled, "metrics is disabled");
    return *MwCASMetrics::instance.MyObject();
  }

  static bool enabled;
  static CoreLocal<MwCASMetrics *> instance;

  uint64_t succeeded_update_count;
  uint64_t failed_update_count;
  uint64_t read_count;
  uint64_t descriptor_scavenge_count;

  uint64_t help_attempt_count;
  uint64_t bailed_help_count;

  uint64_t descriptor_alloc_count;
  uint64_t padding;
};

}  // namespace pmwcas
