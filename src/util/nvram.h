// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#ifdef WIN32
#include <intrin.h>
#endif

#include "glog/logging.h"
#include "glog/raw_logging.h"
#include "include/environment.h"

namespace pmwcas {

struct NVRAM {
#ifdef PMEM
  /// How many cycles to delay for an emulated NVRAM write
  static uint64_t write_delay_cycles;
  static double write_byte_per_cycle;
  static bool use_clflush;

  static void InitializeClflush() {
    use_clflush = true;
    LOG(INFO) << "Will use CLFLUSH";
  }

  static void InitializeSpin(uint64_t delay_ns, bool emulate_wb) {
    use_clflush = false;
    if(delay_ns) {
      uint64_t start = __rdtsc();

#ifdef WIN32
      Sleep(1000);  // 1 second
#else
      sleep(1);
#endif

      uint64_t end = __rdtsc();
      write_delay_cycles = (double)(end - start) / 1000000000 * delay_ns;
    }
    LOG(INFO) << "Write delay: " << delay_ns << "ns (" <<
        write_delay_cycles << " cycles)";

    if(emulate_wb) {
      char test_array[kCacheLineSize];
      unsigned int not_used = 0;
      uint64_t start = __rdtscp(&not_used);
      _mm_clflush(test_array);
      uint64_t end = __rdtscp(&not_used);
      write_byte_per_cycle = (double)kCacheLineSize / (end - start);
    } else {
      write_byte_per_cycle = 0;
    }

    LOG(INFO) << "BW emulation: " << write_byte_per_cycle << " bytes per cycle";
  }

  static inline void Flush(uint64_t bytes, const void* data) {
    if(use_clflush) {
      RAW_CHECK(data, "null data");
      uint64_t ncachelines = (bytes + kCacheLineSize - 1) / kCacheLineSize;
      // Flush each cacheline in the given [data, data + bytes) range
      for(uint64_t i = 0; i < ncachelines; ++i) {
        _mm_clflush(&((char*)data)[i * ncachelines]);
      }
    } else {
#if 0
      // Previously this was calculated outside the [if] block, slowing down
      // read-only workloads (slower than with clflush).
      //
      // Update #2: perhaps the fairest way is comment out this whole else
      // block so it does nothing when we don't specify delays.
      if(write_delay_cycles > 0) {
        unsigned int aux = 0;
        uint64_t bw_cycles = write_byte_per_cycle > 0 ?
          bytes / write_byte_per_cycle : 0;
        uint64_t start = __rdtscp(&aux);

        while (__rdtscp(&aux) - start < write_delay_cycles + bw_cycles) {
          // SPIN
        }
      }
#endif
    }
  }
#endif
};

}  // namespace pmwcas
