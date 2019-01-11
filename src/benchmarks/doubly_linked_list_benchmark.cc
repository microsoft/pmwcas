// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#define NOMINMAX

#include <string>
#include <inttypes.h>

#include <gtest/gtest.h>
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "glog/raw_logging.h"

#include "benchmarks/benchmark.h"
#ifdef WIN32
#include "environment/environment_windows.h"
#else
#include "environment/environment_linux.h"
#endif
#include "include/pmwcas.h"

#include "util/auto_ptr.h"
#include "util/core_local.h"
#include "double-linked-list/doubly_linked_list.h"
#include "util/random_number_generator.h"

using namespace pmwcas::benchmark;

DEFINE_uint64(seed, 1234,
    "base random number generator seed, the thread index is added to this "
    "number to form the full seed");
DEFINE_uint64(metrics_dump_interval, 0, "if greater than 0, the benchmark "
    "driver dumps metrics at this fixed interval (in seconds)");
DEFINE_int32(insert_pct, 0, "percentage of insert");
DEFINE_int32(delete_pct, 0, "percentage of delete");
DEFINE_int32(search_pct, 0, "percentage of search");
DEFINE_int32(read_heavy, 0, "whether to run the read-heavy experiment");
DEFINE_uint64(read_heavy_modify_range, 0,
  "maximum how far away to randomly choose a location for insert/delete");
DEFINE_int32(initial_size, 0, "initial number of nodes in the list");
DEFINE_string(sync, "cas", "syncronization method: cas, pcas, mwcas, or pmwcas");
DEFINE_int32(affinity, 1, "affinity to use in scheduling threads");
DEFINE_uint64(threads, 2, "number of threads to use for multi-threaded tests");
DEFINE_uint64(seconds, 10, "default time to run a benchmark");
DEFINE_uint64(mwcas_desc_pool_size, 10000000, "number of total descriptors");
#ifdef PMEM
DEFINE_uint64(write_delay_ns, 0, "NVRAM write delay (ns)");
DEFINE_bool(emulate_write_bw, false, "Emulate write bandwidth");
DEFINE_bool(clflush, false, "Use CLFLUSH, instead of spinning delays."
  "write_dealy_ns and emulate_write_bw will be ignored.");
#endif

//DEFINE_uint64(payload_size, 8, "payload size of each node");

namespace pmwcas {

/// Maximum number of threads that the benchmark driver supports.
const size_t kMaxNumThreads = 64;

/// Dumps args in a format that can be extracted by an experiment script
void DumpArgs() {
  printf("> Args sync %s\n", FLAGS_sync.c_str());
  printf("> Args insert %d%%\n", FLAGS_insert_pct);
  printf("> Args delete %d%%\n", FLAGS_delete_pct);
  printf("> Args search %d%%\n", FLAGS_search_pct);
  printf("> Args initial_size %d\n", FLAGS_initial_size);
  std::cout << "> Args threads " << FLAGS_threads << std::endl;
  std::cout << "> Args seconds " << FLAGS_seconds << std::endl;
  std::cout << "> Args affinity " << FLAGS_affinity << std::endl;
  std::cout << "> Args read_heavy " << FLAGS_read_heavy << std::endl;
  std::cout << "> Args mwcas_desc_pool_size " << FLAGS_mwcas_desc_pool_size
      << std::endl;
#ifdef PMEM
  if(FLAGS_clflush) {
    printf("> Args using clflush\n");
  } else {
    std::cout << "> Args write_delay_ns " << FLAGS_write_delay_ns << std::endl;
    std::cout << "> Args emulate_write_bw " << FLAGS_emulate_write_bw
        << std::endl;
  }
#endif

  if(FLAGS_insert_pct + FLAGS_delete_pct + FLAGS_search_pct != 100) {
    LOG(FATAL) << "wrong operation mix";
  }
}

struct DllStats {
  uint64_t n_insert;
  uint64_t n_delete;
  uint64_t n_search;
  uint64_t n_effective_insert;
  uint64_t n_effective_delete;
  uint64_t n_effective_search;
  DllStats() : n_insert(0), n_delete(0), n_search(0), 
    n_effective_insert(0), n_effective_delete(0), n_effective_search(0) {}
  friend DllStats& operator+(DllStats left, const DllStats& right) {
    left += right;
    return left;
  }
  DllStats& operator+=(const DllStats& other) {
    n_insert += other.n_insert;
    n_delete += other.n_delete;
    n_search += other.n_search;
    n_effective_insert += other.n_effective_insert;
    n_effective_delete += other.n_effective_delete;
    n_effective_search += other.n_effective_search;
    return *this;
  }
  DllStats& operator-=(const DllStats& other) {
    n_insert -= other.n_insert;
    n_delete -= other.n_delete;
    n_search -= other.n_search;
    n_effective_insert -= other.n_effective_insert;
    n_effective_delete -= other.n_effective_delete;
    n_effective_search -= other.n_effective_search;
    return *this;
  }
};

struct DListBench : public Benchmark {
  DListBench()
    : Benchmark{}
    , cumulative_mwcas_stats {}
    , cumulative_dll_stats {} {
  }

  IDList* dll;
  MwCASMetrics cumulative_mwcas_stats;
  DllStats cumulative_dll_stats;
  CoreLocal<DllStats *> stats;
  uint32_t initial_local_insert;

  void Setup(size_t thread_count) {
    stats.Initialize();

    if(FLAGS_sync == "cas") {
      dll = new CASDList;
    } else if(FLAGS_sync == "pcas") {
#ifdef PMEM
      if(FLAGS_clflush) {
        NVRAM::InitializeClflush();
      } else {
        NVRAM::InitializeSpin(FLAGS_write_delay_ns, FLAGS_emulate_write_bw);
      }
      dll = new CASDList();
#else
      LOG(FATAL) << "PMEM undefined";
#endif
    } else if(FLAGS_sync == "mwcas") {
      DescriptorPool* pool = new DescriptorPool(
        FLAGS_mwcas_desc_pool_size, FLAGS_threads, nullptr);
      dll = new MwCASDList(pool);
    } else if(FLAGS_sync == "pmwcas") {
#ifdef PMEM
      Descriptor* pool_va = nullptr;
      if(FLAGS_clflush) {
        NVRAM::InitializeClflush();
      } else {
        NVRAM::InitializeSpin(FLAGS_write_delay_ns, FLAGS_emulate_write_bw);
      }
      DescriptorPool* pool = new DescriptorPool(
        FLAGS_mwcas_desc_pool_size, FLAGS_threads, nullptr);
      dll = new MwCASDList(pool);
#else
      LOG(FATAL) << "PMEM undefined";
#endif
    } else {
      LOG(FATAL) << "wrong sync method";
    }

    // Populate the list on behalf of each thread
    uint64_t thread_index = 0;
    uint64_t local_insert = 0;
    if(dll->GetSyncMethod() == IDList::kSyncMwCAS) {
      MwCASMetrics::ThreadInitialize();
    }
    int32_t inserted = 0;
    for(int32_t i = 0; i < FLAGS_initial_size; ++i) {
      if(dll->GetSyncMethod() == IDList::kSyncMwCAS) {
        ((MwCASDList*)dll)->GetEpoch()->Protect();
      }
      uint64_t payload_base = thread_index << 32;
      auto* node = IDList::NewNode(nullptr, nullptr, sizeof(uint64_t));
      uint64_t val = local_insert | payload_base;
      memcpy(node->GetPayload(), (char *)&val, sizeof(uint64_t));
      local_insert += ++thread_index % FLAGS_threads == 0 ? 1 : 0;
      thread_index %= FLAGS_threads;
      auto s = dll->InsertBefore(dll->GetTail(), node, false);
      RAW_CHECK(s.ok(), "loading failed");
      inserted++;
      if(inserted % 10000 == 0) {
        LOG(INFO) << "Inserted " << inserted;
      }
      if(dll->GetSyncMethod() == IDList::kSyncMwCAS) {
        ((MwCASDList*)dll)->GetEpoch()->Unprotect();
      }
    }
    initial_local_insert = local_insert;
    if(dll->GetSyncMethod() == IDList::kSyncMwCAS) {
      MwCASMetrics::Uninitialize();
      MwCASMetrics::Initialize();
    }
  }

  void Main(size_t thread_index) {
    DllStats *local_stats = new DllStats;
    *stats.MyObject() = local_stats;

    if(dll->GetSyncMethod() == IDList::kSyncMwCAS) {
      MwCASMetrics::ThreadInitialize();
    }

    // WARNING: do not change the way these four variables are added
    // unless you know what you are doing.
    uint32_t insert_pct = FLAGS_insert_pct;
    uint32_t delete_pct = insert_pct + FLAGS_delete_pct;
    uint32_t search_pct = delete_pct + FLAGS_search_pct;

    uint64_t payload_base = (uint64_t)thread_index << 32;
    RandomNumberGenerator rng{};

    const uint64_t kEpochThreshold = 1000;

    const uint64_t kPreallocNodes = 600000000 / FLAGS_threads;

#ifdef WIN32
    DListNode* nodes = (DListNode*)_aligned_malloc(
      (sizeof(DListNode) + sizeof(uint64_t)) * kPreallocNodes, kCacheLineSize);
#else
    DListNode* nodes = nullptr;
    int n = posix_memalign((void**)&nodes, kCacheLineSize,
        (sizeof(DListNode) + sizeof(uint64_t)) * kPreallocNodes);
#endif
    RAW_CHECK(nodes, "out of memory");

    uint64_t next_node = 0;

    WaitForStart();
    auto* node = dll->GetHead();
    bool mwcas = dll->GetSyncMethod() == IDList::kSyncMwCAS;

    DListCursor cursor((IDList*)dll);
    uint64_t epochs = 0;
    if(FLAGS_read_heavy) {
      while(!IsShutdown()) {
        cursor.Reset();
        if(mwcas && ++epochs == kEpochThreshold) {
          ((MwCASDList*)dll)->GetEpoch()->Unprotect();
          ((MwCASDList*)dll)->GetEpoch()->Protect();
          epochs = 0;
        }
        uint32_t op = rng.Generate(100);
        uint64_t range = FLAGS_read_heavy_modify_range;
        if(FLAGS_read_heavy_modify_range == 0) {
          // Find a random position
          // (est. total_insert = local_insert * number of threads)
          range = FLAGS_initial_size + 
              (local_stats->n_insert - local_stats->n_delete) * FLAGS_threads;
          range = std::max(range, (uint64_t)0);
        }
        int32_t pos = rng.Generate(range);
        if(op < insert_pct) {
          while(pos-- > 0) {
            node = cursor.Next();
            if(node == dll->GetTail()) {
              cursor.Reset();
            }
          }
          RAW_CHECK(node, "invalid node pointer");
          uint64_t val = (initial_local_insert + local_stats->n_insert) |
              payload_base;
          auto* new_node = &nodes[next_node++];
          RAW_CHECK(next_node < kPreallocNodes, "No more nodes");
          new(new_node) DListNode(nullptr, nullptr, sizeof(uint64_t));
          memcpy(new_node->GetPayload(), (char *)&val, sizeof(uint64_t));
          Status s;
          if (rng.Generate(2) == 0) {
            s = dll->InsertAfter(node, new_node, true);
          } else {
            s = dll->InsertBefore(node, new_node, true);
          }
          if(s.ok()) { ++local_stats->n_effective_insert; }
          ++local_stats->n_insert;
        } else {
          if(FLAGS_read_heavy_modify_range == 0) {
            uint32_t thread_index = rng.Generate(FLAGS_threads);
            uint32_t local_index = rng.Generate(
                initial_local_insert + local_stats->n_insert + 1);
            uint64_t expected_value = ((uint64_t)thread_index << 32) |
                local_index;

            auto* node = dll->GetNext(dll->GetHead());
            bool found = false;
            while(!found && node != dll->GetTail()) {
              if(expected_value == *(uint64_t*)node->GetPayload()) {
                found = true;
              } else {
                node = cursor.Next();
              }
            }
            if(op < delete_pct) {
              auto s = dll->Delete(node, true);
              ++local_stats->n_delete;
              if(s.ok()) { ++local_stats->n_effective_delete; }
            } else {
              ++local_stats->n_search;
              if(found) { ++local_stats->n_effective_search; }
            }
          } else {
            while(pos-- > 0) {
              node = cursor.Next();
              if(node == dll->GetTail()) {
                cursor.Reset();
              }
            }
            // Must be delete, search is not supported here
            RAW_CHECK(op < delete_pct,
                "search is not supported for the current setting");
            auto s = dll->Delete(node, true);
            ++local_stats->n_delete;
            if(s.ok()) { ++local_stats->n_effective_delete; }
          }
        }
      }
    } else {
      // This one resembles the original DLL paper's experiment
      while(!IsShutdown()) {
        if(mwcas && ++epochs == kEpochThreshold) {
          ((MwCASDList*)dll)->GetEpoch()->Unprotect();
          ((MwCASDList*)dll)->GetEpoch()->Protect();
          epochs = 0;
        }
        uint32_t op = rng.Generate(100);
        bool forward = true;
        if (op < insert_pct) {
          uint64_t val = (initial_local_insert + local_stats->n_insert) |
            payload_base;
          auto* new_node = &nodes[next_node++];
          RAW_CHECK(next_node < kPreallocNodes, "no more nodes");
          new(new_node) DListNode(nullptr, nullptr, sizeof(uint64_t));
          memcpy(new_node->GetPayload(), (char *)&val, sizeof(uint64_t));
          Status s;
          if (rng.Generate(2) == 0) {
            s = dll->InsertAfter(node, new_node, true);
          } else {
            s = dll->InsertBefore(node, new_node, true);
          }
          if(s.ok()) { ++local_stats->n_effective_insert; }
          ++local_stats->n_insert;
        } else if(op < delete_pct) {
          auto s = dll->Delete(node, true);
          ++local_stats->n_delete;
          if(s.ok()) { ++local_stats->n_effective_delete; }
        } else {
          // Search
          if(node == dll->GetTail()) {
            forward = false;
          } else if (node == dll->GetHead()) {
            forward = true;
          } else {
            uint64_t payload = *(uint64_t*)node->GetPayload();
          }
          if(forward) {
            node = cursor.Next();
          } else {
            node = cursor.Prev();
          }
          ++local_stats->n_search;
          ++local_stats->n_effective_search;
        }
      }
    }
  }

  virtual void Dump(size_t thread_count, uint64_t run_ticks,
                    uint64_t dump_id, bool final_dump) {
    MARK_UNREFERENCED(thread_count);
    Benchmark::Dump(thread_count, run_ticks, dump_id, final_dump);

    if(dll->GetSyncMethod() == IDList::kSyncMwCAS) {
      MwCASMetrics mstats;
      MwCASMetrics::Sum(mstats);
      if(!final_dump) {
        mstats -= cumulative_mwcas_stats;
        cumulative_mwcas_stats += mstats;
      }
      mstats.Print();
    }

    DllStats sum;
    SumDllStats(sum);
    if(final_dump) {
      std::cout << "> Benchmark " << dump_id << " InsertPerSecond " <<
        (double)sum.n_insert / FLAGS_seconds << std::endl;
      std::cout << "> Benchmark " << dump_id << " DeletePerSecond " <<
        (double)sum.n_delete / FLAGS_seconds << std::endl;
      std::cout << "> Benchmark " << dump_id << " SearchPerSecond " <<
        (double)sum.n_search / FLAGS_seconds << std::endl;
      std::cout << "> Benchmark " << dump_id << " EffectiveInsertPerSecond " <<
        (double)sum.n_effective_insert / FLAGS_seconds << std::endl;
      std::cout << "> Benchmark " << dump_id << " EffectiveDeletePerSecond " <<
        (double)sum.n_effective_delete / FLAGS_seconds << std::endl;
      std::cout << "> Benchmark " << dump_id << " EffectiveSearchPerSecond " <<
        (double)sum.n_effective_search / FLAGS_seconds << std::endl;
    } else {
      sum -= cumulative_dll_stats;
      cumulative_dll_stats += sum;
      std::cout << "> Benchmark " << dump_id << " Insert " <<
        sum.n_insert << std::endl;
      std::cout << "> Benchmark " << dump_id << " Delete " <<
        sum.n_delete << std::endl;
      std::cout << "> Benchmark " << dump_id << " Search " <<
        sum.n_search << std::endl;
      std::cout << "> Benchmark " << dump_id << " EffectiveInsert " <<
        sum.n_effective_insert << std::endl;
      std::cout << "> Benchmark " << dump_id << " EffectiveDelete " <<
        sum.n_effective_delete << std::endl;
      std::cout << "> Benchmark " << dump_id << " EffectiveSearch " <<
        sum.n_effective_search << std::endl;
    }
  }

  void Teardown() {
    stats.Uninitialize();
  }

  void SumDllStats(DllStats& sum) {
    for (uint32_t i = 0; i < stats.NumberOfObjects(); ++i) {
      auto* thread_metric = *stats.GetObject(i);
      sum += *thread_metric;
    }
  }

  uint64_t GetOperationCount() {
    if(dll->GetSyncMethod() == IDList::kSyncMwCAS) {
      MwCASMetrics metrics;
      MwCASMetrics::Sum(metrics);
      return metrics.GetUpdateAttemptCount();
    }
    return 0;
  }
};

}  // namespace pmwcas

int main(int argc, char* argv[]) {
  google::InitGoogleLogging(argv[0]);
  google::ParseCommandLineFlags(&argc, &argv, true);
#ifdef WIN32
  pmwcas::InitLibrary(pmwcas::DefaultAllocator::Create,
                           pmwcas::DefaultAllocator::Destroy,
                           pmwcas::WindowsEnvironment::Create,
                           pmwcas::WindowsEnvironment::Destroy);
#else
  pmwcas::InitLibrary(pmwcas::TlsAllocator::Create,
                           pmwcas::TlsAllocator::Destroy,
                           pmwcas::LinuxEnvironment::Create,
                           pmwcas::LinuxEnvironment::Destroy);
#endif
  pmwcas::DumpArgs();
  pmwcas::DListBench test{};
  test.Run(FLAGS_threads, FLAGS_seconds,
           static_cast<pmwcas::AffinityPattern>(FLAGS_affinity),
           FLAGS_metrics_dump_interval);
  return 0;
}
