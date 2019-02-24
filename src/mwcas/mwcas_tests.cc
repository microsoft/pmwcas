// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include <gtest/gtest.h>
#include <stdlib.h>
#include "common/allocator_internal.h"
#include "include/pmwcas.h"
#include "include/environment.h"
#include "include/allocator.h"
#include "include/status.h"
#include "util/random_number_generator.h"
#include "util/auto_ptr.h"
#include "mwcas/mwcas.h"
#ifdef WIN32
#include "environment/environment_windows.h"
#else
#include "environment/environment_linux.h"
#endif

namespace pmwcas {

typedef pmwcas::MwcTargetField<uint64_t> PMwCASPtr;

const uint32_t kDescriptorPoolSize = 0x400;
const uint32_t kTestArraySize = 0x80;
const uint32_t kWordsToUpdate = 4;
const std::string kSharedMemorySegmentName = "mwcastest";

GTEST_TEST(PMwCASTest, SingleThreadedUpdateSuccess) {
  auto thread_count = Environment::Get()->GetCoreCount();
  std::unique_ptr<pmwcas::DescriptorPool> pool(
    new pmwcas::DescriptorPool(kDescriptorPoolSize, thread_count));
  RandomNumberGenerator rng(rand(), 0, kTestArraySize);
  PMwCASPtr test_array[kTestArraySize];
  PMwCASPtr* addresses[kWordsToUpdate];
  uint64_t values[kWordsToUpdate];

  for (uint32_t i = 0; i < kWordsToUpdate; ++i) {
    test_array[i] = 0;
    addresses[i] = nullptr;
    values[i] = 0;
  }

  pool.get()->GetEpoch()->Protect();

  for (uint32_t i = 0; i < kWordsToUpdate; ++i) {
    retry:
    uint64_t idx = rng.Generate();
    for (uint32_t existing_entry = 0; existing_entry < i; ++existing_entry) {
      if (addresses[existing_entry] == reinterpret_cast<PMwCASPtr*>(
          &test_array[idx])) {
        goto retry;
      }
    }

    addresses[i] = reinterpret_cast<PMwCASPtr*>(&test_array[idx]);
    values[i] = test_array[idx].GetValueProtected();
  }

  Descriptor* descriptor = pool->AllocateDescriptor();
  EXPECT_NE(nullptr, descriptor);

  for (uint32_t i = 0; i < kWordsToUpdate; ++i) {
    descriptor->AddEntry((uint64_t*)addresses[i], values[i], 1ull);
  }

  EXPECT_TRUE(descriptor->MwCAS());

  for (uint32_t i = 0; i < kWordsToUpdate; ++i) {
    EXPECT_EQ(1ull, *((uint64_t*)addresses[i]));
  }

  pool.get()->GetEpoch()->Unprotect();
  Thread::ClearRegistry(true);
}

GTEST_TEST(PMwCASTest, SingleThreadedAbort) {
  auto thread_count = Environment::Get()->GetCoreCount();
  std::unique_ptr<pmwcas::DescriptorPool> pool(
    new pmwcas::DescriptorPool(kDescriptorPoolSize, thread_count));
  RandomNumberGenerator rng(rand(), 0, kTestArraySize);
  PMwCASPtr test_array[kTestArraySize];
  PMwCASPtr* addresses[kWordsToUpdate];

  for (uint32_t i = 0; i < kWordsToUpdate; ++i) {
    test_array[i] = 0;
    addresses[i] = nullptr;
  }

  for (uint32_t i = 0; i < kWordsToUpdate; ++i) {
    retry:
    uint64_t idx = rng.Generate();
    for (uint32_t existing_entry = 0; existing_entry < i; ++existing_entry) {
      if (addresses[existing_entry] == reinterpret_cast<PMwCASPtr*>(
          &test_array[idx])) {
        goto retry;
      }
    }

    addresses[i] = reinterpret_cast<PMwCASPtr*>(&test_array[idx]);
  }

  pool.get()->GetEpoch()->Protect();

  Descriptor* descriptor = pool->AllocateDescriptor();
  EXPECT_NE(nullptr, descriptor);

  for (uint32_t i = 0; i < kWordsToUpdate; ++i) {
    descriptor->AddEntry((uint64_t*)addresses[i], 0ull, 1ull);
  }

  EXPECT_TRUE(descriptor->Abort().ok());

  for (uint32_t i = 0; i < kWordsToUpdate; ++i) {
    EXPECT_EQ(0ull, *((uint64_t*)addresses[i]));
  }

  pool.get()->GetEpoch()->Unprotect();
  Thread::ClearRegistry(true);
}

GTEST_TEST(PMwCASTest, SingleThreadedConflict) {
  auto thread_count = Environment::Get()->GetCoreCount();
  std::unique_ptr<pmwcas::DescriptorPool> pool(
    new pmwcas::DescriptorPool(kDescriptorPoolSize, thread_count));
  RandomNumberGenerator rng(rand(), 0, kTestArraySize);
  PMwCASPtr test_array[kTestArraySize];
  PMwCASPtr* addresses[kWordsToUpdate];

  for (uint32_t i = 0; i < kTestArraySize; ++i) {
    test_array[i] = 0ull;
  }

  for (uint32_t i = 0; i < kWordsToUpdate; ++i) {
    addresses[i] = nullptr;
  }

  for (uint32_t i = 0; i < kWordsToUpdate; ++i) {
    retry:
    uint64_t idx = rng.Generate();
    for (uint32_t existing_entry = 0; existing_entry < i; ++existing_entry) {
      if (addresses[existing_entry] == reinterpret_cast<PMwCASPtr*>(
          &test_array[idx])) {
        goto retry;
      }
    }

    addresses[i] = reinterpret_cast<PMwCASPtr*>(&test_array[idx]);
  }

  pool.get()->GetEpoch()->Protect();

  Descriptor* descriptor = pool->AllocateDescriptor();
  EXPECT_NE(nullptr, descriptor);

  for (uint32_t i = 0; i < kWordsToUpdate; ++i) {
    descriptor->AddEntry((uint64_t*)addresses[i], 0ull, 1ull);
  }

  EXPECT_TRUE(descriptor->MwCAS());

  for (uint32_t i = 0; i < kWordsToUpdate; ++i) {
    EXPECT_EQ(1ull, *((uint64_t*)addresses[i]));
  }

  pool.get()->GetEpoch()->Unprotect();

  pool.get()->GetEpoch()->Protect();

  descriptor = pool->AllocateDescriptor();
  EXPECT_NE(nullptr, descriptor);

  for (uint32_t i = 0; i < kWordsToUpdate; ++i) {
    descriptor->AddEntry((uint64_t*)addresses[i], 0ull, 1ull);
  }

  EXPECT_FALSE(descriptor->MwCAS());

  pool.get()->GetEpoch()->Unprotect();
  Thread::ClearRegistry(true);
}

#ifdef PMEM
GTEST_TEST(PMwCASTest, SingleThreadedRecovery) {
  auto thread_count = Environment::Get()->GetCoreCount();
  RandomNumberGenerator rng(rand(), 0, kTestArraySize);
  PMwCASPtr* addresses[kWordsToUpdate];
  PMwCASPtr* test_array = nullptr;

  // Round shared segment (descriptor pool and test data) up to cacheline size
  uint64_t segment_size = sizeof(DescriptorPool) +
    sizeof(DescriptorPool::Metadata) +
    sizeof(Descriptor) * kDescriptorPoolSize +
    sizeof(PMwCASPtr) * kTestArraySize;

  // Create a shared memory segment. This will serve as our test area for
  // the first PMwCAS process that "fails" and will be re-attached to a new
  // descriptor pool and recovered.
  unique_ptr_t<char> memory_segment = alloc_unique<char>(segment_size);
  ::memset(memory_segment.get(), 0, segment_size);
  void* segment_raw = memory_segment.get();

  // Record descriptor count and the initial virtual address of the shm space
  // for recovery later
  DescriptorPool::Metadata *metadata = (DescriptorPool::Metadata*)segment_raw;
  metadata->descriptor_count = kDescriptorPoolSize;
  metadata->initial_address = (uintptr_t)segment_raw;

  DescriptorPool *pool = (DescriptorPool*)(segment_raw + sizeof(DescriptorPool::Metadata));

  test_array = (PMwCASPtr*)((uintptr_t)segment_raw + sizeof(DescriptorPool) +
    sizeof(DescriptorPool::Metadata) +
    sizeof(Descriptor) * kDescriptorPoolSize);

  // Create a new descriptor pool using an existing memory block, which will
  // be reused by new descriptor pools that will recover from whatever is in the
  // pool from previous runs.
  new (pool) DescriptorPool(kDescriptorPoolSize, thread_count, false);

  for (uint32_t i = 0; i < kTestArraySize; ++i) {
    test_array[i] = 0ull;
  }

  for (uint32_t i = 0; i < kWordsToUpdate; ++i) {
    addresses[i] = nullptr;
  }

  for (uint32_t i = 0; i < kWordsToUpdate; ++i) {
  retry:
    uint64_t idx = rng.Generate();
    for (uint32_t existing_entry = 0; existing_entry < i; ++existing_entry) {
      if (addresses[existing_entry] == reinterpret_cast<PMwCASPtr*>(
          &test_array[idx])) {
        goto retry;
      }
    }
    addresses[i] = reinterpret_cast<PMwCASPtr*>(&test_array[idx]);
  }
  
  pool->GetEpoch()->Protect();

  Descriptor* descriptor = pool->AllocateDescriptor();
  EXPECT_NE(nullptr, descriptor);

  for (uint32_t i = 0; i < kWordsToUpdate; ++i) {
    descriptor->AddEntry((uint64_t*)addresses[i], 0ull, 1ull);
  }

  EXPECT_TRUE(descriptor->MwCAS());

  for (uint32_t i = 0; i < kWordsToUpdate; ++i) {
    EXPECT_EQ(1ull, *((uint64_t*)addresses[i]));
  }

  pool->GetEpoch()->Unprotect();
  Thread::ClearRegistry(true);

  // Create a fresh descriptor pool from the previous pools existing memory.
  pool->Recovery(false);

  // The prior MwCAS succeeded, so check whether the pool recovered correctly
  // by ensuring the updates are still present.
  for (uint32_t i = 0; i < kWordsToUpdate; ++i) {
    EXPECT_EQ(1ull, *((uint64_t*)addresses[i]));
  }

  pool->GetEpoch()->Protect();

  descriptor = pool->AllocateDescriptor();
  EXPECT_NE(nullptr, descriptor);

  for (uint32_t i = 0; i < kWordsToUpdate; ++i) {
    descriptor->AddEntry((uint64_t*)addresses[i], 1ull, 2ull);
  }

  EXPECT_FALSE(descriptor->MwCASWithFailure());

  Thread::ClearRegistry(true);

  pool->Recovery(false);
  // Recovery should have rolled back the previously failed pmwcas.
  for (uint32_t i = 0; i < kWordsToUpdate; ++i) {
    EXPECT_EQ(1ull, *((uint64_t*)addresses[i]));
  }

  pool->GetEpoch()->Protect();

  descriptor = pool->AllocateDescriptor();
  EXPECT_NE(nullptr, descriptor);

  for (uint32_t i = 0; i < kWordsToUpdate; ++i) {
    descriptor->AddEntry((uint64_t*)addresses[i], 1ull, 2ull);
  }

  EXPECT_FALSE(descriptor->MwCASWithFailure(0, true));

  Thread::ClearRegistry(true);

  pool->Recovery(false);

  // Recovery should have rolled forward the previously failed pmwcas that made
  // it through the first phase (installing all descriptors).
  for (uint32_t i = 0; i < kWordsToUpdate; ++i) {
    EXPECT_EQ(2ull, *((uint64_t*)addresses[i]));
  }

  Thread::ClearRegistry(true);
}
#endif

} // namespace pmwcas

int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  FLAGS_minloglevel = 2;

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

  return RUN_ALL_TESTS();
}
