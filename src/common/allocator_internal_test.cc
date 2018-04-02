// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include <gtest/gtest.h>
#include "include/status.h"
#include "include/allocator.h"
#include "include/pmwcas.h"
#include "include/environment.h"
#include "common/allocator_internal.h"
#include "common/garbage_list.h"
#include "util/random_number_generator.h"
#include "util/auto_ptr.h"

namespace pmwcas {
namespace test {

class AllocatorTest : public ::testing::Test {
 public:
  const uint64_t kAllocationCount = 1000;

 protected:
  virtual void SetUp() {}

  virtual void TearDown() { Thread::ClearRegistry(true); }
};

#ifdef WIN32
TEST_F(AllocatorTest, Alloc__Free) {
  RandomNumberGenerator rng;
  unique_ptr_t<void*> allocations_guard = alloc_unique<void*>(
      kAllocationCount * sizeof(void*));
  void** allocations = allocations_guard.get();

  for(uint64_t i = 0; i < kAllocationCount; ++i) {
    allocations[i] = nullptr;
  }

#ifdef _DEBUG
  uint64_t allocation_count =
    static_cast<DefaultAllocator*>(Allocator::Get())->GetTotalAllocationCount();
#endif

  for(uint64_t i = 0; i < kAllocationCount; ++i) {
    uint64_t size = rng.Generate(200);
    allocations[i] = Allocator::Get()->Allocate(size);
    ASSERT_NE(nullptr, allocations[i]);
  }

  for(uint64_t idx = 0; idx < 1000; ++idx) {
    Allocator::Get()->Free(allocations[idx]);
  }

#ifdef _DEBUG
  ASSERT_EQ(allocation_count,
      static_cast<DefaultAllocator*>(
       Allocator::Get())->GetTotalAllocationCount());
#endif
}

TEST_F(AllocatorTest, AllocAligned__FreeAligned) {
  RandomNumberGenerator rng;
  unique_ptr_t<void*> allocations_guard = alloc_unique<void*>(
      kAllocationCount * sizeof(void*));
  void** allocations = allocations_guard.get();

  for(uint64_t i = 0; i < kAllocationCount; ++i) {
    allocations[i] = nullptr;
  }

#ifdef _DEBUG
  uint64_t allocation_count =
    static_cast<DefaultAllocator*>(Allocator::Get())->GetTotalAllocationCount();
#endif

  for(uint64_t i = 0; i < kAllocationCount; ++i) {
    uint64_t size = rng.Generate(200);
    uint64_t alignment = std::exp2(rng.Generate(8));
    allocations[i] = Allocator::Get()->AllocateAligned(size, alignment);
    ASSERT_NE(nullptr, allocations[i]);
    ASSERT_EQ((size_t)0, reinterpret_cast<size_t>(allocations[i]) % alignment);
  }

  for(uint64_t idx = 0; idx < 1000; ++idx) {
    Allocator::Get()->FreeAligned(allocations[idx]);
  }

#ifdef _DEBUG
  ASSERT_EQ(allocation_count,
      static_cast<DefaultAllocator*>(
      Allocator::Get())->GetTotalAllocationCount());
#endif
}

TEST_F(AllocatorTest, AllocAlignedOffset__FreeAligned) {
  RandomNumberGenerator rng;
  unique_ptr_t<void*> allocations_guard = alloc_unique<void*>(
      kAllocationCount * sizeof(void*));
  void** allocations = allocations_guard.get();

  for(uint64_t i = 0; i < kAllocationCount; ++i) {
    allocations[i] = nullptr;
  }

#ifdef _DEBUG
  uint64_t allocation_count =
    static_cast<DefaultAllocator*>(Allocator::Get())->GetTotalAllocationCount();
#endif

  for(uint64_t i = 0; i < kAllocationCount; ++i) {
    uint64_t size = rng.Generate(200);
    uint64_t alignment = std::exp2(rng.Generate(8));
    uint64_t offset = size != 0 ? rng.Generate(size) : 0;

    // _aligned_malloc() requires allocation size >= alignment.
    allocations[i] = Allocator::Get()->AllocateAlignedOffset(size, alignment,
        offset);
    ASSERT_NE(nullptr, allocations[i]);
    ASSERT_EQ((size_t)0,
        (reinterpret_cast<size_t>(allocations[i]) + offset) % alignment);
  }

  for(uint64_t idx = 0; idx < 1000; ++idx) {
    Allocator::Get()->FreeAligned(allocations[idx]);
  }

#ifdef _DEBUG
  ASSERT_EQ(allocation_count,
      static_cast<DefaultAllocator*>(
      Allocator::Get())->GetTotalAllocationCount());
#endif
}
#endif

} // namespace test
} // namespace pmwcas

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
#ifdef WIN32
  pmwcas::InitLibrary(pmwcas::DefaultAllocator::Create,
                           pmwcas::DefaultAllocator::Destroy);
#endif
  return RUN_ALL_TESTS();
}
