// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include <memory>
#include <gtest/gtest.h>
#include "util/performance_test.h"
#include "include/status.h"
#include "include/allocator.h"
#include "include/pmwcas.h"
#include "common/epoch.h"
#include "common/garbage_list.h"
#ifdef WIN32
#include "environment/environment_windows.h"
#else
#include "environment/environment_linux.h"
#endif

namespace pmwcas {
namespace test {

struct MockItem {
 public:
  MockItem()
    : deallocations{ 0 } {
  }

  static void Destroy(void* destroyContext, void* p) {
    ++(reinterpret_cast<MockItem*>(p))->deallocations;
  }

  std::atomic<uint64_t> deallocations;
};

class GarbageListTest : public ::testing::Test {
 public:
  GarbageListTest() {}

 protected:
  EpochManager epoch_manager_;
  GarbageList garbage_list_;
  std::stringstream out_;

  virtual void SetUp() {
    out_.str("");
    ASSERT_TRUE(epoch_manager_.Initialize().ok());
    ASSERT_TRUE(garbage_list_.Initialize(&epoch_manager_, 1024).ok());
  }

  virtual void TearDown() {
    EXPECT_TRUE(garbage_list_.Uninitialize().ok());
    EXPECT_TRUE(epoch_manager_.Uninitialize().ok());
    Thread::ClearRegistry(true);
  }
};

struct GarbageListSmokeTest : public PerformanceTest {
  GarbageListSmokeTest(IGarbageList* garbage_list, MockItem* mock_item,
                       uint64_t item_count)
    : PerformanceTest{}
    , garbage_list_{ garbage_list }
    , mock_item_{ mock_item }
    , iterations_{ item_count } {
  }

  void Teardown() {
  }

  void Entry(size_t thread_index) {
    WaitForStart();
    for(uint64_t index = 0; index < iterations_; ++index) {
      EXPECT_TRUE(garbage_list_->Push(mock_item_, MockItem::Destroy,
          nullptr).ok());
    }
  }

  IGarbageList* garbage_list_;
  MockItem* mock_item_;
  uint64_t iterations_;
};

TEST_F(GarbageListTest, Uninitialize) {
  MockItem items[2];

  EXPECT_TRUE(garbage_list_.Push(&items[0], MockItem::Destroy, nullptr).ok());
  EXPECT_TRUE(garbage_list_.Push(&items[1], MockItem::Destroy, nullptr).ok());
  EXPECT_TRUE(garbage_list_.Uninitialize().ok());
  EXPECT_EQ(1, items[0].deallocations);
  EXPECT_EQ(1, items[1].deallocations);
}

TEST_F(GarbageListTest, Smoke) {
  uint64_t iterations = 1 << 15;
  MockItem item{};
  GarbageListSmokeTest test{ static_cast<IGarbageList*>(&garbage_list_), &item,
      iterations };

  size_t core_count = Environment::Get()->GetCoreCount();
  test.Run(core_count);

  EXPECT_TRUE(garbage_list_.Uninitialize().ok());
  EXPECT_EQ((core_count*iterations), item.deallocations);
}

} // namespace test
} // namespace pmwcas

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
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
