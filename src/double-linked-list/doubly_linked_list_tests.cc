// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include <vector>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include "common/allocator_internal.h"
#include "doubly_linked_list.h"
#include "include/pmwcas.h"
#include "include/environment.h"
#include "include/allocator.h"
#include "include/status.h"
#include "util/random_number_generator.h"
#include "util/performance_test.h"
#ifdef WIN32
#include "environment/environment_windows.h"
#else
#include "environment/environment_linux.h"
#endif

uint32_t descriptor_pool_size = 100000;

namespace pmwcas {
namespace test {

struct SingleThreadTest {
  IDList* dll;
  std::vector<DListNode*> nodes;
  const uint32_t kInitialNodes = 1000;

  SingleThreadTest(IDList* list) : dll(list) {
    Initialize();
  }

  void Initialize() {
    if(dll->GetSyncMethod() == IDList::kSyncMwCAS) {
      MwCASMetrics::ThreadInitialize();
    }
    ASSERT_TRUE(nodes.size() == 0);
    ASSERT_TRUE(dll->GetHead()->prev == nullptr);
    ASSERT_TRUE(dll->GetHead()->next == dll->GetTail());
    ASSERT_TRUE(dll->GetTail()->prev == dll->GetHead());
    ASSERT_TRUE(dll->GetTail()->next == nullptr);

    for(int i = 0; i < kInitialNodes; ++i) {
      auto* n = new DListNode;
      nodes.push_back(n);
      auto s = dll->InsertAfter(dll->GetTail(), n, false);
      ASSERT_TRUE(s.ok());
    }
    ASSERT_TRUE(nodes.size() == kInitialNodes);
    for(uint32_t i = 0; i < nodes.size(); i++) {
      if(dll->GetSyncMethod() == IDList::kSyncCAS) {
        ASSERT_FALSE(CASDList::MarkedNext(nodes[i]));
        ASSERT_FALSE(CASDList::MarkedPrev(nodes[i]));
      }
    }
    SanityCheck();
  }

  ~SingleThreadTest() {
    Teardown();
  }

  void Teardown() {
    SanityCheck();
    for(uint32_t i = 0; i < nodes.size(); i++) {
      delete nodes[i];
    }
    Thread::ClearRegistry();
  }

  void SanityCheck() {
    if(nodes.size() == 0) {   // TestDelete empties node the vector
      return;
    }
    bool found_head = false, found_tail = false;
    for(auto& n : nodes) {
      ASSERT_TRUE(n->prev && n->next);
      auto* next = dll->GetNext(n);
      if(next) {
        ASSERT_TRUE(dll->GetPrev(next) == n);
        if(n->prev == dll->GetHead()) {
          ASSERT_FALSE(found_head);
          found_head = true;
        }
      }
      auto* prev = dll->GetPrev(n);
      if(prev) {
        ASSERT_TRUE(prev->next == n);
        if(n->next == dll->GetTail()) {
          ASSERT_FALSE(found_tail);
          found_tail = true;
        }
      }
    }
    ASSERT_TRUE(found_head);
    ASSERT_TRUE(found_tail);

    uint32_t n = 0;
    DListCursor iter((IDList*)dll);
    while(iter.Next() != dll->GetTail()) {
      n++;
    }
    EXPECT_EQ(n, nodes.size());
    dll->SingleThreadSanityCheck();
  }

  void TestInsert() {
    RandomNumberGenerator rng(1234567);
    // Insert some new nodes at random locations
    const int kInserts = 1000;
    for(int i = 0; i < kInserts; i++) {
      int idx = rng.Generate(nodes.size());
      ASSERT_TRUE(idx >= 0 && idx < nodes.size());
      DListNode* n = new DListNode;
      if(rng.Generate(2) == 0) {
        auto s = dll->InsertBefore(nodes[idx], n, false);
        ASSERT_TRUE(s.ok());
      } else {
        auto s = dll->InsertAfter(nodes[idx], n, false);
        ASSERT_TRUE(s.ok());
      }
      nodes.push_back(n);
    }
    ASSERT_TRUE(nodes.size() == kInitialNodes + kInserts);
  }

  void TestDelete() {
    RandomNumberGenerator rng(1234567);
    // Delete some nodes at random locations
    while(nodes.size()) {
      int idx = rng.Generate(nodes.size());
      ASSERT_TRUE(idx >= 0 && idx < nodes.size());
      dll->Delete(nodes[idx], false);
      delete nodes[idx];
      nodes.erase(nodes.begin() + idx);
    }
    ASSERT_TRUE(nodes.size() == 0);
  }

  void TestInsertDelete() {
    RandomNumberGenerator rng(1234567);
    const int kInsertPct = 50;
    const int kDeletePct = 100 - kInsertPct;
    const int kOps = 1000;

    for(int i = 0; i < kOps; i++) {
      if(nodes.size() && rng.Generate(100) >= kDeletePct) {
        int idx = rng.Generate(nodes.size());
        ASSERT_TRUE(nodes[idx]->prev);
        ASSERT_TRUE(nodes[idx]->next);
        ASSERT_TRUE(dll->GetNext(dll->GetPrev(nodes[idx])) == dll->GetPrev(
            dll->GetNext(nodes[idx])));
        ASSERT_TRUE(dll->GetNext(dll->GetPrev(nodes[idx])) == nodes[idx]);
        auto s = dll->Delete(nodes[idx], false);
        ASSERT_TRUE(s.ok());
        DListNode* prev = nodes[idx]->prev;
        DListNode* next = nodes[idx]->next;
        if(dll->GetSyncMethod() == IDList::kSyncCAS) {
          ASSERT_TRUE(nodes[idx]->prev);
          ASSERT_TRUE(nodes[idx]->next);
          prev = ((CASDList*)dll)->DereferenceNodePointer(&(nodes[idx]->prev));
          next = ((CASDList*)dll)->DereferenceNodePointer(&(nodes[idx]->next));
          ASSERT_TRUE(dll->GetNext(prev) == next);
          ASSERT_TRUE(dll->GetPrev(next) == prev);
        }
        delete nodes[idx];
        nodes.erase(nodes.begin() + idx);
      } else {
        int idx = rng.Generate(nodes.size());
        ASSERT_TRUE(idx >= 0 && idx < nodes.size());
        DListNode* n = new DListNode(nullptr, nullptr, 0);
        if(rng.Generate(2) == 0) {
          auto s = dll->InsertBefore(nodes[idx], n, false);
          ASSERT_TRUE(s.ok());
          ASSERT_TRUE(dll->GetPrev(nodes[idx]) == n);
          ASSERT_TRUE(n->next == nodes[idx]);
        } else {
          auto s = dll->InsertAfter(nodes[idx], n, false);
          ASSERT_TRUE(s.ok());
          ASSERT_TRUE(nodes[idx]->next == n);
          ASSERT_TRUE(n->prev == nodes[idx]);
        }
        nodes.push_back(n);
      }
    }
  }
};

struct MultiThreadInsertDeleteTest : public PerformanceTest {
  static const int kInitialNodes = 1000;
  std::vector<DListNode*> nodes;
  std::atomic<int32_t> node_count;
  IDList* dll;
  Barrier barrier;

  MultiThreadInsertDeleteTest(IDList* list, uint32_t thread_count)
    : PerformanceTest(), node_count(0), dll(list), barrier{ thread_count } {}

  void Entry(size_t thread_index) {
    if(dll->GetSyncMethod() == IDList::kSyncMwCAS) {
      MwCASMetrics::ThreadInitialize();
    }

    if(thread_index == 0) {
      RandomNumberGenerator rng(1234567);
      for(int i = 0; i < kInitialNodes; ++i) {
        nodes.push_back(new DListNode());
        dll->InsertAfter(dll->GetHead(), nodes[i], false);
        node_count++;
      }
      dll->SingleThreadSanityCheck();
    }

    const int kOps = 1000;
    RandomNumberGenerator rng(1234567);
    int next_insert_idx = 0;
    int done_ops = 0;
    std::vector<DListNode*> my_nodes;
    for(int i = 0; i < kOps; ++i) {   // Lazy, this is more than enough
      my_nodes.push_back(new DListNode());
    }
    WaitForStart();

    while(done_ops++ != kOps) {
      int32_t nc = node_count.load(std::memory_order_seq_cst);
      uint8_t ops = 3;  // insert-before/after/delete
      if(nc <= 0) {
        ops--; // exclude delete
      }
      ASSERT_TRUE(ops == 2 || ops == 3);
      DListNode* target = dll->GetHead();
      int idx = rng.Generate(nc);
      DListCursor iter((IDList*)dll);
      while(--idx > 0 && target) {
        target = iter.Next();
      }
      if(!target) {
        continue;
      }
      int op = rng.Generate(ops);
      if(op == 0) {
        auto s = dll->InsertBefore(target, my_nodes[next_insert_idx++], false);
        ASSERT_TRUE(s.ok());
        node_count++;
      } else if(op == 1) {
        auto s = dll->InsertAfter(target, my_nodes[next_insert_idx++], false);
        ASSERT_TRUE(s.ok());
        node_count++;
      } else {
        auto s = dll->Delete(target, false);
        ASSERT_TRUE(s.ok());
        if(target != dll->GetHead() && target != dll->GetTail()) {
          node_count--;
        }
      }
    }

    barrier.CountAndWait();
    if(thread_index == 0) {
      dll->SingleThreadSanityCheck();
    }
  }
};

struct MultiThreadDeleteTest : public PerformanceTest {
  static const int kInitialNodes = 1000;
  std::vector<DListNode*> nodes;
  IDList* dll;
  Barrier barrier;

  MultiThreadDeleteTest(IDList* list, uint32_t thread_count)
    : PerformanceTest(), dll(list), barrier { thread_count } {}

  void Entry(size_t thread_index) {
    if(dll->GetSyncMethod() == IDList::kSyncMwCAS) {
      MwCASMetrics::ThreadInitialize();
    }
    if(thread_index == 0) {
      RandomNumberGenerator rng(1234567);
      for(int i = 0; i < kInitialNodes; ++i) {
        nodes.push_back(new DListNode());
        dll->InsertAfter(dll->GetHead(), nodes[i], false);
      }
    }

    RandomNumberGenerator rng(1234567);
    int deletes = 200;
    WaitForStart();
    while(deletes-- > 0 && dll->GetHead()->next != dll->GetTail()) {
      int idx = rng.Generate(kInitialNodes);
      DListNode* node = nodes[idx];
      ASSERT_TRUE(node);
      ASSERT_TRUE(node != dll->GetHead());
      ASSERT_TRUE(node != dll->GetTail());
      dll->Delete(node, false);
    }

    barrier.CountAndWait();
    if(thread_index == 0) {
      dll->SingleThreadSanityCheck();
      for(int i = 0; i < kInitialNodes; ++i) {
        delete nodes[i];
      }
    }
  }
};

struct MultiThreadInsertTest : public PerformanceTest {
  std::atomic<uint32_t> insert_count;
  IDList* dll;
  Barrier barrier;

  MultiThreadInsertTest(IDList* list, uint32_t thread_count)
    : PerformanceTest(), insert_count(0), dll(list), barrier { thread_count } {}

  void Entry(size_t thread_index) {
    if(dll->GetSyncMethod() == IDList::kSyncMwCAS) {
      MwCASMetrics::ThreadInitialize();
    }
    RandomNumberGenerator rng(1234567);
    const int kInserts = 1000;
    std::vector<DListNode*> thread_nodes;
    // Populate my vector of nodes to be inserted
    for(int i = 0; i < kInserts; ++i) {
      thread_nodes.push_back(new DListNode());
    }
    for(int i = 0; i < kInserts; ++i) {
      ASSERT_TRUE(((int64_t)thread_nodes[i] & 0x3) == 0);
    }
    WaitForStart();

    for(int i = 0; i < kInserts; ++i) {
      // Pick a neighbor: the dummy head if dll is empty, otherwise
      // try a random guy in the middle of the list.
      DListNode* neighbor = dll->GetHead();
      if(insert_count > 0) {
        int idx = rng.Generate(insert_count);
        DListCursor iter((IDList*)dll);
        do {
          neighbor = iter.Next();
        } while(--idx > 0);
        ASSERT_TRUE(neighbor);
      }
      if(rng.Generate(2) == 0) {
        auto s = dll->InsertBefore(neighbor, thread_nodes[i], false);
        ASSERT_TRUE(s.ok());
      } else {
        auto s = dll->InsertAfter(neighbor, thread_nodes[i], false);
        ASSERT_TRUE(s.ok());
      }
      ++insert_count;
    }

    barrier.CountAndWait();
    if(thread_index == 0) {
      dll->SingleThreadSanityCheck();
      uint32_t n = 0;
      auto* prev = dll->GetHead();
      DListCursor iter((IDList*)dll);
      // Check the insert count too
      while(true) {
        auto* node = iter.Next();
        ASSERT_TRUE(dll->GetPrev(node) == prev);
        ASSERT_TRUE(prev->next == node);
        if(node == dll->GetTail()) {
          break;
        }
        n++;
        prev = node;
      }
      EXPECT_EQ(n, insert_count);
    }
  }
};

GTEST_TEST(DListTest, CASDSingleThreadInsert) {
  CASDList dll;
  SingleThreadTest t(&dll);
  t.TestInsert();
}

GTEST_TEST(DListTest, CASDSingleThreadDelete) {
  CASDList dll;
  SingleThreadTest t(&dll);
  t.TestDelete();
}

GTEST_TEST(DListTest, CASDSingleThreadInsertDelete) {
  CASDList dll;
  SingleThreadTest t(&dll);
  t.TestInsertDelete();
}

GTEST_TEST(DListTest, CASDMultiThreadInsert) {
  CASDList dll;
  auto thread_count = Environment::Get()->GetCoreCount();
  MultiThreadInsertTest test(&dll, thread_count);
  test.Run(thread_count);
}

GTEST_TEST(DListTest, CASDMultiThreadDelete) {
  CASDList dll;
  auto thread_count = Environment::Get()->GetCoreCount();
  MultiThreadDeleteTest test(&dll, thread_count);
  test.Run(thread_count);
}

GTEST_TEST(DListTest, CASDMultiThreadInsertDelete) {
  CASDList dll;
  auto thread_count = Environment::Get()->GetCoreCount();
  MultiThreadInsertDeleteTest test(&dll, thread_count);
  test.Run(thread_count);
}

GTEST_TEST(DListTest, MwCASSingleThreadInsert) {
  auto thread_count = Environment::Get()->GetCoreCount();
  std::unique_ptr<pmwcas::DescriptorPool> pool(
      new pmwcas::DescriptorPool(descriptor_pool_size, thread_count));
  std::unique_ptr<MwCASDList> dll(new MwCASDList(pool.get()));
  std::unique_ptr<SingleThreadTest> t (new SingleThreadTest(dll.get()));
  t->TestInsert();
}

GTEST_TEST(DListTest, MwCASSingleThreadDelete) {
  auto thread_count = Environment::Get()->GetCoreCount();
  std::unique_ptr<pmwcas::DescriptorPool> pool(
      new pmwcas::DescriptorPool(descriptor_pool_size, thread_count));
  std::unique_ptr<MwCASDList> dll(new MwCASDList(pool.get()));
  std::unique_ptr<SingleThreadTest> t (new SingleThreadTest(dll.get()));
  t->TestDelete();
}

GTEST_TEST(DListTest, MwCASSingleThreadInsertDelete) {
  auto thread_count = Environment::Get()->GetCoreCount();
  std::unique_ptr<pmwcas::DescriptorPool> pool(
      new pmwcas::DescriptorPool(descriptor_pool_size, thread_count));
  std::unique_ptr<MwCASDList> dll(new MwCASDList(pool.get()));
  std::unique_ptr<SingleThreadTest> t (new SingleThreadTest(dll.get()));
  t->TestInsertDelete();
}

GTEST_TEST(DListTest, MwCASMultiThreadDelete) {
  auto thread_count = Environment::Get()->GetCoreCount();
  std::unique_ptr<pmwcas::DescriptorPool> pool(
      new pmwcas::DescriptorPool(descriptor_pool_size, thread_count));
  std::unique_ptr<MwCASDList> dll(new MwCASDList(pool.get()));
  MultiThreadDeleteTest* t = new MultiThreadDeleteTest(dll.get(), thread_count);
  t->Run(thread_count);
}

GTEST_TEST(DListTest, MwCASMultiThreadInsert) {
  auto thread_count = Environment::Get()->GetCoreCount();
  std::unique_ptr<pmwcas::DescriptorPool> pool(
      new pmwcas::DescriptorPool(descriptor_pool_size, thread_count));
  std::unique_ptr<MwCASDList> dll(new MwCASDList(pool.get()));
  MultiThreadInsertTest* t = new MultiThreadInsertTest(dll.get(), thread_count);
  t->Run(thread_count);
}

GTEST_TEST(DListTest, MwCASMultiThreadInsertDelete) {
  auto thread_count = Environment::Get()->GetCoreCount();
  std::unique_ptr<pmwcas::DescriptorPool> pool(
      new pmwcas::DescriptorPool(descriptor_pool_size, thread_count));
  std::unique_ptr<MwCASDList> dll(new MwCASDList(pool.get()));
  MultiThreadInsertDeleteTest* t = new MultiThreadInsertDeleteTest(
      dll.get(), thread_count);
  t->Run(thread_count);
}

} // namespace test
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
#ifdef PMDK
  pmwcas::InitLibrary(pmwcas::PMDKAllocator::Create("doubly_linked_test_pool",
                                                    "doubly_linked_layout",
                                                    static_cast<uint64_t >(1024) * 1024 * 1204 * 5),
                      pmwcas::PMDKAllocator::Destroy,
                      pmwcas::LinuxEnvironment::Create,
                      pmwcas::LinuxEnvironment::Destroy);
#else
  pmwcas::InitLibrary(pmwcas::TlsAllocator::Create,
                           pmwcas::TlsAllocator::Destroy,
                           pmwcas::LinuxEnvironment::Create,
                           pmwcas::LinuxEnvironment::Destroy);
#endif  // PMDK
#endif  // WIN32

  return RUN_ALL_TESTS();
}
