// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once
#include "common/allocator_internal.h"
#include "mwcas/mwcas.h"
#include "util/atomics.h"
#include "util/macros.h"
#include "util/random_number_generator.h"

namespace pmwcas {

struct DListNode {
  const static int kCacheLineSize = 64;

  // Put prev and next in the same cacheline so that a single NVRAM::Flush is
  // enough.
  DListNode* prev;  // 8-byte
  DListNode* next;  // 8-byte
  uint32_t payload_size;  // 4-byte
  char padding[kCacheLineSize - sizeof(DListNode*) * 2 - sizeof(uint32_t)];

  DListNode(DListNode* p, DListNode* n, uint32_t s)
      : prev(p),
        next(n),
        payload_size(s) {
  }

  DListNode()
      : DListNode(nullptr, nullptr, 0) {
  }

  inline char* GetPayload() {
    return (char *)this + sizeof(*this);
  }
};

class IDList {
 public:
  static DListNode* NewNode(DListNode* prev, DListNode* next,
      uint32_t payload_size) {
    DListNode *node = nullptr;
    Allocator::Get()->Allocate((void **) &node,
                               sizeof(DListNode) + payload_size);
    new (node) DListNode(prev, next, payload_size);
    return node;
  }

  static const int kSyncUnknown = 0;
  static const int kSyncMwCAS = 1;
  static const int kSyncCAS = 2;

  IDList(int sync)
      : head_(nullptr, &tail_, 0),
        tail_(&head_, nullptr, 0),
        sync_method_(sync) {
  }

  ~IDList() {}

  /// Insert [node] right before [next]
  virtual Status InsertBefore(DListNode* next, DListNode* node,
      bool already_protected) = 0;

  /// Insert [node] right after [prev]
  virtual Status InsertAfter(DListNode* prev, DListNode* node,
      bool already_protected) = 0;

  /// Delete [node]
  virtual Status Delete(DListNode* node, bool already_protected) = 0;

  /// Figure out the node lined up after [node]
  virtual DListNode* GetNext(DListNode* node,
      bool already_protected = false) = 0;

  virtual DListNode* GetPrev(DListNode* node,
      bool already_protected = false) = 0;

  inline DListNode* GetHead() { return &head_; }

  inline DListNode* GetTail() { return &tail_; }

  inline int GetSyncMethod() { return sync_method_; }

  /// Verify the links between each pair of nodes (including head and tail).
  /// For single-threaded cases only, no CC whatsoever.
  void SingleThreadSanityCheck();

 protected:
  DListNode head_;
  DListNode tail_;
  int sync_method_;
};

/// A lock-free doubly linked list using single-word CAS,
/// based off of the following paper:
/// Håkan Sundell and Philippas Tsigas. 2008.
/// Lock-free deques and doubly linked lists.
/// J. Parallel Distrib. Comput. 68, 7 (July 2008), 1008-1020.
class CASDList : public IDList {
 private:
  /// MSB of the next pointer to indicate the underlying node is logically
  /// deleted.
  static const uint64_t kNodeDeleted = ((uint64_t)1 << 60);

  /// An RNG for back off
  RandomNumberGenerator rng{};

  inline void backoff() {
    uint64_t loops = rng.Generate(500);
    while(loops--) {}
  };

 public:
#ifdef PMEM
  /// The same dirty bit as in persistent MwCAS.
  static const uint64_t kDirtyFlag  = Descriptor::kDirtyFlag;
#endif

  CASDList() : IDList(kSyncCAS), rng(__rdtsc(), 0, 500) {}

  /// Insert [node] in front of [next] - [node] might end up before another node
  /// in case [prev] is being deleted or due to concurrent insertions at the
  /// same spot.
  virtual Status InsertBefore(DListNode* next, DListNode* node,
      bool already_protected = false) override;

  /// Similar to InsertBefore, but try to insert [node] after [prev].
  virtual Status InsertAfter(DListNode* prev, DListNode* node,
      bool already_protected = false) override;

  /// Delete [node]
  virtual Status Delete(DListNode* node,
      bool already_protected = false) override;

  virtual DListNode* GetNext(DListNode* node,
      bool already_protected = false) override;

  virtual DListNode* GetPrev(DListNode* node, bool) override;

  /// Set the deleted bit on the given node
  inline void MarkNodePointer(DListNode** node) {
#ifdef PMEM
    uint64_t flags = kNodeDeleted | kDirtyFlag;
#else
    uint64_t flags = kNodeDeleted;
#endif

    while (true) {
      DListNode* node_ptr = *node;
      RAW_CHECK(node != &head_.next, "cannot mark head node's next pointer");
      if (((uint64_t)node_ptr & kNodeDeleted) ||
          node_ptr == CompareExchange64Ptr(node,
            (DListNode*)((uint64_t)node_ptr | flags), node_ptr)) {
        break;
      }
    }
  }

  inline DListNode* ReadPersist(DListNode** node) {
    auto* node_ptr = *node;

#ifdef PMEM
    if(((uint64_t)node_ptr & kDirtyFlag)) {
      NVRAM::Flush(sizeof(uint64_t), node);
      CompareExchange64Ptr(node,
          (DListNode*)((uint64_t)node_ptr & ~kDirtyFlag), node_ptr);
    }
    return (DListNode*)((uint64_t)node_ptr & ~kDirtyFlag);
#else
    return node_ptr;
#endif
  }

  /// Extract the real underlying node (masking out the MSB and flush if needed)
  inline DListNode* DereferenceNodePointer(DListNode** node) {
    DListNode* ret = nullptr;
#ifdef PMEM
    ret = ReadPersist(node);
#else
    ret = *node;
#endif
    return (DListNode*)((uint64_t)ret & ~kNodeDeleted);
  }

  inline static bool MarkedNext(DListNode* node) {
    return (uint64_t)node->next & kNodeDeleted;
  }

  inline static bool MarkedPrev(DListNode* node) {
    return (uint64_t)node->prev & kNodeDeleted;
  }

 private:
  DListNode* CorrectPrev(DListNode* prev, DListNode* node);
};

/// A lock-free doubly-linked list using multi-word CAS
class MwCASDList : public IDList {
 private:
  DescriptorPool* descriptor_pool_;

  /// MSB of the next pointer to indicate the underlying node is logically
  /// deleted.
  static const uint64_t kNodeDeleted = ((uint64_t)1 << 60);

 public:
  MwCASDList(DescriptorPool* pool) :
    IDList(kSyncMwCAS), descriptor_pool_(pool) {}

  /// Insert [node] in front of [next] - [node] might end up before another node
  /// in case [prev] is being deleted or due to concurrent insertions at the
  /// same spot.
  virtual Status InsertBefore(DListNode* next, DListNode* node,
      bool already_protected) override;

  /// Similar to InsertBefore, but try to insert [node] after [prev].
  virtual Status InsertAfter(DListNode* prev, DListNode* node,
      bool already_protected) override;

  /// Delete [node]
  virtual Status Delete(DListNode* node, bool already_protected) override;

  inline virtual DListNode* GetNext(DListNode* node,
      bool already_protected = false) override {
    node = (DListNode*)((uint64_t)node & ~kNodeDeleted);
    DListNode* next = ResolveNodePointer(&node->next, already_protected);
    return (DListNode*)((uint64_t)next & ~kNodeDeleted);
  }

  inline virtual DListNode* GetPrev(DListNode* node,
      bool already_protected = false) override {
    node = (DListNode*)((uint64_t)node & ~kNodeDeleted);
    DListNode* prev = ResolveNodePointer(&node->prev, already_protected);
    return (DListNode*)((uint64_t)prev & ~kNodeDeleted);
  }

  inline EpochManager* GetEpoch() {
    return descriptor_pool_->GetEpoch();
  }

 private:
  /// Return a stable value using MwCAS's GetValue().
  /// Note: [node] must be a pointer to a pointer to a node,
  /// where it denotes the node pointer's real location, as
  /// we're casting it to an MwCAS field. E.g., it cannot be
  /// the address of a pointer parameter passed in to the caller.
  inline DListNode* ResolveNodePointer(DListNode** node, bool already_protected) {
    int64_t stable_ptr = 0;
    if(already_protected) {
      stable_ptr = ((MwcTargetField<uint64_t> *)node)->GetValueProtected();
    } else {
      stable_ptr = ((MwcTargetField<uint64_t> *)node)->GetValue(
          descriptor_pool_->GetEpoch());
    }

    return (DListNode*)stable_ptr;
  }
};

class DListCursor {
 private:
  IDList* list_;
  DListNode* current_node_;
  bool unprot;

 public:
  DListCursor(IDList* list) :
    list_(list), current_node_(list->GetHead()) {
    if(list->GetSyncMethod() == IDList::kSyncMwCAS) {
      auto* epoch = ((MwCASDList*)list_)->GetEpoch();
      unprot = !epoch->IsProtected();
      if(unprot) {
        epoch->Protect();
      }
    }
  }

  ~DListCursor() {
    if(unprot && list_->GetSyncMethod() == IDList::kSyncMwCAS) {
      ((MwCASDList*)list_)->GetEpoch()->Unprotect();
    }
  }

  inline void Reset() {
    current_node_ = list_->GetHead();
  }

  inline DListNode* Next() {
    current_node_ = list_->GetNext(current_node_, true);
    return current_node_;
  }

  inline DListNode* Prev() {
    current_node_ = list_->GetPrev(current_node_, true);
    return current_node_;
  }
};
}  // namespace pmwcas
