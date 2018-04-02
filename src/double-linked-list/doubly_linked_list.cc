// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include "doubly_linked_list.h"
#include "glog/raw_logging.h"

namespace pmwcas {

void IDList::SingleThreadSanityCheck() {
  RAW_CHECK(head_.prev == nullptr, "head.prev doesn't point to null");
  RAW_CHECK(head_.next, "head.next is null");
  RAW_CHECK(tail_.prev, "tail.prev is null");
  RAW_CHECK(tail_.next == nullptr, "tail.next doesn't point to null");

#ifdef PMEM
  head_.next = (DListNode*)((uint64_t)head_.next & ~CASDList::kDirtyFlag);
#endif
  DListNode* node = head_.next;
  DListNode* prev = &head_;
  int i = 0;
  do {
#ifdef PMEM
    node->next = (DListNode*)((uint64_t)node->next & ~CASDList::kDirtyFlag);
    node->prev = (DListNode*)((uint64_t)node->prev & ~CASDList::kDirtyFlag);
#endif
    RAW_CHECK(node, "null dlist node");
    RAW_CHECK(prev->next == node, "node.prev doesn't match prev.next");
    RAW_CHECK(node->prev == prev, "node.prev doesn't match prev.next");
    prev = node;
    node = node->next;
  } while (node && node->next != &tail_);
}

DListNode* CASDList::GetNext(DListNode* node, bool) {
  while (node != &tail_) {
    RAW_CHECK(node, "null current node");
    auto* next = DereferenceNodePointer(&node->next);
    RAW_CHECK(next, "null next pointer in list");
#ifdef PMEM
    RAW_CHECK(!((uint64_t)next & kDirtyFlag), "dirty pointer");
    auto* next_next = ReadPersist(&next->next);
#else
    auto* next_next = next->next;
#endif
    if ((uint64_t)next_next & kNodeDeleted) {
      // The next pointer of the node behind me has the deleted mark set
#ifdef PMEM
      auto* node_next = ReadPersist(&node->next);
#else
      auto* node_next = node->next;
#endif
      if ((uint64_t)node_next != ((uint64_t)next | kNodeDeleted)) {
        // But my next pointer isn't pointing the next with the deleted bit set,
        // so we set the deleted bit in next's prev pointer.
        MarkNodePointer(&next->prev);
        // Now try to unlink the deleted next node
#ifdef PMEM
        next_next = (DListNode*)((uint64_t)next_next | kDirtyFlag);
#endif
        CompareExchange64Ptr(&node->next, (DListNode*)((uint64_t)next_next &
            ~kNodeDeleted), next);
        continue;
      }
    }
    node = next;
    if(((uint64_t)next_next & kNodeDeleted) == 0) {
      return next;
    }
  }
  return nullptr;  // nothing after tail
}

DListNode* CASDList::GetPrev(DListNode* node, bool) {
  while(node != &head_) {
    RAW_CHECK(node, "null current node");
    auto* prev = DereferenceNodePointer(&node->prev);
    RAW_CHECK(prev, "null prev pointer in list");
#ifdef PMEM
    RAW_CHECK(!((uint64_t)prev & kDirtyFlag), "dirty pointer");
    auto* prev_next = ReadPersist(&prev->next);
    auto* next = ReadPersist(&node->next);
#else
    auto* prev_next = prev->next;
    auto* next = node->next;
#endif

    if(prev_next == node && ((uint64_t)next & kNodeDeleted) == 0) {
      return prev;
    } else if((uint64_t)next & kNodeDeleted) {
      node = GetNext(node);
    } else {
      prev = CorrectPrev(prev, node);
    }
  }
  return nullptr;
}

Status CASDList::InsertBefore(DListNode* next, DListNode* node, bool) {
  RAW_CHECK(!((uint64_t)next & kNodeDeleted), "invalid next pointer state");
#ifdef PMEM
  RAW_CHECK(!((uint64_t)node & kDirtyFlag), "dirty node pointer");
  RAW_CHECK(!((uint64_t)next & kDirtyFlag), "dirty next pointer");
#endif

  if (next == &head_) {
    return InsertAfter(next, node);
  }

  DListNode* prev = nullptr;
  // Persist the payload
#ifdef PMEM
  NVRAM::Flush(node->payload_size, node->GetPayload());
#endif

  while (true) {
    prev = DereferenceNodePointer(&next->prev);
    // If the guy supposed to be behind me got deleted, fast
    // forward to its next node and retry
#ifdef PMEM
    auto* next_next = ReadPersist(&next->next);
#else
    auto* next_next = next->next;
#endif
    if ((uint64_t)next_next & kNodeDeleted) {
      next = GetNext(next);
#ifdef PMEM
      RAW_CHECK(!((uint64_t)next & kDirtyFlag), "dirty next pointer");
#endif
      prev = CorrectPrev(prev, next);  // using the new next
#ifdef PMEM
      RAW_CHECK(!((uint64_t)prev & kDirtyFlag), "dirty prev pointer");
#endif
      continue;
    }

    node->prev = (DListNode*)((uint64_t)prev & ~kNodeDeleted);
    node->next = (DListNode*)((uint64_t)next & ~kNodeDeleted);
#ifdef PMEM
    // Flush node.prev and node.next before installing
    NVRAM::Flush(sizeof(node->prev) + sizeof(node->next), &node->prev);
#endif

    // Install [node] on prev->next
    DListNode* expected = (DListNode*)((uint64_t)next & ~kNodeDeleted);
    if (expected == CompareExchange64Ptr(&prev->next, node, expected)) {
      break;
    }
    // Failed, get a new hopefully-correct prev
    prev = CorrectPrev(prev, next);
    backoff();
  }
  RAW_CHECK(prev, "invalid prev pointer");
  CorrectPrev(prev, next);
  return Status::OK();
}

Status CASDList::InsertAfter(DListNode* prev, DListNode* node, bool) {
  RAW_CHECK(!((uint64_t)prev & kNodeDeleted), "invalid prev pointer state");
#ifdef PMEM
  RAW_CHECK(!((uint64_t)node & kDirtyFlag), "dirty node pointer");
  RAW_CHECK(!((uint64_t)prev & kDirtyFlag), "dirty next pointer");
#endif

  if (prev == &tail_) {
    return InsertBefore(prev, node);
  }

  DListNode* prev_next = nullptr;
  // Persist the payload
#ifdef PMEM
  NVRAM::Flush(node->payload_size, node->GetPayload());
#endif

  while (true) {
#ifdef PMEM
    prev_next = ReadPersist(&prev->next);
#else
    prev_next = prev->next;
#endif
    node->prev = (DListNode*)((uint64_t)prev & ~kNodeDeleted);
    node->next = (DListNode*)((uint64_t)prev_next & ~kNodeDeleted);
#ifdef PMEM
    // Flush node.prev and node.next before installing
    NVRAM::Flush(sizeof(node->prev) + sizeof(node->next), &node->prev);
#endif

    // Install [node] after [next]
    DListNode* expected = (DListNode*)((uint64_t)prev_next & ~kNodeDeleted);
    if (expected == CompareExchange64Ptr(&prev->next, node, expected)) {
      break;
    }

    if ((uint64_t)prev_next & kNodeDeleted) {
      Delete(node);
      return InsertBefore(prev, node);
    }

    backoff();
  }
  RAW_CHECK(prev_next, "invalid prev_next pointer");
  CorrectPrev(prev, prev_next);
  return Status::OK();
}

Status CASDList::Delete(DListNode* node, bool) {
#ifdef PMEM
  RAW_CHECK(!((uint64_t)node & kDirtyFlag), "dirty node pointer");
#endif
  if (node == &head_ || node == &tail_) {
    RAW_CHECK(((uint64_t)node->next & kNodeDeleted) == 0,
        "invalid next pointer");
    RAW_CHECK(((uint64_t)node->prev & kNodeDeleted) == 0,
        "invalid next pointer");

    return Status::OK();
  }
  while (true) {
#ifdef PMEM
    DListNode* node_next = ReadPersist(&node->next);
#else
    auto* node_next = node->next;
#endif
    if ((uint64_t)node_next & kNodeDeleted) {
      return Status::OK();
    }

    // Try to set the deleted bit in node->next
#ifdef PMEM
    auto* desired =
      (DListNode*)((uint64_t)node_next | kNodeDeleted | kDirtyFlag);
#else
    auto* desired =
      (DListNode*)((uint64_t)node_next | kNodeDeleted);
#endif
    DListNode* rnode = CompareExchange64Ptr(
      &node->next, desired,
      node_next);
    if (rnode == node_next) {
      DListNode* node_prev = nullptr;
      while (true) {
#ifdef PMEM
        node_prev = ReadPersist(&node->prev);
#else
        node_prev = node->prev;
#endif
        if ((uint64_t)node_prev & kNodeDeleted) {
          break;
        }
#ifdef PMEM
        auto* desired =
          (DListNode*)((uint64_t)node_prev | kNodeDeleted | kDirtyFlag);
#else
        auto* desired =
          (DListNode*)((uint64_t)node_prev | kNodeDeleted);
#endif

        if (node_prev == CompareExchange64Ptr(
          &node->prev, desired,
          node_prev)) {
          break;
        }
      }
      RAW_CHECK(node_prev, "invalid node_prev pointer");
      RAW_CHECK(((uint64_t)head_.next & kNodeDeleted) == 0,
          "invalid next pointer");

      CorrectPrev((DListNode*)((uint64_t)node_prev & ~kNodeDeleted), node_next);

      return Status::OK();
    }
  }
}

DListNode* CASDList::CorrectPrev(DListNode* prev, DListNode* node) {
  RAW_CHECK(((uint64_t)node & kNodeDeleted) == 0, "node has deleted mark");
#ifdef PMEM
  RAW_CHECK(!((uint64_t)node & kDirtyFlag), "dirty node pointer");
#endif
  DListNode* last_link = nullptr;
  while (true) {
#ifdef PMEM
    auto* link1 = ReadPersist(&node->prev);
#else
    auto* link1 = node->prev;
#endif
    if ((uint64_t)link1 & kNodeDeleted) {
      break;
    }

    DListNode* prev_cleared = (DListNode*)((uint64_t)prev & ~kNodeDeleted);
#ifdef PMEM
    auto* prev_next = ReadPersist(&prev_cleared->next);
#else
    auto* prev_next = prev_cleared->next;
#endif
    if ((uint64_t)prev_next & kNodeDeleted) {
      if (last_link) {
        MarkNodePointer(&prev_cleared->prev);
#ifdef PMEM
        auto* desired =
          (DListNode*)(((uint64_t)prev_next & ~kNodeDeleted) | kDirtyFlag);
#else
        auto* desired = (DListNode*)(((uint64_t)prev_next & ~kNodeDeleted));
#endif
        CompareExchange64Ptr(&last_link->next, desired, prev);
        prev = last_link;
        last_link = nullptr;
        continue;
      }
#ifdef PMEM
      prev_next = ReadPersist(&prev_cleared->prev);
#else
      prev_next = prev_cleared->prev;
#endif
      prev = prev_next;
      RAW_CHECK(prev, "invalid prev pointer");
      continue;
    }

    RAW_CHECK(((uint64_t)prev_next & kNodeDeleted) == 0,
        "invalid next field in predecessor");

    if (prev_next != node) {
      last_link = prev_cleared;
      prev = prev_next;
      continue;
    }

#ifdef PMEM
    DListNode* p = (DListNode*)(((uint64_t)prev & ~kNodeDeleted) | kDirtyFlag);
#else
    DListNode* p = (DListNode*)(((uint64_t)prev & ~kNodeDeleted));
#endif
    if (link1 == CompareExchange64Ptr(&node->prev, p, link1)) {
#ifdef PMEM
      auto* prev_cleared_prev = ReadPersist(&prev_cleared->prev);
#else
      auto* prev_cleared_prev = prev_cleared->prev;
#endif
      if ((uint64_t)prev_cleared_prev & kNodeDeleted) {
        continue;
      }
      break;
    }
    backoff();
  }
#ifdef PMEM
  RAW_CHECK(!((uint64_t)prev & kDirtyFlag), "dirty prev pointer");
#endif
  return prev;
}

Status MwCASDList::InsertBefore(DListNode* next, DListNode* node,
    bool already_protected) {
  if (next == &head_) {
    return InsertAfter(next, node, already_protected);
  }
  EpochGuard guard(GetEpoch(), !already_protected);

  // Persist the payload
#ifdef PMEM
  NVRAM::Flush(node->payload_size, node->GetPayload());
#endif

  while (true) {
    RAW_CHECK(((uint64_t)next & kNodeDeleted) == 0, "invalid prev pointer");
    auto* prev = ResolveNodePointer(&next->prev, true);

    auto* next_next = ResolveNodePointer(&next->next, true);
    if (((uint64_t)prev & kNodeDeleted) || (uint64_t)next_next & kNodeDeleted) {
      next = GetNext(next, true);
      continue;
    }

    auto* prev_next = ResolveNodePointer(&prev->next, true);
    if ((uint64_t)prev_next & kNodeDeleted) {
      continue;
    }

    node->next = next;
    node->prev = prev;
#ifdef PMEM
    // Persist node
    NVRAM::Flush(sizeof(node->prev) + sizeof(node->next), &node->prev);
#endif
    RAW_CHECK(MwcTargetField<uint64_t>::IsCleanPtr((uint64_t)prev),
        "prev is not normal value");
    RAW_CHECK(MwcTargetField<uint64_t>::IsCleanPtr((uint64_t)next),
        "next is not normal value");

    auto* desc = descriptor_pool_->AllocateDescriptor();
    RAW_CHECK(desc, "null MwCAS descriptor");
    desc->AddEntry(
        (uint64_t*)&node->prev->next, (uint64_t)node->next, (uint64_t)node);
    desc->AddEntry(
        (uint64_t*)&next->prev, (uint64_t)node->prev, (uint64_t)node);
    if(desc->MwCAS()) {
      return Status::OK();
    }
  }

  return Status::OK();
}

Status MwCASDList::InsertAfter(DListNode* prev, DListNode* node,
    bool already_protected) {
  if (prev == &tail_) {
    return InsertBefore(prev, node, already_protected);
  }
  EpochGuard guard(GetEpoch(), !already_protected);

  // Persist the payload
#ifdef PMEM
  NVRAM::Flush(node->payload_size, node->GetPayload());
#endif

  while (true) {
    RAW_CHECK(((uint64_t)prev & kNodeDeleted) == 0, "invalid prev pointer");
    auto* next = ResolveNodePointer(&prev->next, true);
    if ((uint64_t)next & kNodeDeleted) {
      prev = GetPrev(prev, true);
      continue;
    }
    auto* next_next = ResolveNodePointer(&next->next, true);
    if ((uint64_t)next_next & kNodeDeleted) {
      continue;
    }

    RAW_CHECK(((uint64_t)next & kNodeDeleted) == 0, "deleted prev node");
    RAW_CHECK(MwcTargetField<uint64_t>::IsCleanPtr((uint64_t)prev),
        "next is not normal value");
    RAW_CHECK(MwcTargetField<uint64_t>::IsCleanPtr((uint64_t)next),
        "next is not normal value");

    node->prev = prev;
    node->next = next;
#ifdef PMEM
    // Persist node
    NVRAM::Flush(sizeof(node->prev) + sizeof(node->next), &node->prev);
#endif
    auto* desc = descriptor_pool_->AllocateDescriptor();
    RAW_CHECK(desc, "null descriptor pointer");

    desc->AddEntry((uint64_t*)&prev->next, (uint64_t)node->next,
        (uint64_t)node);
    desc->AddEntry((uint64_t*)&node->next->prev, (uint64_t)node->prev,
        (uint64_t)node);

    if(desc->MwCAS()) {
      return Status::OK();
    }
  }
  return Status::OK();
}

Status MwCASDList::Delete(DListNode* node, bool already_protected) {
  EpochGuard guard(GetEpoch(), !already_protected);
  while (((uint64_t)node->next & kNodeDeleted) == 0) {
    if (node == &head_ || node == &tail_) {
      RAW_CHECK(((uint64_t)node->next & kNodeDeleted) == 0,
          "invalid next pointer");
      RAW_CHECK(((uint64_t)node->prev & kNodeDeleted) == 0,
          "invalid next pointer");
      break;
    }
    DListNode* prev = ResolveNodePointer(&node->prev, true);
    DListNode* next = ResolveNodePointer(&node->next, true);
    if (prev->next != node || next->prev != node) {
      continue;
    }

    auto* desc = descriptor_pool_->AllocateDescriptor();
    RAW_CHECK(desc, "null MwCAS descriptor");

    desc->AddEntry((uint64_t*)&prev->next, (uint64_t)node, (uint64_t)next);
    desc->AddEntry((uint64_t*)&next->prev, (uint64_t)node, (uint64_t)prev);
    desc->AddEntry((uint64_t*)&node->next, (uint64_t)next,
        (uint64_t)next | kNodeDeleted);
    if(desc->MwCAS()) {
      return Status::OK();
    }
  }

  return Status::OK();
}
}  // namespace pmwcas
