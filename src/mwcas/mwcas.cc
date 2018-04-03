// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#ifdef WIN32
#include <Windows.h>
#undef ERROR // Avoid collision of ERROR definition in Windows.h with glog
#endif
#include "glog/logging.h"
#include "glog/raw_logging.h"
#include "include/pmwcas.h"
#include "mwcas/mwcas.h"
#include "util/atomics.h"

namespace pmwcas {

bool MwCASMetrics::enabled = false;
CoreLocal<MwCASMetrics*> MwCASMetrics::instance;

DescriptorPartition::DescriptorPartition(EpochManager* epoch,
    DescriptorPool* pool)
    : desc_pool(pool) {
  free_list = nullptr;
  garbage_list = (GarbageListUnsafe*)Allocator::Get()->Allocate(
        sizeof(GarbageListUnsafe));
  new(garbage_list) GarbageListUnsafe;
  auto s = garbage_list->Initialize(epoch);
  RAW_CHECK(s.ok(), "garbage list initialization failure");
}

DescriptorPartition::~DescriptorPartition() {
    garbage_list->Uninitialize();
}

DescriptorPool::DescriptorPool(
    uint32_t pool_size, uint32_t partition_count, Descriptor* desc_va,
    bool enable_stats)
    : pool_size_(pool_size),
      descriptors_(desc_va),
      partition_count_(partition_count),
      partition_table_(nullptr),
      next_partition_(0) {

  MwCASMetrics::enabled = enable_stats;

  auto s = MwCASMetrics::Initialize();
  RAW_CHECK(s.ok(), "failed initializing metric objects");

  s = epoch_.Initialize();
  RAW_CHECK(s.ok(), "epoch initialization failure");

  // Round up pool size to the nearest power of 2
  auto pool_size_requested = pool_size_;
  pool_size_ = 1;
  while (true) {
    if(pool_size_requested <= pool_size_) {
      break;
    }
    pool_size_ *= 2;
  }

  // Round partitions to a power of two but no higher than 1024
  auto requested = partition_count_;
  partition_count_ = 1;
  for(uint32_t exp = 1; exp < 10; exp++) {
    if(requested <= partition_count_) {
      break;
    }
    partition_count_ *= 2;
  }

  partition_table_ = (DescriptorPartition*)Allocator::Get()->AllocateAligned(
    sizeof(DescriptorPartition)*partition_count_, kCacheLineSize);
  RAW_CHECK(nullptr != partition_table_, "out of memory");

  for(uint32_t i = 0; i < partition_count_; ++i) {
    new(&partition_table_[i]) DescriptorPartition(&epoch_, this);
  }

  // If a pool area is provided, recover from it. Otherwise create a new one.
  // A new descriptor pool area always comes zeroed.
  RAW_CHECK(pool_size_ > 0, "invalid pool size");

  if(descriptors_) {
    Metadata *metadata = (Metadata*)((uint64_t)descriptors_ - sizeof(Metadata));
    RAW_CHECK((uint64_t)metadata->initial_address == (uint64_t)metadata,
              "invalid initial address");
    RAW_CHECK(metadata->descriptor_count == pool_size_,
              "wrong descriptor pool size");

    // If it is an existing pool, see if it has anything in it
    uint64_t in_progress_desc = 0, redo_words = 0, undo_words = 0;
    if(descriptors_[0].status_ != Descriptor::kStatusInvalid) {

      // Must not be a new pool which comes with everything zeroed
      for(uint32_t i = 0; i < pool_size_; ++i) {
        auto& desc = descriptors_[i];

        if(desc.status_ == Descriptor::kStatusInvalid) {
          // Must be a new pool - comes with everything zeroed but better
          // find this as we look at the first descriptor.
          RAW_CHECK(i == 0, "corrupted descriptor pool/data area");
          break;
        }

        desc.assert_valid_status();

        // Otherwise do recovery 
        uint32_t status = desc.status_ & ~Descriptor::kStatusDirtyFlag;
        if(status == Descriptor::kStatusFinished) {
          continue;
        } else if(status == Descriptor::kStatusUndecided ||
                  status == Descriptor::kStatusFailed) {
          in_progress_desc++;
          for(int w = 0; w < desc.count_; ++w) {
            auto& word = desc.words_[w];
            uint64_t val = Descriptor::CleanPtr(*word.address_);

            if(val == (uint64_t)&desc || val == (uint64_t)&word) {
              // If it's a CondCAS descriptor, then MwCAS descriptor wasn't
              // installed/persisted, i.e., new value (succeeded) or old value
              // (failed) wasn't installed on the field. If it's an MwCAS
              // descriptor, then the final value didn't make it to the field
              // (status is Undecided). In both cases we should roll back to old
              // value.
              *word.address_ = word.old_value_;
#ifdef PMEM
              word.PersistAddress();
#endif
              undo_words++;
              LOG(INFO) << "Applied old value 0x" << std::hex
                        << word.old_value_ << " at 0x" << word.address_;
            }
          }
        } else {
          RAW_CHECK(status == Descriptor::kStatusSucceeded, "invalid status");
          in_progress_desc++;

          for(int w = 0; w < desc.count_; ++w) {
            auto& word = desc.words_[w];

            uint64_t val = Descriptor::CleanPtr(*word.address_);
            RAW_CHECK(val != (uint64_t)&word, "invalid field value");

            if(val == (uint64_t)&desc) {
              *word.address_ = word.new_value_;
#ifdef PMEM
              word.PersistAddress();
#endif
              redo_words++;
              LOG(INFO) << "Applied new value 0x" << std::hex
                        << word.new_value_ << " at 0x" << word.address_;
            }
          }
        }

        for(int w = 0; w < desc.count_; ++w) {
          int64_t val = *desc.words_[w].address_;

          RAW_CHECK((val & ~Descriptor::kDirtyFlag) !=
              ((int64_t)&desc | Descriptor::kMwCASFlag),
              "invalid word value");
          RAW_CHECK((val & ~Descriptor::kDirtyFlag) !=
                ((int64_t)&desc | Descriptor::kCondCASFlag),
                "invalid word value");
        }
      }

      LOG(INFO) << "Found " << in_progress_desc <<
        " in-progress descriptors, rolled forward " << redo_words <<
        " words, rolled back " << undo_words << " words";
    }
  } else {
    // No existing pool space provided, create one, but won't support recovery
    descriptors_ = (Descriptor*)Allocator::Get()->AllocateAligned(
        sizeof(Descriptor) * pool_size_, kCacheLineSize);
    RAW_CHECK(descriptors_, "out of memory");
  }

  // (Re-)initialize descriptors. Any recovery business should be done by now,
  // start as a clean slate.
  RAW_CHECK(descriptors_, "null descriptor pool");
  memset(descriptors_, 0, sizeof(Descriptor) * pool_size_);

  // Distribute this many descriptors per partition
  RAW_CHECK(pool_size_ > partition_count_,
      "provided pool size is less than partition count");
  uint32_t desc_per_partition = pool_size_ / partition_count_;

  uint32_t partition = 0;
  for(uint32_t i = 0; i < pool_size_; ++i) {
    auto* desc = descriptors_ + i;
    DescriptorPartition* p = partition_table_ + partition;
    new(desc) Descriptor(p);
    desc->next_ptr_ = p->free_list;
    p->free_list = desc;

    if((i + 1) % desc_per_partition == 0) {
      partition++;
    }
  }
}

DescriptorPool::~DescriptorPool() {
  MwCASMetrics::Uninitialize();
}

Descriptor* DescriptorPool::AllocateDescriptor(Descriptor::AllocateCallback ac,
    Descriptor::FreeCallback fc) {
  thread_local DescriptorPartition* tls_part = nullptr;
  if(!tls_part) {
    // Sometimes e.g., benchmark data loading will create new threads when
    // starting real work. So % partition_count_ here is safe. This is so far
    // the only safe case allowed.
    auto index = next_partition_.fetch_add(1, std::memory_order_seq_cst) %
      partition_count_;

    // TODO: Currently we actually require strictly TLS partitions - there is no
    // CC whatsoever for partitions.
    tls_part = &partition_table_[index];

    // Track the partition pointer handed out to this thread.
    Thread::RegisterTls((uint64_t*)&tls_part, (uint64_t)nullptr);
  }

  Descriptor* desc = tls_part->free_list;
  while(!desc) {
    // See if we can scavenge some descriptors from the garbage list
    tls_part->garbage_list->GetEpoch()->BumpCurrentEpoch();
    tls_part->garbage_list->Scavenge();
    desc = tls_part->free_list;
    MwCASMetrics::AddDescriptorScavenge();
  }
  tls_part->free_list = desc->next_ptr_;

  MwCASMetrics::AddDescriptorAlloc();
  RAW_CHECK(desc, "null descriptor pointer");
  desc->allocate_callback_ = ac ? ac : Descriptor::DefaultAllocateCallback;
  desc->free_callback_ = fc ? fc : Descriptor::DefaultFreeCallback;
  return desc;
}

Descriptor::Descriptor(DescriptorPartition* partition) 
    : owner_partition_(partition) {
  Initialize();
}

inline void Descriptor::Initialize() {
  status_ = kStatusFinished;
  count_ = 0;
  next_ptr_ = nullptr;
  memset(words_, 0, sizeof(WordDescriptor) * kMaxCount);
}

void* Descriptor::DefaultAllocateCallback(size_t size) {
  return Allocator::Get()->AllocateAligned(size, kCacheLineSize);
}

void Descriptor::DefaultFreeCallback(void* context, void* p) {
  Allocator::Get()->FreeAligned(p);
}

uint32_t Descriptor::AddEntry(uint64_t* addr, uint64_t oldval, uint64_t newval,
      uint32_t recycle_policy) {
  // IsProtected() checks are quite expensive, use DCHECK instead of RAW_CHECK.
  DCHECK(owner_partition_->garbage_list->GetEpoch()->IsProtected());
  DCHECK(IsCleanPtr(oldval));
  DCHECK(IsCleanPtr(newval) || newval == kNewValueReserved);
  int insertpos = GetInsertPosition(addr);
  RAW_CHECK(insertpos >= 0, "invalid insert position");
  words_[insertpos].address_ = addr;
  words_[insertpos].old_value_ = oldval;
  words_[insertpos].new_value_ = newval;
  words_[insertpos].status_address_ = &status_;
  words_[insertpos].recycle_policy_ = recycle_policy;
  ++count_;
  return insertpos;
}

uint32_t Descriptor::AllocateAndAddEntry(uint64_t* addr, uint64_t oldval,
      size_t size, uint32_t recycle_policy) {
  // IsProtected() checks are quite expensive, use DCHECK instead of RAW_CHECK.
  DCHECK(owner_partition_->garbage_list->GetEpoch()->IsProtected());
  DCHECK(IsCleanPtr(oldval));
  int insertpos = GetInsertPosition(addr);
  RAW_CHECK(insertpos >= 0, "invalid insert position");
  words_[insertpos].address_ = addr;
  words_[insertpos].old_value_ = oldval;
  words_[insertpos].new_value_ = (uint64_t)allocate_callback_(size);
  RAW_CHECK(words_[insertpos].new_value_, "allocation failed");
  words_[insertpos].status_address_ = &status_;
  words_[insertpos].recycle_policy_ = recycle_policy;
  ++count_;
  return insertpos;
}

inline int Descriptor::GetInsertPosition(uint64_t* addr) {
  DCHECK(uint64_t(addr) % sizeof(uint64_t) == 0);
  RAW_CHECK(count_ < kMaxCount, "too many words");

  int insertpos = count_;
  for(int i = count_ - 1; i >= 0; i--) {
    if(words_[i].address_ == addr) {
      // Can't allow duplicate addresses because it makes the desired result of
      // the operation ambigous. If two different new values are specified for
      // the same address, what is the correct result? Also, if the operation
      // fails we can't guarantee that the old values will be correctly
      // restored.
      return -2;
    }
    if(words_[i].address_ < addr) {
      break;
    }
    words_[i + 1] = words_[i];
    insertpos--;
  }
  return insertpos;
}

#ifdef PMEM
inline uint32_t Descriptor::ReadPersistStatus() {
  auto curr_status = *& status_;
  uint32_t stable_status = curr_status & ~kStatusDirtyFlag;
  if(curr_status & kStatusDirtyFlag) {
    // We have a persistent descriptor that includes all the old and new values
    // needed for recovery, so only persist the new status.
    PersistStatus();
    // Now we can clear the dirty bit; this has to be a CAS, somebody else might
    // be doing the same and already proceeded to further phases.
    CompareExchange32(&status_, stable_status, curr_status);
  }
  return stable_status;
}
#endif

/// Installing mwcas descriptor must be a conditional CAS (double-compare
/// single-swap, RDCSS): a thread can only CAS in a pointer to the mwcas
/// descriptor if the mwcas operation is still in Undecided status. Otherwise
/// if a thread delays the CAS until another thread T1 (as a helper) has
/// finished the mwcas operation, and another thread T2 conducted yet another
/// mwcas to change the value back to the original value, T1 when resumes
/// would produce incorrect result. An example:
///
/// T1 mwcas(A1 [1 - 2], A2 [3 - 4])
/// Suppose T1 went to sleep after installed descriptor on A1.
/// T2 comes to help the mwcas operation and finished.
/// Now A1=2, A2=4.
/// Suppose T3 now conducted an mwcas that reversed the previous mwcas:
/// Now A1=1, A2=3 again.
/// Suppose T1 now resumes execution, it will continue to install
/// descriptor on A2, which will succeed because it has value 3.
/// T1 then continues to CAS in new values, which fails for A1 but the
/// algo will think it's ok (assuming some other thread did it). The
/// installation for A2, however, will succeeded because it contains
/// a descriptor. Now A1=1, A2=4, an inconsistent state.
uint64_t Descriptor::CondCAS(uint32_t word_index, uint64_t dirty_flag) {
  auto* w = &words_[word_index];
  uint64_t cond_descptr = SetFlags((uint64_t)w, kCondCASFlag);

retry:
  uint64_t ret = CompareExchange64(w->address_, cond_descptr, w->old_value_);
  if(IsCondCASDescriptorPtr(ret)) {
    // Already a CondCAS descriptor (ie a WordDescriptor pointer)
    WordDescriptor* wd = (WordDescriptor*)CleanPtr(ret);
    RAW_CHECK(wd->address_ == w->address_, "wrong address");
    uint64_t dptr = SetFlags(wd->GetDescriptor(), kMwCASFlag | dirty_flag);
    uint64_t desired =
      *wd->status_address_ == kStatusUndecided ? dptr : wd->old_value_;

    if(*(volatile uint64_t*)wd->address_ != ret) {
      goto retry;
    }
    auto rval = CompareExchange64(
      wd->address_,
      *wd->status_address_ == kStatusUndecided ? dptr : wd->old_value_,
      ret);
    if(rval == ret) {
      if(desired == dptr) {
        // Another competing operation succeeded, return
        return dptr;
      }
    }

    // Retry this operation
    goto retry;
  } else if(ret == w->old_value_) {
    uint64_t mwcas_descptr = SetFlags(this, kMwCASFlag | dirty_flag);
    CompareExchange64(w->address_,
        status_ == kStatusUndecided ? mwcas_descptr : w->old_value_,
        cond_descptr);
  }

  // ret could be a normal value or a pointer to a MwCAS descriptor
  return ret;
}

#ifdef RTM
bool Descriptor::RTMInstallDescriptors(uint64_t dirty_flag) {
  uint64_t mwcas_descptr = SetFlags(this, kMwCASFlag | dirty_flag);
  uint64_t tries = 0;
  static const uint64_t kMaxTries = 10000;

retry:
  if(_XBEGIN_STARTED == _xbegin()) {
    for(uint32_t i = 0; i < count_; ++i) {
      WordDescriptor* wd = &words_[i];
      if(*wd->address_ != wd->old_value_) {
        _xabort(0);
      }
      *wd->address_ = mwcas_descptr;
    }
    _xend();
    return true;
  }
  if(tries++ < kMaxTries) {
    goto retry;
  }
  return false;
}
#endif

#ifndef PMEM
inline bool Descriptor::VolatileMwCAS(uint32_t calldepth) {
  DCHECK(owner_partition_->garbage_list->GetEpoch()->IsProtected());

  if(status_ != kStatusUndecided) {
    if(calldepth > 0) {
      // Short circuit and return if the operation has already concluded.
      MwCASMetrics::AddBailedHelp();
      return status_ == kStatusSucceeded;
    } else {
      return Cleanup();
    }
  }

  uint64_t descptr = SetFlags(this, kMwCASFlag);
  uint32_t my_status = kStatusSucceeded;

  // Try to swap a pointer to this descriptor into all target addresses using
  // CondCAS
#ifdef RTM
  // If this operation is helping along, go to phase 2 directly
  if(calldepth == 0 && !RTMInstallDescriptors()) {
    my_status = kStatusFailed;
  }
#else

  for(uint32_t i = 0; i < count_ && my_status == kStatusSucceeded; i++) {
    WordDescriptor* wd = &words_[i];
retry_entry:
    auto rval = CondCAS(i);

    // Ok if a) we succeeded to swap in a pointer to this descriptor or b) some
    // other thread has already done so.
    if(rval == wd->old_value_ || rval == descptr) {
      continue;
    }

    // Do we need to help another MWCAS operation?
    if(IsMwCASDescriptorPtr(rval)) {
      // Clashed with another MWCAS; help complete the other MWCAS if it is
      // still being worked on.
      Descriptor* otherMWCAS = (Descriptor*)CleanPtr(rval);
      otherMWCAS->VolatileMwCAS(calldepth + 1);
      MwCASMetrics::AddHelpAttempt();
      goto retry_entry;
    } else {
      // rval must be another value, we failed
      my_status = kStatusFailed;
    }
  }
#endif

  CompareExchange32(&status_, my_status, kStatusUndecided);

  bool succeeded = (status_ == kStatusSucceeded);
  for(int i = 0; i < count_; i++) {
    WordDescriptor* wd = &words_[i];
    CompareExchange64(wd->address_,
        succeeded ? wd->new_value_ : wd->old_value_, descptr);
  }

  if(calldepth == 0) {
    return Cleanup();
  } else {
    return succeeded;
  }
}

bool Descriptor::VolatileMwCASWithFailure(uint32_t calldepth,
    bool complete_descriptor_install) {
  DCHECK(owner_partition_->garbage_list->GetEpoch()->IsProtected());

  if(status_ != kStatusUndecided) {
    if(calldepth > 0) {
      MwCASMetrics::AddBailedHelp();
      return status_ == kStatusSucceeded;
    } else {
      return Cleanup();
    }
  }

  uint64_t descptr = SetFlags(this, kMwCASFlag);
  uint32_t my_status = kStatusSucceeded;

  // Try to swap a pointer to this descriptor into all target addresses using
  // CondCAS
#ifdef RTM
  // If I'm helping, go to phase 2 directly
  if(calldepth == 0 && !RTMInstallDescriptors()) {
    my_status = kStatusFailed;
  }
#else

  for(uint32_t i = 0; i < count_ && my_status == kStatusSucceeded; i++) {
    WordDescriptor* wd = &words_[i];
retry_entry:
    auto rval = CondCAS(i);

    // Ok if a) we succeeded to swap in a pointer to this descriptor or b) some
    // other thread has already done so.
    if(rval == wd->old_value_ || rval == descptr) {
      continue;
    }

    // Do we need to help another MWCAS operation?
    if(IsMwCASDescriptorPtr(rval)) {
      // Clashed with another MWCAS; help complete the other MWCAS if it is
      // still being worked on
      Descriptor* otherMWCAS = (Descriptor*)CleanPtr(rval);
      otherMWCAS->VolatileMwCAS(calldepth + 1);
      MwCASMetrics::AddHelpAttempt();
      goto retry_entry;
    } else {
      // rval must be another value, we failed
      my_status = kStatusFailed;
    }
  }
#endif

  // The compare exchange below will determine whether the mwcas will roll
  // forward or back on recovery. If we are told to not complete descriptor
  // install, exit the function before updating the operation status. Otherwise
  // update the status but do not perform the final phase of the mwcas
  // (installing final values in place of the descriptors and memory cleanup).
  if (!complete_descriptor_install) {
    return false;
  }

  CompareExchange32(&status_, my_status, kStatusUndecided);

  // Always return false in this function, since operation is not supposed to
  // fully succeed.
  return false;
}

#endif

#ifdef PMEM
inline bool Descriptor::PersistentMwCAS(uint32_t calldepth) {
  DCHECK(owner_partition_->garbage_list->GetEpoch()->IsProtected());

  // Not visible to anyone else, persist before making the descriptor visible
  if(calldepth == 0) {
    RAW_CHECK(status_ == kStatusUndecided, "invalid status");
    NVRAM::Flush(sizeof(Descriptor), this);
  }

  auto status = status_;
  if(status & kStatusDirtyFlag) {
    PersistStatus();
    CompareExchange32(&status_, status & ~kStatusDirtyFlag, status);
    status &= ~kStatusDirtyFlag;
  }
  if(status != kStatusUndecided) {
    if(calldepth > 0) {
      // Operation has already concluded, return.
      MwCASMetrics::AddBailedHelp();
      return status == kStatusSucceeded;
    } else {
      return Cleanup();
    }
  }

  uint64_t descptr = SetFlags(this, kMwCASFlag | kDirtyFlag);
  uint32_t my_status = kStatusSucceeded;

#ifdef RTM
  // Go to phase 2 directly if helping along.
  if(calldepth > 0 && !RTMInstallDescriptors(kDirtyFlag)) {
    my_status = kStatusFailed;
  }
#else

  for(uint32_t i = 0; i < count_ && my_status == kStatusSucceeded; ++i) {
    WordDescriptor* wd = &words_[i];
retry_entry:
    auto rval = CondCAS(i, kDirtyFlag);

    // Ok if a) we succeeded to swap in a pointer to this descriptor or b) some
    // other thread has already done so. Need to persist all fields (which point
    // to descriptors) before switching to final status, so that recovery will
    // know reliably whether to roll forward or back for this descriptor.
    if(rval == wd->old_value_ || CleanPtr(rval) == (uint64_t)this) {
      continue;
    }

    // Do we need to help another MWCAS operation?
    if(IsMwCASDescriptorPtr(rval)) {
      if(rval & kDirtyFlag) {
        wd->PersistAddress();
        CompareExchange64(wd->address_, rval & ~kDirtyFlag, rval);
      }

      // Clashed with another MWCAS; help complete the other MWCAS if it is
      // still in flight.
      Descriptor* otherMWCAS = (Descriptor*)CleanPtr(rval);
      otherMWCAS->PersistentMwCAS(calldepth + 1);
      MwCASMetrics::AddHelpAttempt();
      goto retry_entry;
    } else {
      // rval must be another value, we failed
      my_status = kStatusFailed;
    }
  }
#endif

  // Persist all target fields if we successfully installed mwcas descriptor on
  // all fields.
  if(my_status == kStatusSucceeded) {
    for (uint32_t i = 0; i < count_; ++i) {
      WordDescriptor* wd = &words_[i];
      uint64_t val = *wd->address_;
      if(val == descptr) {
        wd->PersistAddress();
        CompareExchange64(wd->address_, descptr & ~kDirtyFlag, descptr);
      }
    }
  }

  // Switch to the final state, the MwCAS concludes after this point
  CompareExchange32(&status_, my_status | kStatusDirtyFlag, kStatusUndecided);

  // Now the MwCAS is concluded - status is either succeeded or failed, and
  // no observers will try to help finish it, so do a blind flush and reset
  // the dirty bit.
  RAW_CHECK((status_ & ~kStatusDirtyFlag) != kStatusUndecided, "invalid status");
  PersistStatus();
  status_ &= ~kStatusDirtyFlag;
  // No need to flush again, recovery does not care about the dirty bit

  bool succeeded = (status_ == kStatusSucceeded);
  for(uint32_t i = 0; i < count_; i++) {
    WordDescriptor* wd = &words_[i];
    uint64_t val = succeeded ? wd->new_value_ : wd->old_value_;
    val |= kDirtyFlag;
    uint64_t clean_descptr = descptr & ~kDirtyFlag;
    if(clean_descptr == CompareExchange64(wd->address_, val, descptr)) {
      // Retry if someone else already cleared the dirty bit
      CompareExchange64(wd->address_, val, clean_descptr);
    }
    wd->PersistAddress();
    RAW_CHECK(val & kDirtyFlag, "invalid final value");
    CompareExchange64(wd->address_, val & ~kDirtyFlag, val);
  }

  if(calldepth == 0) {
    return Cleanup();
  } else {
    return succeeded;
  }
}

bool Descriptor::PersistentMwCASWithFailure(uint32_t calldepth,
    bool complete_descriptor_install) {
  DCHECK(owner_partition_->garbage_list->GetEpoch()->IsProtected());

  // Not visible to anyone else, persist before making the descriptor visible
  if(calldepth == 0) {
    RAW_CHECK(status_ == kStatusUndecided, "invalid status");
    NVRAM::Flush(sizeof(Descriptor), this);
  }

  auto status = status_;
  if(status & kStatusDirtyFlag) {
    PersistStatus();
    CompareExchange32(&status_, status & ~kStatusDirtyFlag, status);
    status &= ~kStatusDirtyFlag;
  }
  if(status != kStatusUndecided) {
    if(calldepth > 0) {
      MwCASMetrics::AddBailedHelp();
      return status == kStatusSucceeded;
    } else {
      return Cleanup();
    }
  }

  uint64_t descptr = SetFlags(this, kMwCASFlag | kDirtyFlag);
  uint32_t my_status = kStatusSucceeded;

#ifdef RTM
  if(calldepth > 0 && !RTMInstallDescriptors(kDirtyFlag)) {
    my_status = kStatusFailed;
  }
#else

  for(uint32_t i = 0; i < count_ && my_status == kStatusSucceeded; ++i) {
    WordDescriptor* wd = &words_[i];
retry_entry:
    auto rval = CondCAS(i, kDirtyFlag);

    // Ok if a) we succeeded to swap in a pointer to this descriptor or b)
    // some other thread has already done so. Need to persist all fields (which
    // point to descriptors) before switching to final status, so that recovery
    // will know reliably whether to roll forward or back for this descriptor.
    if(rval == wd->old_value_ || CleanPtr(rval) == (uint64_t)this) {
      continue;
    }

    // Do we need to help another MWCAS operation?
    if(IsMwCASDescriptorPtr(rval)) {
      if(rval & kDirtyFlag) {
        wd->PersistAddress();
        CompareExchange64(wd->address_, rval & ~kDirtyFlag, rval);
      }
      // Clashed with another MWCAS; help complete the other MWCAS if it is
      // still being worked on.
      Descriptor* otherMWCAS = (Descriptor*)CleanPtr(rval);
      otherMWCAS->PersistentMwCAS(calldepth + 1);
      MwCASMetrics::AddHelpAttempt();
      goto retry_entry;
    } else {
      // rval must be another value, we failed
      my_status = kStatusFailed;
    }
  }
#endif

  // Persist all target fields if we successfully installed mwcas descriptor on
  // all fields.
  if(my_status == kStatusSucceeded) {
    for (uint32_t i = 0; i < count_; ++i) {
      WordDescriptor* wd = &words_[i];
      uint64_t val = *wd->address_;
      if(val == descptr) {
        wd->PersistAddress();
        CompareExchange64(wd->address_, descptr & ~kDirtyFlag, descptr);
      }
    }
  }

  // The compare exchange below will determine whether the mwcas will roll
  // forward or back on recovery. If we are told to not complete descriptor
  // install, exit the function before updating the operation status. Otherwise
  // update the status but do not perform the final phase of the mwcas
  // (installing final values in place of the descriptors and memory cleanup).
  if (!complete_descriptor_install) {
    return false;
  }

  // Switch to the final state, the MwCAS concludes after this point
  CompareExchange32(&status_, my_status | kStatusDirtyFlag, kStatusUndecided);

  RAW_CHECK((status_ & ~kStatusDirtyFlag) != kStatusUndecided, "invalid status");
  PersistStatus();
  status_ &= ~kStatusDirtyFlag;

  // Always return false, since this function is used to test failure paths.
  return false;
}
#endif

bool Descriptor::Cleanup() {
  // Remeber outcome so we can return it.
  // We are sure here Status doesn't have dirty flag set
  RAW_CHECK((status_ & kStatusDirtyFlag) == 0, "invalid status");
  RAW_CHECK(status_ == kStatusFailed || status_ == kStatusSucceeded,
      "invalid status");

  if(status_ == kStatusSucceeded) {
    MwCASMetrics::AddSucceededUpdate();
  } else {
    MwCASMetrics::AddFailedUpdate();
  }

  // There will be no new accessors once we have none of the target fields
  // contain a pointer to this descriptor; this is the point we can put this
  // descriptor in the garbage list. Note that multiple threads might be working
  // on the same descriptor at the same time, and only one thread can push to
  // the garbage list, so we can only change to kStatusFinished state after no
  // one is using the descriptor, i.e., in FreeDescriptor(), and let the
  // original owner (calldepth=0, i.e., the op that calls Cleanup()) push the
  // descriptor to the garbage list.
  //
  // Note: It turns out frequently Protect() and Unprotect() is expensive, so
  // let the user determine when to do it (e.g., exit/re-enter every X mwcas
  // operations). Inside any mwcas-related operation we assume it's already
  // protected.
  auto s = owner_partition_->garbage_list->Push(this,
      Descriptor::FreeDescriptor, nullptr);
  RAW_CHECK(s.ok(), "garbage list push() failed");
  DCHECK(owner_partition_->garbage_list->GetEpoch()->IsProtected());
  return status_ == kStatusSucceeded;
}

Status Descriptor::Abort() {
  RAW_CHECK(status_ == kStatusFinished, "cannot abort under current status");
  status_ = kStatusFailed;
  auto s = owner_partition_->garbage_list->Push(this,
      Descriptor::FreeDescriptor, nullptr);
  RAW_CHECK(s.ok(), "garbage list push() failed");
  return s;
}

void Descriptor::DeallocateMemory() {
  // Free the memory associated with the descriptor if needed
  for(uint32_t i = 0; i < count_; ++i) {
    auto& word = words_[i];
    auto status = status_;
    switch(word.recycle_policy_) {
      case kRecycleNever:
      case kRecycleOnRecovery:
        break;
      case kRecycleAlways:
        if(status == kStatusSucceeded) {
          if(word.old_value_ != kNewValueReserved) {
            free_callback_(nullptr, (void*)word.old_value_);
          }
        } else {
          RAW_CHECK(status == kStatusFailed || status == kStatusFinished,
              "incorrect status found on used/discarded descriptor");
          if(word.new_value_ != kNewValueReserved) {
            free_callback_(nullptr, (void*)word.new_value_);
          }
        }
        break;
      case kRecycleOldOnSuccess:
        if(status == kStatusSucceeded) {
          if(word.old_value_ != kNewValueReserved) {
            free_callback_(nullptr, (void*)word.old_value_);
          }
        }
        break;
      case kRecycleNewOnFailure:
        if(status != kStatusSucceeded) {
          if(word.new_value_ != kNewValueReserved) {
            free_callback_(nullptr, (void*)word.new_value_);
          }
        }
        break;
      default:
        LOG(FATAL) << "invalid recycle policy";
    }
  }
  count_ = 0;
}

void Descriptor::FreeDescriptor(void* context, void* desc) {
  MARK_UNREFERENCED(context);

  Descriptor* desc_to_free = reinterpret_cast<Descriptor*>(desc);
  desc_to_free->DeallocateMemory();
  desc_to_free->Initialize();

  RAW_CHECK(desc_to_free->status_ == kStatusFinished, "invalid status");

  desc_to_free->next_ptr_ = desc_to_free->owner_partition_->free_list;
  desc_to_free->owner_partition_->free_list = desc_to_free;
}

} // namespace pmwcas
