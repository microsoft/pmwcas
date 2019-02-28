// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.
//
// Implements variants of the multi-word compare-and-swap (MwCAS) primitive
// that can work for volatile DRAM and persistent memory. The operation is
// lock-free and non-blocking. It requires flag bits on each word. Currently
// Intel and AMD implements 48 out of the 64 bits for addresses, so these
// bits reside in the most significant 16 bits.
//
// The basic MwCAS algorithm is based on ideas in:
//    Harris, Timothy L., Fraser, Keir, Pratt, Ian A.,
//    A Practical Multi-word Compare-and-Swap Operation", DISC 2002, 265-279
//
// It requires using a conditional CAS (RDCSS) to install MwCAS descriptors,
// RDCSS itself in turn needs descriptors. This requires two bits in the MSBs.
//
// The persistence support employes an extra bit to indicate if the word is
// possbily dirty (not reflected in NVRAM yet).
//
// |--63---|----62---|---61--|--rest bits--|
// |-MwCAS-|-CondCAS-|-Dirty-|-------------|
//
// Any application that uses this MwCAS primitive thus cannot use the 3 MSBs
// (e.g., the deletion marker of a linked list has to be at bit 60 or lower).
//
// Interactions with user data memory to prevent persistent memory leaks:
// Assume a persistent allocator is already in place, and it provides the
// following semantics:
// 1. Allocation is done through a posix_memalign-like interface which accepts
//    a reference to the location which stores the address of the allocated
//    memory. This provided 'reference' must be part of the allocation's
//    persistent data structure, or any place that the application can read
//    during recovery.
// 2. Upon recovery, the allocator will examine (by actively remembering all
//    memory it allocated through e.g., a hashtab or upon request from the
//    application) each reference provided. If it contains null, then the memory
//    should be freed internally (i.e., ownership didn't transfer to
//    application); otherwise the application should decide what to do with the
//    memory. Ownership is transferred.
//
// With the above interface/guarantees, the application can pass a reference of
// the 'new value' fields in each word to the allocator. The allocator will
// examine these places upon recovery. In general, MwCAS's recovery routine will
// deallocate the 'old values' for successful mwcas ops, and deallocate the 'new
// values' for failed mwcas ops. See kRecycle* for all possible policies. After
// freeing memory, the recovery routine resets the descriptor fields to null.
//
// Note: this requires the allocator to do double-free detection in case there
// is repeated failure.
//
// (If an persistent allocator can provide a drop-in replacement to malloc, then
// it's up to the application to decide what to do.)
//
// During forward processing, the application can choose to piggy back on
// MwCAS's epoch manager for pointer stability. Depending on whether the mwcas
// succeeded, the descriptor free routine will deallocate memory addresses
// stored in 'old value' or 'new value'. This means the application also need
// not handle pointer stability itself, the MwCAS's epoch manager does it
// transparently.
//
// The application can specify the callbacks for allocating and deallocating
// memory when allocating MwCAS descriptors. Each word can be specified to
// whether use mwcas's mechanism for memory deallocation.
#pragma once

#ifdef WIN32
#include <Windows.h>
#undef ERROR // Avoid collision of ERROR definition in Windows.h with glog
#endif

#include <stdio.h>
#include <assert.h>
#include <cstdint>
#include <mutex>
#include <gtest/gtest_prod.h>
#include "glog/logging.h"
#include "glog/raw_logging.h"
#include "common/allocator_internal.h"
#include "common/environment_internal.h"
#include "common/epoch.h"
#include "common/garbage_list_unsafe.h"
#include "include/environment.h"
#include "metrics.h"
#include "util/nvram.h"

namespace pmwcas {

// Forward references
struct DescriptorPartition;
class Descriptor;
class DescriptorPool;

class alignas(kCacheLineSize) Descriptor {
  template<typename T> friend class MwcTargetField;

public:
  /// Signifies a dirty word requiring cache line write back
  static const uint64_t kDirtyFlag   = (uint64_t)1 << 61;

  /// Garbage list recycle policy: only free [new_value] upon restart
  static const uint32_t kRecycleOnRecovery = 0x1;
  
  /// Garbage list recycle policy: leave the memory alone
  static const uint32_t kRecycleNever = 0x2;

  /// Garbage list recycle policy: free [old/new_value] if succeeded/failed
  static const uint32_t kRecycleAlways = 0x3;

  /// Garbage list recycle policy: free only [old value] if succeeded
  static const uint32_t kRecycleOldOnSuccess = 0x4;

  /// Garbage list recycle policy: free only [new value] if succeeded
  static const uint32_t kRecycleNewOnFailure = 0x5;

  /// Signaure for garbage free callback (see free_callback_ below)
  typedef void (*FreeCallback)(void* context, void* word);

  /// Signature for NVM allocation callback (see allocate_callback_ below)
  typedef void* (*AllocateCallback)(size_t size);

  /// The default NVM allocate callback used if no callback is specified by the user
  static void* DefaultAllocateCallback(size_t size);

  /// The default free callback used if no callback is specified by the user
  static void DefaultFreeCallback(void* context, void* p);

  /// Specifies what word to update in the mwcas, storing before/after images so
  /// others may help along. This also servers as the descriptor for conditional
  /// CAS(RDCSS in the Harris paper). status_address_ points to the parent
  /// Descriptor's status_ field which determines whether a CAS that wishes to
  /// make address_ point to WordDescriptor can happen.
  struct WordDescriptor {

    /// The target address
    uint64_t* address_;

    /// The original old value stored at /a Address
    uint64_t old_value_;

    /// The new value to be stored at /a Address
    uint64_t new_value_;

    /// The parent Descriptor's status
    uint32_t* status_address_;

    /// Whether to invoke the user-provided memory free callback to free the
    /// memory when recycling this descriptor. This must be per-word - the
    /// application could mix pointer and non-pointer changes in a single mwcas,
    /// e.g., in the bwtree we might use a single mwcas to change both the root
    /// lpid and other memory page pointers along the way.
    uint32_t recycle_policy_;

    /// Returns the parent descriptor for this particular word
    inline Descriptor* GetDescriptor() {
      return (Descriptor*)((uint64_t)status_address_ -
          offsetof(Descriptor, status_));
    }

#ifdef PMEM
    /// Persist the content of address_
    inline void PersistAddress() {
      NVRAM::Flush(sizeof(uint64_t*), (void*)&address_);
    }
#endif

  };

  /// Default constructor
  Descriptor()  = delete;
  Descriptor(DescriptorPartition* partition);

  /// Function for initializing a newly allocated Descriptor.
  void Initialize();

  /// Executes the multi-word compare and swap operation.
  bool MwCAS() {
    RAW_CHECK(status_ == kStatusFinished,
      "status of descriptor is not kStatusFinished");
    status_ = kStatusUndecided;
#ifdef PMEM
    return PersistentMwCAS(0);
#else
    return VolatileMwCAS(0);
#endif
  }

  /// Retrieves the new value for the given word index in the PMwCAS
  inline uint64_t GetNewValue(uint32_t index) {
    return words_[index].new_value_;
  }

  /// Retrieves the pointer to the new value slot for a given word in the PMwCAS
  inline uint64_t* GetNewValuePtr(uint32_t index) {
    return &words_[index].new_value_;
  }

  /// Adds information about a new word to be modifiec by the MwCAS operator.
  /// Word descriptors are stored sorted on the word address to prevent
  /// livelocks. Return value is negative if the descriptor is full.
  /// @free_on_recycle: use the user-provided callback to free the address
  /// stored in [oldval] (if mwcas succeeded) or [newval] (if mwcas failed).
  /// Pre-requisite: the application is using mwcas to change memory pointers,
  /// although technically the application can abuse this mechanism to do
  /// anything.
  uint32_t AddEntry(uint64_t* addr, uint64_t oldval, uint64_t newval,
                    uint32_t recycle_policy = kRecycleNever);

  /// Allocate [size] bytes of memory and store the address in [newval]. Assume
  /// the allocator features an interface similar to posix_memalign's which
  /// accepts a reference to the location that will store the address of
  /// allocated memory. In our case it's [newval]. Note: applies only if the
  /// MwCAS is intended to change pointer values.
  uint32_t AllocateAndAddEntry(uint64_t* addr, uint64_t oldval, size_t size,
                               uint32_t recycle_policy = kRecycleNever);

  /// Reserve a slot in the words array, but don't know what the new value is
  /// yet. The application should use GetNewValue[Ptr] to fill in later.
  inline uint32_t ReserveAndAddEntry(uint64_t* addr, uint64_t oldval,
                                     uint32_t recycle_policy = kRecycleNever) {
    return AddEntry(addr, oldval, kNewValueReserved, recycle_policy);
  }

  /// Abort the MwCAS operation, can be used only before the operation starts.
  Status Abort();

private:
  /// Allow tests to access privates for failure injection purposes.
  FRIEND_TEST(PMwCASTest, SingleThreadedRecovery);

  friend class DescriptorPool;

  /// Value signifying an internal reserved value for a new entry
  static const uint64_t kNewValueReserved = ~0ull;

  /// Internal helper function to conduct a double-compare, single-swap
  /// operation on an target field depending on the value of the status_ field
  /// in Descriptor. The conditional CAS tries to install a pointer to the MwCAS
  /// descriptor derived from one of words_, expecting the status_ field
  /// indicates Undecided. [dirty_flag] will be applied on the MwCAS descriptor
  /// address if specified.
  uint64_t CondCAS(uint32_t word_index, uint64_t dirty_flag = 0);

  /// A version of the MwCAS function that will fail/abort during execution.
  /// This is a private function that should only be used for testing failure
  /// modes and recovery.
  bool MwCASWithFailure(uint32_t calldepth = 0,
      bool complete_descriptor_install = false) {
    RAW_CHECK(status_ == kStatusFinished,
        "status of descriptor is not kStatusFinished");
    status_ = kStatusUndecided;
#ifdef PMEM
    return PersistentMwCASWithFailure(calldepth, complete_descriptor_install);
#else
    return VolatileMwCASWithFailure(calldepth, complete_descriptor_install);
#endif
  }

#ifdef RTM
  bool RTMInstallDescriptors(uint64_t dirty_flag = 0);
#endif

  /// Retrieve the index position in the descriptor of the given address.
  int GetInsertPosition(uint64_t* addr);

#ifndef PMEM
  /// Execute the multi-word compare and swap operation.
  bool VolatileMwCAS(uint32_t calldepth = 0);

  /// Volatile version of the multi-word CAS with failure injection.
  bool VolatileMwCASWithFailure(uint32_t calldepth = 0,
    bool complete_descriptor_install = false);
#endif

#ifdef PMEM
  /// Execute the multi-word compare and swap operation on persistent memory.
  bool PersistentMwCAS(uint32_t calldepth = 0);

  /// Persistent version of the multi-word CAS with failure injection.
  bool PersistentMwCASWithFailure(uint32_t calldepth = 0,
    bool complete_descriptor_install = false);

  /// Flush only the Status field to persistent memory.
  inline void PersistStatus() { NVRAM::Flush(sizeof(status_), &status_); }

  // Read and persist the status field (if its dirty bit is set).
  // The caller must ensure that the descriptor is already persistent.
  // The returned value is guaranteed to be persistent in PM.
  uint32_t ReadPersistStatus();
#endif

  /// Flag signifying an multi-word CAS is underway for the target word.
  static const uint64_t kMwCASFlag   = (uint64_t)1 << 63;

  /// Flag signifying a conditional CAS is underway for the target word.
  static const uint64_t kCondCASFlag = (uint64_t)1 << 62;

  /// Returns whether the value given is an MwCAS descriptor or not.
  inline static bool IsMwCASDescriptorPtr(uint64_t value) {
    return value & kMwCASFlag;
  }

  /// Returns whether the value given is a CondCAS descriptor or not.
  inline static bool IsCondCASDescriptorPtr(uint64_t value) {
    return value & kCondCASFlag;
  }

  /// Returns whether the underlying word is dirty (not surely persisted).
  inline static bool IsDirtyPtr(uint64_t value) {
    return value & kDirtyFlag;
  }

  /// Returns true if the target word has no pmwcas management flags set.
  inline static bool IsCleanPtr(uint64_t value) {
    return (value & (kCondCASFlag | kMwCASFlag | kDirtyFlag)) == 0;
  }

  /// Clear the descriptor flag for the provided /a ptr
  static inline uint64_t CleanPtr(uint64_t ptr) {
    return ptr & ~(kMwCASFlag | kCondCASFlag | kDirtyFlag);
  }

  /// Bitwise-or the given flags to the given value
  inline static uint64_t SetFlags(uint64_t value, uint64_t flags) {
    RAW_CHECK((flags & ~(kMwCASFlag | kCondCASFlag | kDirtyFlag)) == 0,
        "invalid flags");
    return value | flags;
  }

  /// Set the given flags for a target descriptor word.
  inline static uint64_t SetFlags(Descriptor* desc, uint64_t flags) {
    return SetFlags((uint64_t)desc, flags);
  }

  /// Mask to indicate the status field is dirty, any reader should first flush
  /// it before use.
  static const uint32_t kStatusDirtyFlag = 1ULL << 31;

  /// Cleanup steps of MWCAS common to both persistent and volatile versions.
  bool Cleanup();

  /// Deallocate the memory associated with the MwCAS if needed.
  void DeallocateMemory();

  /// Places a descriptor back on the descriptor free pool (partitioned). This
  /// can be used as the callback function for the epoch manager/garbage list to
  /// reclaim this descriptor for reuse after we are sure no one is using or
  /// could possibly access this descriptor.
  static void FreeDescriptor(void* context, void* desc);

  /// Descriptor states. Valid transitions are as follows:
  /// kStatusUndecided->kStatusSucceeded->kStatusFinished->kStatusUndecided
  ///               \-->kStatusFailed-->kStatusFinished->kStatusUndecided
  static const uint32_t kStatusInvalid   = 0U;
  static const uint32_t kStatusFinished  = 1U;
  static const uint32_t kStatusSucceeded = 2U;
  static const uint32_t kStatusFailed    = 3U;
  static const uint32_t kStatusUndecided = 4U;

  inline void assert_valid_status() {
    auto s = status_ & ~kStatusDirtyFlag;
    RAW_CHECK(s == kStatusFinished || s == kStatusFailed ||
              s == kStatusSucceeded || s == kStatusUndecided, "invalid status");
  }

  /// Setting kMaxCount to 4 so MwCASDescriptor occupies three cache lines. If
  /// changing this, also remember to adjust the static assert below.
  static const int kMaxCount = 4;

  /// Free list pointer for managing free pre-allocated descriptor pools
  Descriptor* next_ptr_;

  /// Back pointer to owning partition so the descriptor can be returned to its
  /// howm partition when it is freed.
  DescriptorPartition* owner_partition_;

  /// Tracks the current status of the descriptor.
  uint32_t status_;

  /// Count of actual descriptors held in #WordDesc
  uint32_t count_;


  /// A callback for freeing the words listed in [words_] when recycling the
  /// descriptor. Optional: only for applications that use it.
  FreeCallback free_callback_;

  /// A callback for allocating memory in AllocateAndAddEntry; the address of
  /// the allocated memory will be store in [new_value].
  AllocateCallback allocate_callback_;

  /// Array of word descriptors bounded my kMaxCount
  WordDescriptor words_[kMaxCount];
};
static_assert(sizeof(Descriptor) <= 4 * kCacheLineSize,
    "Descriptor larger than 4 cache lines");

/// A partitioned pool of Descriptors used for fast allocation of descriptors.
/// The pool of descriptors will be bounded by the number of threads actively
/// performing an mwcas operation.
struct alignas(kCacheLineSize)DescriptorPartition {

  DescriptorPartition() = delete;
  DescriptorPartition(EpochManager* epoch, DescriptorPool* pool);

  ~DescriptorPartition();

  /// Pointer to the free list head (currently managed by lock-free list)
  Descriptor *free_list;

  /// Back pointer to the owner pool
  DescriptorPool* desc_pool;

  /// Garbage list holding freed pointers/words waiting to clear epoch
  /// protection before being truly recycled.
  GarbageListUnsafe* garbage_list;

  /// Number of allocated descriptors
  uint32_t allocated_desc;
};

class DescriptorPool {
private:
  /// Total number of descriptors in the pool
  uint32_t pool_size_;

  /// Number of descriptors per partition
  uint32_t desc_per_partition_;

  /// Points to all descriptors
  Descriptor* descriptors_;

  /// Number of partitions in the partition_table_
  uint32_t partition_count_;

  /// Descriptor partitions (per thread)
  DescriptorPartition* partition_table_;

  /// The next partition to assign (round-robin) for a new thread joining the
  /// pmwcas library.
  std::atomic<uint32_t> next_partition_;

  /// Epoch manager controling garbage/access to descriptors.
  EpochManager epoch_;

  /// Track the pmdk pool for recovery purpose
  uint64_t pmdk_pool_;

  void InitDescriptors();

 public:
  /// Metadata that prefixes the actual pool of descriptors for persistence
  struct Metadata {
    /// Number of descriptors
    uint64_t descriptor_count;
    /// Address of the area got after initializing the area first-time
    uintptr_t initial_address;
    /// Pad to cacheline size
    char padding[kCacheLineSize - sizeof(uint64_t) - sizeof(uintptr_t)];
    Metadata() : descriptor_count(0), initial_address(0) {}
  };
  static_assert(sizeof(Metadata) == kCacheLineSize,
                "Metadata not of cacheline size");

  DescriptorPool(uint32_t pool_size, uint32_t partition_count, bool enable_stats = false);

  Descriptor* GetDescriptor(){
    return descriptors_;
  }

#ifdef PMEM
  void Recovery(bool enable_stats);
#endif

  ~DescriptorPool();

  /// Returns a pointer to the epoch manager associated with this pool.
  /// MwcTargetField::GetValue() needs it.
  EpochManager* GetEpoch() {
    return &epoch_;
  }

  // Get a free descriptor from the pool.
  Descriptor* AllocateDescriptor(Descriptor::AllocateCallback ac,
    Descriptor::FreeCallback fc);
  
  // Allocate a free descriptor from the pool using default allocate and
  // free callbacks.
  inline Descriptor* AllocateDescriptor() {
    return AllocateDescriptor(nullptr, nullptr);
  }
};

/// Represents an 8-byte word that is a target for a compare-and-swap. Used to
/// abstract away and hide all the low-level bit manipulation to track internal
/// status of the word. By default use the 2 LSBs as flags, assuming the values
/// point to word-aligned addresses.
template <class T>
class MwcTargetField {
  static_assert(sizeof(T) == 8, "MwCTargetField type is not of size 8 bytes");

public:
  static const uint64_t kMwCASFlag = Descriptor::kMwCASFlag;
  static const uint64_t kCondCASFlag = Descriptor::kCondCASFlag;
  static const uint64_t kDescriptorMask = kMwCASFlag | kCondCASFlag;
  static const uint64_t kDirtyFlag   = Descriptor::kDirtyFlag;

  MwcTargetField(void* desc = nullptr) {
    value_ = T(desc);
  }

  /// Enter epoch protection and then return the value.
  inline T GetValue(EpochManager* epoch) {
#ifdef PMEM
    return GetValuePersistent(epoch);
#else
    return GetValueVolatile(epoch);
#endif
  }

  /// Get value assuming epoch protection.
  inline T GetValueProtected() {
#ifdef PMEM
    return GetValueProtectedPersistent();
#else
    return GetValueProtectedVolatile();
#endif
  }

  /// Returns true if the given word does not have any internal management
  /// flags set, false otherwise.
  static inline bool IsCleanPtr(uint64_t ptr) {
    return (ptr & (kCondCASFlag | kMwCASFlag | kDirtyFlag)) == 0;
  }

  /// Returns true if the value does not have any internal management flags set,
  /// false otherwise.
  inline bool IsCleanPtr() {
    return (value_ & (kCondCASFlag | kMwCASFlag | kDirtyFlag)) == 0;
  }

#ifdef PMEM
  /// Persist the value_ to be read
  inline void PersistValue() {
    NVRAM::Flush(sizeof(uint64_t), (const void*)&value_);
  }
#endif

  /// Return an integer representation of the target word
  operator uint64_t() {
    return uint64_t(value_);
  }

  /// Copy operator
  MwcTargetField<T>& operator= (MwcTargetField<T>& rhval) {
    value_ = rhval.value_;
    return *this;
  }

  /// Address-of operator
  T* operator& () {
    return const_cast<T*>(&value_);
  }

  /// Assignment operator
  MwcTargetField<T>& operator= (T rhval) {
    value_ = rhval;
    return *this;
  }

  /// Content-of operator
  T& operator* () {
    return *value_;
  }

  /// Dereference operator
  T* operator-> () {
    return value_;
  }

private:
#ifndef PMEM
  /// Return the value in this word. If the value is a descriptor there is a CAS
  /// in progress, so help along completing the CAS before returning the value
  /// in the target word.
  inline T GetValueVolatile(EpochManager* epoch) {
    MwCASMetrics::AddRead();
    EpochGuard guard(epoch, !epoch->IsProtected());

  retry:
    uint64_t val = (uint64_t)value_;

  #ifndef RTM
    if(val & kCondCASFlag) {
      Descriptor::WordDescriptor* wd =
        (Descriptor::WordDescriptor*)Descriptor::CleanPtr(val);
      uint64_t dptr = Descriptor::SetFlags(wd->GetDescriptor(), kMwCASFlag);
      RAW_CHECK((char*)this == (char*)wd->address_, "wrong addresses");

      CompareExchange64(
        wd->address_,
        *wd->status_address_ == Descriptor::kStatusUndecided ?
            dptr : wd->old_value_,
        val);
      goto retry;
    }
  #endif

    if(val & kMwCASFlag) {
      // While the address contains a descriptor, help along completing the CAS
      Descriptor* desc = (Descriptor*)Descriptor::CleanPtr(val);
      RAW_CHECK(desc, "invalid descriptor pointer");
      desc->VolatileMwCAS(1);
      goto retry;
    }

    return val;
  }
 
  /// Same as GetValue, but guaranteed to be Protect()'ed already.
  inline T GetValueProtectedVolatile() {
    MwCASMetrics::AddRead();

  retry:
    uint64_t val = (uint64_t)value_;

#ifndef RTM
    if(val & kCondCASFlag) {
      Descriptor::WordDescriptor* wd =
        (Descriptor::WordDescriptor*)Descriptor::CleanPtr(val);
      uint64_t dptr = Descriptor::SetFlags(wd->GetDescriptor(), kMwCASFlag);
      RAW_CHECK((char*)this == (char*)wd->address_, "wrong addresses");
      CompareExchange64(
        wd->address_,
        *wd->status_address_ == Descriptor::kStatusUndecided ?
            dptr : wd->old_value_,
        val);
      goto retry;
    }
#endif

    if(val & kMwCASFlag) {
      // While the address contains a descriptor, help along completing the CAS
      Descriptor* desc = (Descriptor*)Descriptor::CleanPtr(val);
      RAW_CHECK(desc, "invalid descriptor pointer");
      desc->VolatileMwCAS(1);
      goto retry;
    }
    return val;
  }
#endif

#ifdef PMEM
  // The persistent variant of GetValue().
  T GetValuePersistent(EpochManager* epoch) {
    MwCASMetrics::AddRead();
    EpochGuard guard(epoch, !epoch->IsProtected());

retry:
    uint64_t val = (uint64_t)value_;

#ifndef RTM
    if(val & kCondCASFlag) {
      RAW_CHECK((val & kDirtyFlag) == 0,
          "dirty flag set on CondCAS descriptor");

      Descriptor::WordDescriptor* wd =
        (Descriptor::WordDescriptor*)Descriptor::CleanPtr(val);
      uint64_t dptr =
        Descriptor::SetFlags(wd->GetDescriptor(), kMwCASFlag | kDirtyFlag);
      CompareExchange64(
        wd->address_,
        *wd->status_address_ == Descriptor::kStatusUndecided ?
            dptr : wd->old_value_,
        val);
      goto retry;
    }
#endif

    if(val & kDirtyFlag) {
      PersistValue();
      CompareExchange64((uint64_t*)&value_, val & ~kDirtyFlag, val);
      val &= ~kDirtyFlag;
    }
    RAW_CHECK((val & kDirtyFlag) == 0, "dirty flag set on return value");

    if(val & kMwCASFlag) {
      // While the address contains a descriptor, help along completing the CAS
      Descriptor* desc = (Descriptor*)Descriptor::CleanPtr(val);
      RAW_CHECK(desc, "invalid descriptor pointer");
      desc->PersistentMwCAS(1);
      goto retry;
    }
    RAW_CHECK(IsCleanPtr(val), "dirty flag set on return value");

    return val;
  }

  // The "protected" variant of GetPersistValue().
  T GetValueProtectedPersistent() {
    MwCASMetrics::AddRead();

  retry:
    uint64_t val = (uint64_t)value_;
#ifndef RTM
    if(val & kCondCASFlag) {
      RAW_CHECK((val & kDirtyFlag) == 0, "dirty flag set on CondCAS descriptor");

      Descriptor::WordDescriptor* wd =
        (Descriptor::WordDescriptor*)Descriptor::CleanPtr(val);
      uint64_t dptr =
        Descriptor::SetFlags(wd->GetDescriptor(), kMwCASFlag | kDirtyFlag);
      CompareExchange64(
        wd->address_,
        *wd->status_address_ == Descriptor::kStatusUndecided ?
            dptr : wd->old_value_,
        val);
      goto retry;
    }
#endif

    if(val & kDirtyFlag) {
      PersistValue();
      CompareExchange64((uint64_t*)&value_, val & ~kDirtyFlag, val);
      val &= ~kDirtyFlag;
    }
    RAW_CHECK((val & kDirtyFlag) == 0, "dirty flag set on return value");

    if(val & kMwCASFlag) {
      // While the address contains a descriptor, help along completing the CAS
      Descriptor* desc = (Descriptor*)Descriptor::CleanPtr(val);
      RAW_CHECK(desc, "invalid descriptor pointer");
      desc->PersistentMwCAS(1);
      goto retry;
    }
    RAW_CHECK(IsCleanPtr(val), "dirty flag set on return value");

    return val;
  }
#endif

  /// The 8-byte target word
  volatile T value_;
};

} // namespace pmwcas
