// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

// Squelch warnings on initializing arrays; it is new (good) behavior in C++11.
#pragma warning(disable: 4351)

#include <atomic>
#include <cstdint>
#include <list>
#include <mutex>
#include <gtest/gtest_prod.h>
#include "include/status.h"
#include "util/macros.h"

namespace pmwcas {
/// A "timestamp" that is used to determine when it is safe to reuse memory in
/// data structures that are protected with an EpochManager. Epochs are
/// opaque to threads and data structures that use the EpochManager. They
/// may receive Epochs from some of the methods, but they never need to
/// perform any computation on them, other than to pass them back to the
/// EpochManager on future calls (for example, EpochManager::GetCurrentEpoch()
/// and EpochManager::IsSafeToReclaim()).
typedef uint64_t Epoch;

/// Used to ensure that concurrent accesses to data structures don't reuse
/// memory that some threads may be accessing. Specifically, for many lock-free
/// data structures items are "unlinked" when they are removed. Unlinked items
/// cannot be disposed until it is guaranteed that no threads are accessing or
/// will ever access the memory associated with the item again. EpochManager
/// makes it easy for data structures to determine if it is safe to reuse
/// memory by "timestamping" removed items and the entry/exit of threads
/// into the protected code region.
///
/// Practically, a developer "protects" some region of code by marking it
/// with calls to Protect() and Unprotect(). The developer must guarantee that
/// no pointers to internal data structure items are retained beyond the
/// Unprotect() call. Up until Unprotect(), pointers to internal items in
/// a data structure may remain safe for access (see the specific data
/// structures that use this class via IsSafeToReclaim() for documentation on
/// what items are safe to hold pointers to within the protected region).
///
/// Data structure developers must "swap" elements out of their structures
/// atomically and with a sequentially consistent store operation. This ensures
/// that all threads call Protect() in the future will not see the deleted item.
/// Afterward, the removed item must be associated with the current Epoch
/// (acquired via GetCurrentEpoch()). Data structures can use any means to
/// track the association between the removed item and the Epoch it was
/// removed during. Such removed elements must be retained and remain safe for
/// access until IsSafeToReclaim() returns true (which indicates no threads are
/// accessing or ever will access the item again).
class EpochManager {
 public:
  EpochManager();
  ~EpochManager();

  Status Initialize();
  Status Uninitialize();

  /// Enter the thread into the protected code region, which guarantees
  /// pointer stability for records in client data structures. After this
  /// call, accesses to protected data structure items are guaranteed to be
  /// safe, even if the item is concurrently removed from the structure.
  ///
  /// Behavior is undefined if Protect() is called from an already
  /// protected thread. Upon creation, threads are unprotected.
  /// \return S_OK indicates thread may now enter the protected region. Any
  ///      other return indicates a fatal problem accessing the thread local
  ///      storage; the thread may not enter the protected region. Most likely
  ///      the library has entered some non-serviceable state.
  Status Protect() {
    return epoch_table_->Protect(
             current_epoch_.load(std::memory_order_relaxed));
  }

  /// Exit the thread from the protected code region. The thread must
  /// promise not to access pointers to elements in the protected data
  /// structures beyond this call.
  ///
  /// Behavior is undefined if Unprotect() is called from an already
  /// unprotected thread.
  /// \return S_OK indicates thread successfully exited protected region. Any
  ///      other return indicates a fatal problem accessing the thread local
  ///      storage; the thread may not have successfully exited the protected
  ///      region. Most likely the library has entered some non-serviceable
  ///      state.
  Status Unprotect() {
    return epoch_table_->Unprotect(
             current_epoch_.load(std::memory_order_relaxed));
  }

  /// Get a snapshot of the current global Epoch. This is used by
  /// data structures to fetch an Epoch that is recorded along with
  /// a removed element.
  Epoch GetCurrentEpoch() {
    return current_epoch_.load(std::memory_order_seq_cst);
  }

  /// Returns true if an item tagged with \a epoch (which was returned by
  /// an earlier call to GetCurrentEpoch()) is safe to reclaim and reuse.
  /// If false is returned the caller then others threads may still be
  /// concurrently accessed the object inquired about.
  bool IsSafeToReclaim(Epoch epoch) {
    return epoch <= safe_to_reclaim_epoch_.load(std::memory_order_relaxed);
  }

  /// Returns true if the calling thread is already in the protected code
  /// region (i.e., have already called Protected()).
  bool IsProtected() {
    return epoch_table_->IsProtected();
  }

  void BumpCurrentEpoch();

 public:
  void ComputeNewSafeToReclaimEpoch(Epoch currentEpoch);

  /// Keeps track of which threads are executing in region protected by
  /// its parent EpochManager. This table does most of the work of the
  /// EpochManager. It allocates a slot in thread local storage. When
  /// threads enter the protected region for the first time it assigns
  /// the thread a slot in the table and stores its address in thread
  /// local storage. On Protect() and Unprotect() by a thread it updates
  /// the table entry that tracks whether the thread is currently operating
  /// in the protected region, and, if so, a conservative estimate of how
  /// early it might have entered.
  class MinEpochTable {
   public:

    /// Entries should be exactly cacheline sized to prevent contention
    /// between threads.
    enum { CACHELINE_SIZE = 64 };

    /// Default number of entries managed by the MinEpochTable
    static const uint64_t kDefaultSize = 128;

    MinEpochTable();
    Status Initialize(uint64_t size = MinEpochTable::kDefaultSize);
    Status Uninitialize();
    Status Protect(Epoch currentEpoch);
    Status Unprotect(Epoch currentEpoch);

    Epoch ComputeNewSafeToReclaimEpoch(Epoch currentEpoch);

    /// An entry tracks the protected/unprotected state of a single
    /// thread. Threads (conservatively) the Epoch when they entered
    /// the protected region, and more loosely when they left.
    /// Threads compete for entries and atomically lock them using a
    /// compare-and-swap on the #m_threadId member.
    struct Entry {
      /// Construct an Entry in an unlocked and ready to use state.
      Entry()
        : protected_epoch{ 0 },
          last_unprotected_epoch{ 0 },
          thread_id{ 0 } {
      }

      /// Threads record a snapshot of the global epoch during Protect().
      /// Threads reset this to 0 during Unprotect().
      /// It is safe that this value may actually lag the real current
      /// epoch by the time it is actually stored. This value is set
      /// with a sequentially-consistent store, which guarantees that
      /// it precedes any pointers that were removed (with sequential
      /// consistency) from data structures before the thread entered
      /// the epoch. This is critical to ensuring that a thread entering
      /// a protected region can never see a pointer to a data item that
      /// was already "unlinked" from a protected data structure. If an
      /// item is "unlinked" while this field is non-zero, then the thread
      /// associated with this entry may be able to access the unlinked
      /// memory still. This is safe, because the value stored here must
      /// be less than the epoch value associated with the deleted item
      /// (by sequential consistency, the snapshot of the epoch taken
      /// during the removal operation must have happened before the
      /// snapshot taken just before this field was updated during
      /// Protect()), which will prevent its reuse until this (and all
      /// other threads that could access the item) have called
      /// Unprotect().
      std::atomic<Epoch> protected_epoch; // 8 bytes

      /// Stores the approximate epoch under which the thread last
      /// completed an Unprotect(). This need not be very accurate; it
      /// is used to determine if a thread's slot can be preempted.
      Epoch last_unprotected_epoch;        //  8 bytes

      /// ID of the thread associated with this entry. Entries are
      /// locked by threads using atomic compare-and-swap. See
      /// reserveEntry() for details.
      /// XXX(tzwang): on Linux pthread_t is 64-bit
      std::atomic<uint64_t> thread_id;    //  8 bytes

      /// Ensure that each Entry is CACHELINE_SIZE.
      char ___padding[40];

      // -- Allocation policy to ensure alignment --

      /// Provides cacheline aligned allocation for the table.
      /// Note: We'll want to be even smarter for NUMA. We'll want to
      /// allocate slots that reside in socket-local DRAM to threads.
      void* operator new[](uint64_t count) {
#ifdef WIN32
        return _aligned_malloc(count, CACHELINE_SIZE);
#else
        void *mem = nullptr;
        int n = posix_memalign(&mem, CACHELINE_SIZE, count);
        return mem;
#endif
      }

      void operator delete[](void* p) {
#ifdef WIN32
        /// _aligned_malloc-specific delete.
        return _aligned_free(p);
#else
        free(p);
#endif
      }

      /// Don't allow single-entry allocations. We don't ever do them.
      /// No definition is provided so that programs that do single
      /// allocations will fail to link.
      void* operator new(uint64_t count);

      /// Don't allow single-entry deallocations. We don't ever do them.
      /// No definition is provided so that programs that do single
      /// deallocations will fail to link.
      void operator delete(void* p);
    };
    static_assert(sizeof(Entry) == CACHELINE_SIZE,
                  "Unexpected table entry size");

   public:

    Status GetEntryForThread(Entry** entry);
    Entry* ReserveEntry(uint64_t startIndex, uint64_t threadId);
    Entry* ReserveEntryForThread();
    void ReleaseEntryForThread();
    void ReclaimOldEntries();
    bool IsProtected();

   private:

    FRIEND_TEST(EpochManagerTest, Protect);
    FRIEND_TEST(EpochManagerTest, Unprotect);
    FRIEND_TEST(EpochManagerTest, ComputeNewSafeToReclaimEpoch);
    FRIEND_TEST(MinEpochTableTest, Initialize);
    FRIEND_TEST(MinEpochTableTest, Uninitialize);
    FRIEND_TEST(MinEpochTableTest, Protect);
    FRIEND_TEST(MinEpochTableTest, Unprotect);
    FRIEND_TEST(MinEpochTableTest, ComputeNewSafeToReclaimEpoch);
    FRIEND_TEST(MinEpochTableTest, getEntryForThread);
    FRIEND_TEST(MinEpochTableTest, getEntryForThread_OneSlotFree);
    FRIEND_TEST(MinEpochTableTest, reserveEntryForThread);
    FRIEND_TEST(MinEpochTableTest, reserveEntry);

    /// Thread protection status entries. Threads lock entries the first time
    /// the call Protect() (see reserveEntryForThread()). See documentation for
    /// the fields to specifics of how threads use their Entries to guarantee
    /// memory-stability.
    Entry* table_;

    /// The number of entries #m_table. Currently, this is fixed after
    /// Initialize() and never changes or grows. If #m_table runs out
    /// of entries, then the current implementation will deadlock threads.
    uint64_t size_;
  };

  /// A notion of time for objects that are removed from data structures.
  /// Objects in data structures are timestamped with this Epoch just after
  /// they have been (sequentially consistently) "unlinked" from a structure.
  /// Threads also use this Epoch to mark their entry into a protected region
  /// (also in sequentially consistent way). While a thread operates in this
  /// region "unlinked" items that they may be accessing will not be reclaimed.
  std::atomic<Epoch> current_epoch_;

  /// Caches the most recent result of ComputeNewSafeToReclaimEpoch() so
  /// that fast decisions about whether an object can be reused or not
  /// (in IsSafeToReclaim()). Effectively, this is periodically computed
  /// by taking the minimum of the protected Epochs in #m_epochTable and
  /// #current_epoch_.
  std::atomic<Epoch> safe_to_reclaim_epoch_;

  /// Keeps track of which threads are executing in region protected by
  /// its parent EpochManager. On Protect() and Unprotect() by a thread it
  /// updates the table entry that tracks whether the thread is currently
  /// operating in the protected region, and, if so, a conservative estimate
  /// of how early it might have entered. See MinEpochTable for more details.
  MinEpochTable* epoch_table_;

  DISALLOW_COPY_AND_MOVE(EpochManager);
};

/// Enters an epoch on construction and exits it on destruction. Makes it
/// easy to ensure epoch protection boundaries tightly adhere to stack life
/// time even with complex control flow.
class EpochGuard {
 public:
  explicit EpochGuard(EpochManager* epoch_manager)
    : epoch_manager_{ epoch_manager }, unprotect_at_exit_(true) {
    epoch_manager_->Protect();
  }

  /// Offer the option of having protext called on \a epoch_manager.
  /// When protect = false this implies "attach" semantics and the caller should
  /// have already called Protect. Behavior is undefined otherwise.
  explicit EpochGuard(EpochManager* epoch_manager, bool protect)
    : epoch_manager_{ epoch_manager }, unprotect_at_exit_(protect) {
    if(protect) {
      epoch_manager_->Protect();
    }
  }

  ~EpochGuard() {
    if(unprotect_at_exit_ && epoch_manager_) {
      epoch_manager_->Unprotect();
    }
  }

  /// Release the current epoch manger. It is up to the caller to manually
  /// Unprotect the epoch returned. Unprotect will not be called upon EpochGuard
  /// desruction.
  EpochManager* Release() {
    EpochManager* ret = epoch_manager_;
    epoch_manager_ = nullptr;
    return ret;
  }

 private:
  /// The epoch manager responsible for protect/unprotect.
  EpochManager* epoch_manager_;

  /// Whether the guard should call unprotect when going out of scope.
  bool unprotect_at_exit_;
};

} // namespace pmwcas
