// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include <glog/logging.h>

#include "common/epoch.h"
#include "common/environment_internal.h"
#include "util/hash.h"

namespace pmwcas {

EpochManager::EpochManager()
    : current_epoch_{ 1 }
    , safe_to_reclaim_epoch_{ 0 }
    , epoch_table_{ nullptr } {
}

EpochManager::~EpochManager() {
  Uninitialize();
}

/**
* Initialize an uninitialized EpochManager. This method must be used before
* it is safe to use an instance via any other members. Calling this on an
* initialized instance has no effect.
*
* \retval S_OK Initialization was successful and instance is ready for use.
* \retval S_FALSE This instance was already initialized; no action was taken.
* \retval E_OUTOFMEMORY Initialization failed due to lack of heap space, the
*      instance was left safely in an uninitialized state.
*/
Status EpochManager::Initialize() {
  if(epoch_table_) return Status::OK();

  auto new_table = reinterpret_cast<MinEpochTable*>
                    (Allocator::Get()->Allocate(sizeof(MinEpochTable)));
  new(new_table) MinEpochTable();

  if(new_table == nullptr) return Status::Corruption("Out of memory");
  RETURN_NOT_OK(new_table->Initialize());

  current_epoch_ = 1;
  safe_to_reclaim_epoch_ = 0;
  epoch_table_ = new_table;

  return Status::OK();
}

/**
* Uninitialize an initialized EpochManager. This method must be used before
* it is safe to destroy or re-initialize an EpochManager. The caller is
* responsible for ensuring no threads are protected (have started a Protect()
* without having completed an Unprotect() and that no threads will call
* Protect()/Unprotect() while the manager is uninitialized; failing to do
* so results in undefined behavior. Calling Uninitialize() on an uninitialized
* instance has no effect.
*
* \return Success or or may return other error codes indicating a
*       failure deallocating the thread local storage used by the EpochManager
*       internally. Even for returns other than success the object is safely
*       left in an uninitialized state, though some thread local resources may
*       not have been reclaimed properly.
* \retval S_OK Success.
* \retval S_FALSE Success; instance was already uninitialized, so no effect.
*/
Status EpochManager::Uninitialize() {
  if(!epoch_table_) return Status::OK();

  Status s = epoch_table_->Uninitialize();

  // Keep going anyway. Even if the inner table fails to completely
  // clean up we want to clean up as much as possible.
  delete epoch_table_;
  epoch_table_ = nullptr;
  current_epoch_ = 1;
  safe_to_reclaim_epoch_ = 0;

  return s;
}

/**
* Increment the current epoch; this should be called "occasionally" to
* ensure that items removed from client data structures can eventually be
* removed. Roughly, items removed from data structures cannot be reclaimed
* until the epoch in which they were removed ends and all threads that may
* have operated in the protected region during that Epoch have exited the
* protected region. As a result, the current epoch should be bumped whenever
* enough items have been removed from data structures that they represent
* a significant amount of memory. Bumping the epoch unnecessarily may impact
* performance, since it is an atomic operation and invalidates a read-hot
* object in the cache of all of the cores.
*
* Only called by GarbageList.
*/
void EpochManager::BumpCurrentEpoch() {
  Epoch newEpoch = current_epoch_.fetch_add(1, std::memory_order_seq_cst);
  ComputeNewSafeToReclaimEpoch(newEpoch);
}

// - private -

/**
* Looks at all of the threads in the protected region and the current
* Epoch and updates the Epoch that is guaranteed to be safe for
* reclamation (stored in #m_safeToReclaimEpoch). This must be called
* occasionally to ensure the system makes garbage collection progress.
* For now, it's called every time bumpCurrentEpoch() is called, which
* might work as a reasonable heuristic for when this should be called.
*/
void EpochManager::ComputeNewSafeToReclaimEpoch(Epoch currentEpoch) {
  safe_to_reclaim_epoch_.store(
    epoch_table_->ComputeNewSafeToReclaimEpoch(currentEpoch),
    std::memory_order_release);
}

// --- EpochManager::MinEpochTable ---

/// Create an uninitialized table.
EpochManager::MinEpochTable::MinEpochTable()
  : table_{ nullptr },
    size_{} {
}

/**
* Initialize an uninitialized table. This method must be used before
* it is safe to use an instance via any other members. Calling this on an
* initialized instance has no effect.
*
* \param size The initial number of distinct threads to support calling
*       Protect()/Unprotect(). This must be a power of two. If the table runs
*       out of space to track threads, then calls may stall. Internally, the
*       table may allocate additional tables to solve this, or it may reclaim
*       entries in the table after a long idle periods of by some threads.
*       If this number is too large it may slow down threads performing
*       space reclamation, since this table must be scanned occasionally to
*       make progress.
* TODO(stutsman) Table growing and entry reclamation are not yet implemented.
* Currently, the manager supports precisely size distinct threads over the
* lifetime of the manager until it begins permanently spinning in all calls to
* Protect().
*
* \retval S_OK Initialization was successful and instance is ready for use.
* \retval S_FALSE Instance was already initialized; instance is ready for use.
* \retval E_INVALIDARG \a size was not a power of two.
* \retval E_OUTOFMEMORY Initialization failed due to lack of heap space, the
*       instance was left safely in an uninitialized state.
* \retval HRESULT_FROM_WIN32(TLS_OUT_OF_INDEXES) Initialization failed because
*       TlsAlloc() failed; the table was safely left in an uninitialized state.
*/
Status EpochManager::MinEpochTable::Initialize(uint64_t size) {
  if(table_) return Status::OK();

  if(!IS_POWER_OF_TWO(size)) return Status::InvalidArgument(
                                        "size not a power of two");

  auto new_table = reinterpret_cast<Entry*>(
      Allocator::Get()->Allocate(sizeof(Entry) * size));
//  auto new_table = new Entry[size];
  if(!new_table) return Status::Corruption("Out of memory");

  // Ensure the table is cacheline size aligned.
  // FIXME(hao): loosen the requirement here
//  assert(!(reinterpret_cast<uintptr_t>(new_table)& (CACHELINE_SIZE - 1)));

  table_ = new_table;
  size_ = size;

  return Status::OK();
}

/**
* Uninitialize an initialized table. This method must be used before
* it is safe to destroy or re-initialize an table. The caller is
* responsible for ensuring no threads are protected (have started a Protect()
* without having completed an Unprotect() and that no threads will call
* Protect()/Unprotect() while the manager is uninitialized; failing to do
* so results in undefined behavior. Calling Uninitialize() on an uninitialized
* instance has no effect.
*
* \return May return other error codes indicating a failure deallocating the
*      thread local storage used by the table internally. Even for returns
*      other than success the object is safely left in an uninitialized state,
*      though some thread local resources may not have been reclaimed
*      properly.
* \retval S_OK Success; resources were reclaimed and table is uninitialized.
* \retval S_FALSE Success; no effect, since table was already uninitialized.
*/
Status EpochManager::MinEpochTable::Uninitialize() {
  if(!table_) return Status::OK();

  size_ = 0;
  delete[] table_;
  table_ = nullptr;
 
  return Status::OK();
}

/**
* Enter the thread into the protected code region, which guarantees
* pointer stability for records in client data structures. After this
* call, accesses to protected data structure items are guaranteed to be
* safe, even if the item is concurrently removed from the structure.
*
* Behavior is undefined if Protect() is called from an already
* protected thread. Upon creation, threads are unprotected.
*
* \param currentEpoch A sequentially consistent snapshot of the current
*      global epoch. It is okay that this may be stale by the time it
*      actually gets entered into the table.
* \return S_OK indicates thread may now enter the protected region. Any
*      other return indicates a fatal problem accessing the thread local
*      storage; the thread may not enter the protected region. Most likely
*      the library has entered some non-serviceable state.
*/
Status EpochManager::MinEpochTable::Protect(Epoch current_epoch) {
  Entry* entry = nullptr;
  RETURN_NOT_OK(GetEntryForThread(&entry));

  entry->last_unprotected_epoch = 0;
#if 1
  entry->protected_epoch.store(current_epoch, std::memory_order_release);
  // TODO: For this to really make sense according to the spec we
  // need a (relaxed) load on entry->protected_epoch. What we want to
  // ensure is that loads "above" this point in this code don't leak down
  // and access data structures before it is safe.
  // Consistent with http://preshing.com/20130922/acquire-and-release-fences/
  // but less clear whether it is consistent with stdc++.
  std::atomic_thread_fence(std::memory_order_acquire);
#else
  entry->m_protectedEpoch.exchange(currentEpoch, std::memory_order_acq_rel);
#endif
  return Status::OK();
}

/**
* Exit the thread from the protected code region. The thread must
* promise not to access pointers to elements in the protected data
* structures beyond this call.
*
* Behavior is undefined if Unprotect() is called from an already
* unprotected thread.
*
* \param currentEpoch A any rough snapshot of the current global epoch, so
*      long as it is greater than or equal to the value used on the thread's
*      corresponding call to Protect().
* \return S_OK indicates thread successfully exited protected region. Any
*      other return indicates a fatal problem accessing the thread local
*      storage; the thread may not have successfully exited the protected
*      region. Most likely the library has entered some non-serviceable
*      state.
*/
Status EpochManager::MinEpochTable::Unprotect(Epoch currentEpoch) {
  Entry* entry = nullptr;
  RETURN_NOT_OK(GetEntryForThread(&entry));

  DCHECK(entry->thread_id.load() == Environment::Get()->GetThreadId());
  entry->last_unprotected_epoch = currentEpoch;
  std::atomic_thread_fence(std::memory_order_release);
  entry->protected_epoch.store(0, std::memory_order_relaxed);
  return Status::OK();
}

/**
* Looks at all of the threads in the protected region and \a currentEpoch
* and returns the latest Epoch that is guaranteed to be safe for reclamation.
* That is, all items removed and tagged with a lower Epoch than returned by
* this call may be safely reused.
*
* \param currentEpoch A snapshot of the current global Epoch; it is okay
*      that the snapshot may lag the true current epoch slightly.
* \return An Epoch that can be compared to Epochs associated with items
*      removed from data structures. If an Epoch associated with a removed
*      item is less or equal to the returned value, then it is guaranteed
*      that no future thread will access the item, and it can be reused
*      (by calling, free() on it, for example). The returned value will
*      never be equal to or greater than the global epoch at any point, ever.
*      That ensures that removed items in one Epoch can never be freed
*      within the same Epoch.
*/
Epoch EpochManager::MinEpochTable::ComputeNewSafeToReclaimEpoch(
  Epoch current_epoch) {
  Epoch oldest_call = current_epoch;
  for(uint64_t i = 0; i < size_; ++i) {
    Entry& entry = table_[i];
    // If any other thread has flushed a protected epoch to the cache
    // hierarchy we're guaranteed to see it even with relaxed access.
    Epoch entryEpoch =
      entry.protected_epoch.load(std::memory_order_acquire);
    if(entryEpoch != 0 && entryEpoch < oldest_call) {
      oldest_call = entryEpoch;
    }
  }
  // The latest safe epoch is the one just before the earlier unsafe one.
  return oldest_call - 1;
}

// - private -

/**
* Get a pointer to the thread-specific state needed for a thread to
* Protect()/Unprotect(). If no thread-specific Entry has been allocated
* yet, then one it transparently allocated and its address is stashed
* in the thread's local storage.
*
* \param[out] entry Points to an address that is populated with
*      a pointer to the thread's Entry upon return. It is illegal to
*      pass nullptr.
* \return S_OK if the thread's entry was discovered or allocated; in such
*      a successful call \a entry points to a pointer to the Entry.
*      Any other return value means there was a problem accessing or
*      setting values in the thread's local storage. The value pointed
*      to by entry remains unchanged, but the library may have entered
*      a non-serviceable state.
*/
Status EpochManager::MinEpochTable::GetEntryForThread(Entry** entry) {
  thread_local Entry *tls = nullptr;
  if(tls) {
    *entry = tls;
    return Status::OK();
  }

  // No entry index was found in TLS, so we need to reserve a new entry
  // and record its index in TLS
  Entry* reserved = ReserveEntryForThread();
  tls = *entry = reserved;

  Thread::RegisterTls((uint64_t*)&tls, (uint64_t)nullptr);

  return Status::OK();
}

uint32_t Murmur3(uint32_t h) {
  h ^= h >> 16;
  h *= 0x85ebca6b;
  h ^= h >> 13;
  h *= 0xc2b2ae35;
  h ^= h >> 16;
  return h;
}

/**
* Allocate a new Entry to track a thread's protected/unprotected status and
* return a pointer to it. This should only be called once for a thread.
*/
EpochManager::MinEpochTable::Entry*
EpochManager::MinEpochTable::ReserveEntryForThread() {
  uint64_t current_thread_id = Environment::Get()->GetThreadId();
  uint64_t startIndex = Murmur3_64(current_thread_id);
  return ReserveEntry(startIndex, current_thread_id);
}

/**
* Does the heavy lifting of reserveEntryForThread() and is really just
* split out for easy unit testing. This method relies on the fact that no
* thread will ever have ID on Windows 0.
* http://msdn.microsoft.com/en-us/library/windows/desktop/ms686746(v=vs.85).aspx
*/
EpochManager::MinEpochTable::Entry*
EpochManager::MinEpochTable::ReserveEntry(uint64_t start_index,
    uint64_t thread_id) {
  for(;;) {
    // Reserve an entry in the table.
    for(uint64_t i = 0; i < size_; ++i) {
      uint64_t indexToTest = (start_index + i) & (size_ - 1);
      Entry& entry = table_[indexToTest];
      if(entry.thread_id == 0) {
        uint64_t expected = 0;
        // Atomically grab a slot. No memory barriers needed.
        // Once the threadId is in place the slot is locked.
        bool success =
        entry.thread_id.compare_exchange_strong(expected,
        thread_id, std::memory_order_relaxed);
        if(success) {
          return &table_[indexToTest];
        }
        // Ignore the CAS failure since the entry must be populated,
        // just move on to the next entry.
      }
    }
    ReclaimOldEntries();
  }
}

bool EpochManager::MinEpochTable::IsProtected() {
  Entry* entry = nullptr;
  Status s = GetEntryForThread(&entry);
  CHECK_EQ(s.ok(), true);
  // It's myself checking my own protected_epoch, safe to use relaxed
  return entry->protected_epoch.load(std::memory_order_relaxed) != 0;
}

void EpochManager::MinEpochTable::ReleaseEntryForThread() {
}

void EpochManager::MinEpochTable::ReclaimOldEntries() {
}

} // namespace pmwcas
