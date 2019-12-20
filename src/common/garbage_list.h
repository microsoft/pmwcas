// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <mutex>
#include <atomic>
#include "common/allocator_internal.h"
#include "common/epoch.h"
#include "util/macros.h"
#include "util/atomics.h"
#include "include/allocator.h"

namespace pmwcas {

/// Interface for the GarbageList; used to make it easy to drop is mocked out
/// garbage lists for unit testing. See GarbageList template below for
/// full documentation.
class IGarbageList {
 public:
  typedef void
  (*DestroyCallback)(void* callback_context, void* object);

  IGarbageList() {}

  virtual ~IGarbageList() {}

  virtual Status Initialize(EpochManager* epoch_manager,
      size_t size = 4 * 1024 * 1024) {
    MARK_UNREFERENCED(epoch_manager);
    MARK_UNREFERENCED(size);
    return Status::OK();
  }

  virtual Status Uninitialize() {
    return Status::OK();
  }

  virtual Status Push(void* removed_item, DestroyCallback destroy_callback,
      void* context) = 0;
};

/// Tracks items that have been removed from a data structure but to which
/// there may still be concurrent accesses using the item from other threads.
/// GarbageList works together with the EpochManager to ensure that items
/// placed on the list are only destructed and freed when it is safe to do so.
///
/// Lock-free data structures use this template by creating an instance specific
/// to the type of the item they will place on the list. When an element is
/// has been "removed" from the data structure it should call Push() to
/// transfer responsibility for the item over to the garbage list.
/// Occasionally, Push() operations will check to see if objects on the list are
/// ready for reuse, freeing them up if it is safe to do so. The user of the
/// GarbageList provides a callback that is invoked so custom logic can be used
/// to reclaim resources.
class GarbageList : public IGarbageList {
 public:
  /// Holds a pointer to an object in the garbage list along with the Epoch
  /// in which it was removed and a chain field so that it can be linked into
  /// a queue.
  struct Item {
    /// Epoch in which the #m_removedItem was removed from the data
    /// structure. In practice, due to delay between the actual removal
    /// operation and the push onto the garbage list, #m_removalEpoch may
    /// be later than when the actual remove happened, but that is safe
    /// since the invariant is that the epoch stored here needs to be
    /// greater than or equal to the current global epoch in which the
    /// item was actually removed.
    Epoch removal_epoch;

    /// Function provided by user on Push() called when an object
    /// that was pushed to the list is safe for reclamation. When invoked the
    /// function is passed a pointer to an object that is safe to destroy and
    /// free along with #m_pbDestroyCallbackContext. The function must
    /// perform all needed destruction and release any resources associated
    /// with the object.
    DestroyCallback destroy_callback;

    /// Passed along with a pointer to the object to destroy to
    /// #m_destroyCallback; it threads state to destroyCallback calls so they
    /// can access, for example, the allocator from which the object was
    /// allocated.
    void* destroy_callback_context;

    /// Point to the object that is enqueued for destruction. Concurrent
    /// accesses may still be ongoing to the object, so absolutely no
    /// changes should be made to the value it refers to until
    /// #m_removalEpoch is deemed safe for reclamation by the
    /// EpochManager.
    void* removed_item;
  };
  static_assert(std::is_pod<Item>::value, "Item should be POD");

  /// Construct a GarbageList in an uninitialized state.
  GarbageList()
    : epoch_manager_{}
    , tail_{}
    , item_count_{}
    , items_{} {
  }

  /// Uninitialize the GarbageList (if still initialized) and destroy it.
  virtual ~GarbageList() {
    Uninitialize();
  }

  /// Initialize the GarbageList and associate it with an EpochManager.
  /// This must be called on a newly constructed instance before it
  /// is safe to call other methods. If the GarbageList is already
  /// initialized then it will have no effect.
  ///
  /// \param pEpochManager
  ///      EpochManager that is used to determine when it is safe to reclaim
  ///      items pushed onto the list. Must not be nullptr.
  /// \param nItems
  ///      Number of addresses that can be held aside for pointer stability.
  ///      If this number is too small the system runs the risk of deadlock.
  ///      Must be a power of two.
  ///
  /// \retval S_OK
  ///      The instance is now initialized and ready for use.
  /// \retval S_FALSE
  ///      The instance was already initialized; no effect.
  /// \retval E_INVALIDARG
  ///      \a nItems wasn't a power of two.
  virtual Status Initialize(EpochManager* epoch_manager,
      size_t item_count = 128 * 1024) {
    if(epoch_manager_) return Status::OK();

    if(!epoch_manager) return Status::InvalidArgument("Null pointer");

    if(!item_count || !IS_POWER_OF_TWO(item_count)) {
      return Status::InvalidArgument("items not a power of two");
    }

    size_t nItemArraySize = sizeof(*items_) * item_count;
    posix_memalign((void **)&items_, 64, nItemArraySize);
    if (!items_)
      return Status::Corruption("Out of memory");

    for(size_t i = 0; i < item_count; ++i) new(&items_[i]) Item{};

    item_count_ = item_count;
    tail_ = 0;
    epoch_manager_ = epoch_manager;

    return Status::OK();
  }

  /// Uninitialize the GarbageList and disassociate from its EpochManager;
  /// for each item still on the list call its destructor and free it.
  /// Careful: objects freed by this call will NOT obey the epoch protocol,
  /// so it is important that this thread is only called when it is clear
  /// that no other threads may still be concurrently accessing items
  /// on the list.
  ///
  /// \retval S_OK
  ///      The instance is now uninitialized; resources were released.
  /// \retval S_FALSE
  ///      The instance was already uninitialized; no effect.
  virtual Status Uninitialize() {
    if(!epoch_manager_) return Status::OK();

    for(size_t i = 0; i < item_count_; ++i) {
      Item& item = items_[i];
      if(item.removed_item) {
        item.destroy_callback(
          item.destroy_callback_context,
          item.removed_item);
        item.removed_item = nullptr;
        item.removal_epoch = 0;
      }
    }

    free(items_);

    items_ = nullptr;
    tail_ = 0;
    item_count_ = 0;
    epoch_manager_ = nullptr;

    return Status::OK();
  }

  /// Append an item to the reclamation queue; the item will be stamped
  /// with an epoch and will not be reclaimed until the EpochManager confirms
  /// that no threads can ever access the item again. Once an item is ready
  /// for removal the destruction callback passed to Initialize() will be
  /// called which must free all resources associated with the object
  /// INCLUDING the memory backing the object.
  ///
  /// \param removed_item
  ///      Item to place on the list; it will remain live until
  ///      the EpochManager indicates that no threads will ever access it
  ///      again, after which the destruction callback will be invoked on it.
  /// \param callback
  ///      Function to call when the object that was pushed to the list is safe
  ///      for reclamation. When invoked the, function is passed a pointer to
  ///      an object that is safe to destroy and free along with
  ///      \a pvDestroyCallbackContext. The function must perform
  ///      all needed destruction and release any resources associated with
  ///      the object. Must not be nullptr.
  /// \param context
  ///      Passed along with a pointer to the object to destroy to
  ///      \a destroyCallback; it threads state to destroyCallback calls so
  ///      they can access, for example, the allocator from which the object
  ///      was allocated. Left uninterpreted, so may be nullptr.
  virtual Status Push(void* removed_item, DestroyCallback callback,
      void* context) {
    Epoch removal_epoch = epoch_manager_->GetCurrentEpoch();
    const uint64_t invalid_epoch = ~0llu;

    for(;;) {
      int64_t slot = (tail_.fetch_add(1) - 1) & (item_count_ - 1);

      // Everytime we work through 25% of the capacity of the list roll
      // the epoch over.
      if(((slot << 2) & (item_count_ - 1)) == 0)
        epoch_manager_->BumpCurrentEpoch();

      Item& item = items_[slot];

      Epoch priorItemEpoch = item.removal_epoch;
      if(priorItemEpoch == invalid_epoch) {
        // Someone is modifying this slot. Try elsewhere.
        continue;
      }

      Epoch result = CompareExchange64<Epoch>(&item.removal_epoch,
          invalid_epoch, priorItemEpoch);
      if(result != priorItemEpoch) {
        // Someone else is now modifying the slot or it has been
        // replaced with a new item. If someone replaces the old item
        // with a new one of the same epoch number, that's ok.
        continue;
      }

      // Ensure it is safe to free the old entry.
      if(priorItemEpoch) {
        if(!epoch_manager_->IsSafeToReclaim(priorItemEpoch)) {
          // Uh-oh, we couldn't free the old entry. Things aren't looking
          // good, but maybe it was just the result of a race. Replace the
          // epoch number we mangled and try elsewhere.
          *((volatile Epoch*) &item.removal_epoch) = priorItemEpoch;
          continue;
        }
        item.destroy_callback(item.destroy_callback_context,
                              item.removed_item);
      }

      // Now populate the entry with the new item.
      item.destroy_callback = callback;
      item.destroy_callback_context = context;
      item.removed_item = removed_item;
      *((volatile Epoch*) &item.removal_epoch) = removal_epoch;

      return Status::OK();
    }
  }

  /// Scavenge items that are safe to be reused - useful when the user cannot
  /// wait until the garbage list is full. Currently (May 2016) the only user is
  /// MwCAS' descriptor pool which we'd like to keep small. Tedious to tune the
  /// descriptor pool size vs. garbage list size, so there is this function.
  int32_t Scavenge() {
    const uint64_t invalid_epoch = ~0llu;
    auto max_slot = tail_.load(std::memory_order_relaxed);
    int32_t scavenged = 0;

    for(int64_t slot = 0; slot < item_count_; ++slot) {
      auto& item = items_[slot];
      Epoch priorItemEpoch = item.removal_epoch;
      if(priorItemEpoch == 0 || priorItemEpoch == invalid_epoch) {
        // Someone is modifying this slot. Try elsewhere.
        continue;
      }

      Epoch result = CompareExchange64<Epoch>(&item.removal_epoch,
          invalid_epoch, priorItemEpoch);
      if(result != priorItemEpoch) {
        // Someone else is now modifying the slot or it has been
        // replaced with a new item. If someone replaces the old item
        // with a new one of the same epoch number, that's ok.
        continue;
      }

      if(priorItemEpoch) {
        if(!epoch_manager_->IsSafeToReclaim(priorItemEpoch)) {
          // Uh-oh, we couldn't free the old entry. Things aren't looking
          // good, but maybe it was just the result of a race. Replace the
          // epoch number we mangled and try elsewhere.
          *((volatile Epoch*) &item.removal_epoch) = priorItemEpoch;
          continue;
        }
        item.destroy_callback(item.destroy_callback_context,
                              item.removed_item);
      }

      // Now reset the entry
      item.destroy_callback = nullptr;
      item.destroy_callback_context = nullptr;
      item.removed_item = nullptr;
      *((volatile Epoch*) &item.removal_epoch) = 0;
    }

    return scavenged;
  }

  /// Returns (a pointer to) the epoch manager associated with this garbage list.
  EpochManager* GetEpoch() {
    return epoch_manager_;
  }

 private:
  /// EpochManager instance that is used to determine when it is safe to
  /// free up items. Specifically, it is used to stamp items during Push()
  /// with the current epoch, and it is used in to ensure
  /// that deletion of each item on the list is safe.
  EpochManager* epoch_manager_;

  /// Point in the #m_items ring where the next pushed address will be placed.
  /// Also indicates the next address that will be freed on the next push.
  /// Atomically incremented within Push().
  std::atomic<int64_t> tail_;

  /// Size of the #m_items array. Must be a power of two.
  size_t item_count_;

  /// Ring of addresses the addresses pushed to the list and metadata about
  /// them needed to determine when it is safe to free them and how they
  /// should be freed. This is filled as a ring; when a new Push() comes that
  /// would replace an already occupied slot the entry in the slot is freed,
  /// if possible.
  Item* items_;
};

} // namespace pmwcas
