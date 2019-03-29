// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once 
#include "common/allocator_internal.h"

#ifndef WIN32
#include <unistd.h>
#endif

namespace pmwcas {

 /// A container that keeps one instance of an object per core on the machine.
 /// Get() tries to intelligently return the same instance to the same thread
 /// each time. Contained objects avoid false sharing; each object is guaranteed
 /// to be aligned on cache line boundaries and it doesn't share any lines with
 /// other objects.
template <class T>
class CoreLocal {
  static const uint64_t kCacheLineSize = 64;
 public:
  CoreLocal() :
    objects_(nullptr),
    core_count_(0),
    next_free_object_(0) {
  }

  Status Initialize() {
    RAW_CHECK(!objects_, "already initialized");

#ifdef WIN32
    SYSTEM_INFO sysinfo;
    GetSystemInfo(&sysinfo);
    core_count_ = sysinfo.dwNumberOfProcessors;
#else
    core_count_ = sysconf(_SC_NPROCESSORS_ONLN);
#endif
    RAW_CHECK(core_count_, "invalid core count");

    uint64_t size = core_count_ *
        ((sizeof(T) + kCacheLineSize - 1) & ~(kCacheLineSize - 1));
    Allocator::Get()->AllocateAligned((void **) &objects_, size, kCacheLineSize);
    if (!objects_) {
      return Status::OutOfMemory();
    }
    memset(objects_, 0, size);
    return Status::OK();
  }

  Status Uninitialize() {
    if (!objects_) {
      return Status::Corruption("not initialized?");
    }
    Allocator::Get()->FreeAligned(objects_);
    objects_ = nullptr;
    next_free_object_ = 0;
    return Status::OK();
  }

  /// Returns the object beloning to the calling thread
  T* MyObject() {
    thread_local bool initialized = false;
    thread_local uint32_t idx = 0;
    void *value = nullptr;
    if (initialized) {
      value = (void*)&objects_[idx];
    }

    if (value) {
      // value should point to one of the objects_
      RAW_CHECK((uintptr_t)value >= (uintptr_t)objects_,
          "invalid tls value");
      RAW_CHECK((uintptr_t)value <= (uintptr_t)(objects_ + core_count_ - 1),
          "invalid tls value");
      RAW_CHECK(((uintptr_t)value - (uintptr_t)objects_) % sizeof(T) == 0,
          "invalid tls value");
      return reinterpret_cast<T*>(value);
    }

    // All clear, put a pointer to my object there
    uint32_t obj_idx = next_free_object_.fetch_add(1);
    T* my_object = objects_ + obj_idx;
    idx = obj_idx;
    initialized = true;
    return my_object;
  }

  // Number of objects we're holding so far, not supposed to be used before
  // all threads finished its initialization (ie changing next_free_object_).
  inline uint32_t NumberOfObjects() {
    return next_free_object_.load(std::memory_order_relaxed);
  }

  inline T* GetObject(uint32_t core_id) {
    return objects_ + core_id;
  }

 private:
  /// Storage for the contained objects, one for each core.
  T* objects_;

  /// Max number of cores supported.
  uint32_t core_count_;

  /// Index into objects_ for the next thread who asks for an object
  std::atomic<uint32_t> next_free_object_;
};

}  // namespace pmwcas
