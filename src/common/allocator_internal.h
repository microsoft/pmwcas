// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <functional>
#include <memory>
#include "include/allocator.h"
#include "include/status.h"

namespace pmwcas {

// Cache line size assumed to be 64 bytes.
static const size_t kCacheLineSize = 64;

/// Holds a pointer to the library-wide allocator. The allocator is set when
/// calling pmwcas::Initialize.
class Allocator {
 public:
  /// Initializes the library-wide allocator, using the specified create
  /// function. The library-wide allocator is stored as a static
  /// std::unique_ptr; in the static destructor, the allocator will be destroyed
  /// using the specified destroy function.
  static Status Initialize(std::function<Status(IAllocator*&)> create,
                           std::function<void(IAllocator*)> destroy);

  /// Get a pointer to the library-wide allocator.
  static IAllocator* Get() {
    return allocator_.get();
  }

  /// Destroys the library-wide allocator.
  static void Uninitialize() {
    allocator_.reset();
  }

 private:
  /// The active library wide allocator.
  static std::unique_ptr<IAllocator, std::function<void(IAllocator*)>> allocator_;

  Allocator() {}
  ~Allocator() {}

  Allocator(const Allocator&) = delete;
  Allocator& operator=(const Allocator&) = delete;
  Allocator(Allocator&&) = delete;
  Allocator& operator=(Allocator&&) = delete;
};

} // namespace pmwcas
