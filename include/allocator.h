// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <cstdint>
#include "include/status.h"

namespace pmwcas {

/// Interface for custom memory allocator plug-in. The PMwCAS library does not1
/// assume a particular allocator, and will use whatever is behind IAllocator to
/// allocate memory. See pmwcas::InitLibrary in /include/pmwcas.h.
class IAllocator {
 public:
  virtual void* Allocate(size_t size) = 0;
  virtual void* AllocateAligned(size_t size, uint32_t alignment) = 0;
  virtual void* AllocateAlignedOffset(size_t size, size_t alignment,
      size_t offset) = 0;
  virtual void* AllocateHuge(size_t size) = 0;
  virtual void* CAlloc(size_t count, size_t size) = 0;
  virtual void Free(void* bytes) = 0;
  virtual void FreeAligned(void* bytes) = 0;
  virtual uint64_t GetAllocatedSize(void* bytes) = 0;
  virtual Status Validate(void* bytes) = 0;
};

} // namespace pmwcas
