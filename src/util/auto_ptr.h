// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <functional>
#include <memory>
#include "common/allocator_internal.h"

namespace pmwcas {

template<typename T>
using unique_ptr_t = std::unique_ptr<T, std::function<void(T*)>>;

template <typename T>
unique_ptr_t<T> make_unique_ptr_t(T* p) {
  return unique_ptr_t<T> (p,
  [](T* t) {
    t->~T();
    Allocator::Get()->Free(t);
  });
}

template <typename T>
unique_ptr_t<T> make_unique_ptr_aligned_t(T* p) {
  return unique_ptr_t<T> (p,
  [](T* t) {
    t->~T();
    Allocator::Get()->FreeAligned(t);
  });
}

/// Allocate memory without concern for alignment.
template <typename T>
unique_ptr_t<T> alloc_unique(size_t size) {
  return make_unique_ptr_t<T>(reinterpret_cast<T*>(
      Allocator::Get()->Allocate(size)));
}

/// Allocate memory, aligned at the specified alignment.
template <typename T>
unique_ptr_t<T> alloc_unique_aligned(size_t size, size_t alignment) {
  return make_unique_ptr_aligned_t<T>(reinterpret_cast<T*>(
      Allocator::Get()->AllocateAligned(size, alignment)));
}

template<typename T>
using shared_ptr_t = std::shared_ptr<T>;

}
