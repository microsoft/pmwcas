// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include "common/allocator_internal.h"
#include "util/auto_ptr.h"
#include "util/macros.h"

#pragma warning(disable: 4172)

namespace pmwcas {

unique_ptr_t<IAllocator> Allocator::allocator_;

Status Allocator::Initialize(std::function<Status(IAllocator*&)> create,
    std::function<void(IAllocator*)> destroy) {
  if(allocator_.get()) {
    return Status::Corruption("Allocator has already been initialized.");
  }
  IAllocator* allocator;
  RETURN_NOT_OK(create(allocator));
  allocator_ = unique_ptr_t<IAllocator>(allocator, destroy);
  return Status::OK();
}

} // namespace pmwcas
