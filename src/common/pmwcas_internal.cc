// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include <glog/logging.h>
#include "common/allocator_internal.h"
#include "common/environment_internal.h"
#include "include/pmwcas.h"

namespace pmwcas {

Status InitLibrary(std::function<Status(IAllocator*&)> create_allocator,
    std::function<void(IAllocator*)> destroy_allocator) {
  return Allocator::Initialize(create_allocator, destroy_allocator);
}

Status InitLibrary(std::function<Status(IAllocator*&)> create_allocator,
    std::function<void(IAllocator*)> destroy_allocator,
    std::function<Status(IEnvironment*&)> create_environment,
    std::function<void(IEnvironment*)> destroy_environment) {
  RETURN_NOT_OK(Allocator::Initialize(create_allocator, destroy_allocator));
  return Environment::Initialize(create_environment, destroy_environment);
}

void UninitLibrary() {
  Environment::Uninitialize();
  Allocator::Uninitialize();
}

} // namespace pmwcas
