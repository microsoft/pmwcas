// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include "common/environment_internal.h"

#pragma warning(disable: 4172)

namespace pmwcas {

unique_ptr_t<IEnvironment> Environment::environment_;

Status Environment::Initialize(std::function<Status(IEnvironment*&)> create,
    std::function<void(IEnvironment*)> destroy) {
  if(environment_.get()) {
    return Status::Corruption("Environment has already been initialized.");
  }
  IEnvironment* environment;
  RETURN_NOT_OK(create(environment));
  environment_ = unique_ptr_t<IEnvironment>(environment, destroy);
  return Status::OK();
}

std::unordered_map<std::thread::id, Thread::TlsList*> Thread::registry_;
std::mutex Thread::registryMutex_;

void Thread::RegisterTls(uint64_t *ptr, uint64_t val) {
  auto id = std::this_thread::get_id();
  std::unique_lock<std::mutex> lock(registryMutex_);
  if (registry_.find(id) == registry_.end()) {
    registry_.emplace(id, new TlsList);
  }
  registry_[id]->emplace_back(ptr, val);
}

void Thread::ClearTls(bool destroy) {
  std::unique_lock<std::mutex> lock(registryMutex_);
  auto iter = registry_.find(id_);
  if (iter != registry_.end()) {
    auto *list = iter->second;
    for (auto &entry : *list) {
      *entry.first = entry.second;
    }
    if (destroy) {
      delete list;
      registry_.erase(id_);
    } else {
      list->clear();
    }
  }
}

void Thread::ClearRegistry(bool destroy) {
  std::unique_lock<std::mutex> lock(registryMutex_);
  for (auto &r : registry_) {
    auto *list = r.second;
    for (auto &entry : *list) {
      *entry.first = entry.second;
    }
    if (destroy) {
      delete list;
    } else {
      list->clear();
    }
  }
  if (destroy) {
    registry_.clear();
  }
}

} // namespace pmwcas
