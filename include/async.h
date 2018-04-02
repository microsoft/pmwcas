// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include "include/allocator.h"
#include "include/status.h"

namespace pmwcas {

class IAsyncContext {
 public:

  IAsyncContext()
    : from_deep_copy_{ false } {
  }

  virtual ~IAsyncContext() {
  }

  /// Duplicate this context on the heap and return a pointer to it. Used
  /// by blocking calls to create a copy of this context that is guaranteed
  /// to persist across the operation even when the call stack that
  /// induced the blocking operation is unwound.
  ///
  /// For performance purposes, contexts are initially stack allocated so as to
  /// avoid allocation overhead if the operation does not go async. DeepCopy
  /// is called only if the operation is preparing to go async.
  ///
  /// WARNING: there are two serious subtleties to this call that callers and
  /// implementors should be aware of:
  /// 1. If this context contains contexts nested within those DeepCopy is
  ///    invoked on those as well, so that all of the contexts transitively
  ///    nested within are copied.
  /// 2. If this context already came from a prior DeepCopy, then the call
  ///    should return this same context without copying it. This is to handle
  ///    the case that this context was already copied to the heap by a prior
  ///    operation that went asynchronous but which didn't invoke the callback
  ///    that cleans up the copy of the context. For example, this can happen
  ///    if an operation is restarted after an asynchronous IO: if the
  ///    operation goes asynchronous again, then the callback context
  ///    shouldn't be copied to the heap a second time.
  ///
  /// \param context_copy
  ///      Will be populated with a copy of this object. Must not be nullptr.
  ///      If 'this' was already a product of a prior DeepCopy, then it is
  ///      returned rather than a copy.
  ///
  /// \return
  ///      Ok status if this context was copied, error otherwise.
  virtual Status DeepCopy(IAsyncContext** context_copy) = 0;

  /// Delete this context from the heap and all of the ICallerAsyncContexts
  /// that it refers to. Used by blocking calls to delete a deep copy of the
  /// context in the case that the callback can't be called. Specifically,
  /// this is used to destroy a chain of contexts in the case that DeepCopy()
  /// was successful, but some other part of scheduling an asynchronus
  /// operation failed.  This should only be called when the user can still be
  /// notified of the failure on the synchronous code path. Silently deleting
  /// a callback chain when user code is expecting a callback will result in
  /// deadlock.
  ///
  /// \return
  ///      Ok status if this context was copied, error otherwise.
  virtual Status DeepDelete() = 0;

  /// Whether the internal state for the async context has been copied to a
  /// heap-allocated memory block.
  bool from_deep_copy_;
};

} // namespace deuteornomy
