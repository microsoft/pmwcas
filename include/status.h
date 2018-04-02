// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <string>
#include "include/slice.h"

// Return the given status if it is not OK.
#define RETURN_NOT_OK(s) do { \
    ::pmwcas::Status _s = (s); \
    if (!_s.ok()) return _s; \
  } while (0);

namespace pmwcas {

class Status {
 public:
  // Create a success status.
  Status() : code_(kOk), state_(nullptr) { }
  ~Status() {
    delete[] state_;
  }

  // Copy the specified status.
  Status(const Status& s);
  void operator=(const Status& s);
  bool operator==(const Status& rhs) const;
  bool operator!=(const Status& rhs) const;

  // Return a success status.
  static Status OK() {
    return Status();
  }

  // Return error status of an appropriate type.
  static Status NotFound(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kNotFound, msg, msg2);
  }
  // Fast path for not found without malloc;
  static Status NotFound() {
    return Status(kNotFound);
  }
  static Status Corruption(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kCorruption, msg, msg2);
  }
  static Status NotSupported(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kNotSupported, msg, msg2);
  }
  static Status InvalidArgument(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kInvalidArgument, msg, msg2);
  }
  static Status IOError(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kIOError, msg, msg2);
  }
  static Status MergeInProgress() {
    return Status(kMergeInProgress);
  }
  static Status UnableToMerge() {
    return Status(kUnableToMerge);
  }
  static Status Incomplete(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kIncomplete, msg, msg2);
  }
  static Status ShutdownInProgress() {
    return Status(kShutdownInProgress);
  }
  static Status ShutdownInProgress(const Slice& msg,
                                   const Slice& msg2 = Slice()) {
    return Status(kShutdownInProgress, msg, msg2);
  }
  static Status Aborted() {
    return Status(kAborted);
  }
  static Status Aborted(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kAborted, msg, msg2);
  }
  static Status Busy() {
    return Status(kBusy);
  }
  static Status Busy(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kBusy, msg, msg2);
  }
  static Status OutOfMemory() {
    return Status(kOutOfMemory);
  }
  static Status TimedOut() {
    return Status(kTimedOut);
  }
  static Status KeyAlreadyExists() {
    return Status(kKeyAlreadyExists);
  }
  static Status MwCASFailure() {
    return Status(kMwCASFailure);
  }

  // Returns true iff the status indicates success.
  bool ok() const {
    return code() == kOk;
  }

  // Returns true iff the status indicates a NotFound error.
  bool IsNotFound() const {
    return code() == kNotFound;
  }

  // Returns true iff the status indicates a Corruption error.
  bool IsCorruption() const {
    return code() == kCorruption;
  }

  // Returns true iff the status indicates a NotSupported error.
  bool IsNotSupported() const {
    return code() == kNotSupported;
  }

  // Returns true iff the status indicates an InvalidArgument error.
  bool IsInvalidArgument() const {
    return code() == kInvalidArgument;
  }

  // Returns true iff the status indicates an IOError.
  bool IsIOError() const {
    return code() == kIOError;
  }

  // Returns true iff the status indicates Incomplete
  bool IsIncomplete() const {
    return code() == kIncomplete;
  }

  // Returns true iff the status indicates Shutdown In progress
  bool IsShutdownInProgress() const {
    return code() == kShutdownInProgress;
  }

  bool IsTimedOut() const {
    return code() == kTimedOut;
  }

  bool IsAborted() const {
    return code() == kAborted;
  }

  bool IsOutOfMemory() const {
    return code() == kOutOfMemory;
  }

  bool IsKeyAlreadyExists() const {
    return code() == kKeyAlreadyExists;
  }

  // Returns true iff the status indicates that a resource is Busy and
  // temporarily could not be acquired.
  bool IsBusy() const {
    return code() == kBusy;
  }

  bool IsMwCASFailure() const {
    return code() == kMwCASFailure;
  }

  // Return a string representation of this status suitable for printing.
  // Returns the string "OK" for success.
  std::string ToString() const;

  enum Code {
    kOk = 0,
    kNotFound = 1,
    kCorruption = 2,
    kNotSupported = 3,
    kInvalidArgument = 4,
    kIOError = 5,
    kMergeInProgress = 6,
    kIncomplete = 7,
    kShutdownInProgress = 8,
    kTimedOut = 9,
    kAborted = 10,
    kBusy = 11,
    kOutOfMemory = 12,
    kKeyAlreadyExists = 13,
    kUnableToMerge = 14,
    kMwCASFailure = 15,
  };

  Code code() const {
    return code_;
  }
 private:
  // A nullptr state_ (which is always the case for OK) means the message
  // is empty.
  // of the following form:
  //    state_[0..3] == length of message
  //    state_[4..]  == message
  Code code_;
  const char* state_;

  explicit Status(Code _code) : code_(_code), state_(nullptr) {}
  Status(Code _code, const Slice& msg, const Slice& msg2);
  static const char* CopyState(const char* s);
};

inline Status::Status(const Status& s) {
  code_ = s.code_;
  state_ = (s.state_ == nullptr) ? nullptr : CopyState(s.state_);
}
inline void Status::operator=(const Status& s) {
  // The following condition catches both aliasing (when this == &s),
  // and the common case where both s and *this are ok.
  code_ = s.code_;
  if(state_ != s.state_) {
    delete[] state_;
    state_ = (s.state_ == nullptr) ? nullptr : CopyState(s.state_);
  }
}

inline bool Status::operator==(const Status& rhs) const {
  return (code_ == rhs.code_);
}

inline bool Status::operator!=(const Status& rhs) const {
  return !(*this == rhs);
}

}  // namespace pmwcas
