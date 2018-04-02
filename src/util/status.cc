// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include <stdint.h>
#include <stdio.h>
#include "include/status.h"

namespace pmwcas {

#ifdef WIn32
#ifndef snprintf
#define snprintf _snprintf_s
#endif
#endif  // WIN32

const char* Status::CopyState(const char* state) {
  uint32_t size;
  memcpy(&size, state, sizeof(size));
  char* result = new char[size + 4];
  memcpy(result, state, size + 4);
  return result;
}

Status::Status(Code _code, const Slice& msg, const Slice& msg2) : code_(_code) {
  assert(code_ != kOk);
  const uint32_t len1 = static_cast<uint32_t>(msg.size());
  const uint32_t len2 = static_cast<uint32_t>(msg2.size());
  const uint32_t size = len1 + (len2 ? (2 + len2) : 0);
  char* result = new char[size + 4];
  memcpy(result, &size, sizeof(size));
  memcpy(result + 4, msg.data(), len1);
  if(len2) {
    result[4 + len1] = ':';
    result[5 + len1] = ' ';
    memcpy(result + 6 + len1, msg2.data(), len2);
  }
  state_ = result;
}

std::string Status::ToString() const {
  char tmp[30];
  const char* type;
  switch(code_) {
  case kOk:
    return "OK";
  case kNotFound:
    type = "NotFound: ";
    break;
  case kCorruption:
    type = "Corruption: ";
    break;
  case kNotSupported:
    type = "Not implemented: ";
    break;
  case kInvalidArgument:
    type = "Invalid argument: ";
    break;
  case kIOError:
    type = "IO error: ";
    break;
  case kMergeInProgress:
    type = "Merge in progress: ";
    break;
  case kUnableToMerge:
    type = "Cannot merge: ";
    break;
  case kIncomplete:
    type = "Result incomplete: ";
    break;
  case kShutdownInProgress:
    type = "Shutdown in progress: ";
    break;
  case kAborted:
    type = "Operation aborted: ";
    break;
  case kBusy:
    type = "Resource busy: ";
    break;
  default:
    snprintf(tmp, sizeof(tmp), "Unknown code(%d): ",
             static_cast<int>(code()));
    type = tmp;
    break;
  }
  std::string result(type);
  if(state_ != nullptr) {
    uint32_t length;
    memcpy(&length, state_, sizeof(length));
    result.append(state_ + 4, length);
  }
  return result;
}

} // namespace pmwcas
