// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#ifdef WIN32
#define NOMINMAX
#endif

#ifndef WIN32
#include <x86intrin.h>
#endif

#include <memory>
#include <assert.h>
#include <cstdio>
#include <stddef.h>
#include <string.h>
#include <algorithm>

namespace pmwcas {

class Slice {
 public:
  /// Create an empty slice.
  Slice() : data_(""), size_(0) { }

  /// Create a slice that refers to d[0,n-1].
  Slice(const char* d, size_t n) : data_(d), size_(n) { }

  /// Create a slice that refers to the contents of "s"
  Slice(const std::string& s) : data_(s.data()), size_(s.size()) { }

  /// Create a slice that refers to s[0,strlen(s)-1]
  Slice(const char* s) : data_(s), size_(strlen(s)) { }

  /// Create a single slice from SliceParts using buf as storage.
  /// buf must exist as long as the returned Slice exists.
  Slice(const struct SliceParts& parts, std::string* buf);

  /// Return a pointer to the beginning of the referenced data
  const char* data() const {
    return data_;
  }

  /// Return the length (in bytes) of the referenced data
  size_t size() const {
    return size_;
  }

  /// Return true iff the length of the referenced data is zero
  bool empty() const {
    return size_ == 0;
  }

  /// Return the ith byte in the referenced data.
  /// REQUIRES: n < size()
  char operator[](size_t n) const {
    assert(n < size());
    return data_[n];
  }

  /// Change this slice to refer to an empty array
  void clear() {
    data_ = "";
    size_ = 0;
  }

  /// Drop the first "n" bytes from this slice.
  void remove_prefix(size_t n) {
    assert(n <= size());
    data_ += n;
    size_ -= n;
  }

  /// Drop the last "n" bytes from this slice.
  void remove_suffix(size_t n) {
    assert(n <= size());
    size_ -= n;
  }

  /// Return a string that contains the copy of the referenced data.
  std::string ToString(bool hex = false) const;

  /// Copies the slice to the specified destination.
  void copy(char* dest, size_t dest_size) const {
    assert(dest_size >= size_);
    memcpy(dest, data_, size_);
  }

  /// Copies the specified number of bytes of the slice to the specified
  /// destination.
  void copy(char* dest, size_t dest_size, size_t count) const {
    assert(count <= size_);
    assert(dest_size >= count);
    memcpy(dest, data_, count);
  }

  /// Three-way comparison.  Returns value:
  ///   <  0 iff "*this" <  "b",
  ///   == 0 iff "*this" == "b",
  ///   >  0 iff "*this" >  "b"
  int compare(const Slice& b) const;

  /// Besides returning the comparison value, also sets out parameter "index" to
  /// be the least index
  /// such that "this[index]" != "b[index]".
  int compare_with_index(const Slice& b, size_t& index) const;

  /// Compares the first "len" bytes of "this" with the first "len" bytes of "b".
  int compare(const Slice& b, size_t len) const;

  /// Besides returning the comparison value, also sets out parameter "index" to
  /// be the least index such that "this[index]" != "b[index]".
  int compare_with_index(const Slice& b, size_t len, size_t& index) const;

  /// Return true iff "x" is a prefix of "*this"
  bool starts_with(const Slice& x) const {
    return ((size_ >= x.size_) &&
            (memcmp(data_, x.data_, x.size_) == 0));
  }

  /// Performs a memcmp() and also sets "index" to the index of the first
  /// disagreement (if any) between the buffers being compared.
  static int memcmp_with_index(const void* buf1, const void* buf2, size_t size,
      size_t& index) {
    // Currently using naive implementation, since this is called only during
    // consolidate/split.
    for(index = 0; index < size; ++index) {
      uint8_t byte1 = reinterpret_cast<const uint8_t*>(buf1)[index];
      uint8_t byte2 = reinterpret_cast<const uint8_t*>(buf2)[index];
      if(byte1 != byte2) {
        return (byte1 < byte2) ? -1 : +1;
      }
    }
    return 0;
  }

 public:
  const char* data_;
  size_t size_;
};

inline bool operator==(const Slice& x, const Slice& y) {
  return ((x.size() == y.size()) &&
          (memcmp(x.data(), y.data(), x.size()) == 0));
}

inline bool operator!=(const Slice& x, const Slice& y) {
  return !(x == y);
}

inline int Slice::compare(const Slice& b) const {
  const size_t min_len = (std::min)(size_, b.size_);
  int r = memcmp(data_, b.data_, min_len);
  if(r == 0) {
    if(size_ < b.size_) r = -1;
    else if(size_ > b.size_) r = +1;
  }
  return r;
}

inline int Slice::compare_with_index(const Slice& b, size_t& index) const {
  const size_t min_len = (std::min)(size_, b.size_);
  int r = memcmp_with_index(data_, b.data_, min_len, index);
  if(r == 0) {
    if(size_ < b.size_) r = -1;
    else if(size_ > b.size_) r = +1;
  }
  return r;
}

inline int Slice::compare(const Slice& b, size_t len) const {
  const size_t min_len = (std::min)(size_, b.size_);
  int r = memcmp(data_, b.data_, (std::min)(min_len, len));
  if(r == 0 && min_len < len) {
    if(size_ < b.size_) r = -1;
    else if(size_ > b.size_) r = +1;
  }
  return r;
}

inline int Slice::compare_with_index(const Slice& b, size_t len,
    size_t& index) const {
  const size_t min_len = (std::min)(size_, b.size_);
  int r = memcmp_with_index(data_, b.data_, (std::min)(min_len, len), index);
  if(r == 0 && min_len < len) {
    if(size_ < b.size_) r = -1;
    else if(size_ > b.size_) r = +1;
  }
  return r;
}

}  // namespace pmwcas
