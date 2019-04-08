// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include "include/environment.h"
#include "common/allocator_internal.h"
#include "util/auto_ptr.h"

namespace pmwcas {

unique_ptr_t<RandomReadWriteAsyncFile>
RandomReadWriteAsyncFile::make_unique_ptr_t(RandomReadWriteAsyncFile* p) {
  return unique_ptr_t<RandomReadWriteAsyncFile>(p,
  [](RandomReadWriteAsyncFile* p) {
    Status s = p->Close();
    ALWAYS_ASSERT(s.ok());
    Allocator::Get()->Free(p);
  });
}

}
