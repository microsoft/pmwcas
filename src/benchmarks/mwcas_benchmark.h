// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#define NOMINMAX
#include <inttypes.h>

#include <gtest/gtest.h>
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "glog/raw_logging.h"

#ifdef WIN32
#include "environment/environment_windows.h"
#else
#include "environment/environment_linux.h"
#endif
#include "benchmarks/benchmark.h"
#include "include/pmwcas.h"
#include "mwcas/mwcas.h"
#include "util/auto_ptr.h"

namespace pmwcas {
typedef MwcTargetField<uint64_t> CasPtr;

}  // namespace pmwcas
