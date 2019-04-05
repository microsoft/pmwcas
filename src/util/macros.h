// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <cstdint>
#include <cassert>
#include <string>

namespace pmwcas {

#ifdef _DEBUG
#define verify(exp) assert(exp)
#else
#define verify(exp) ((void)0)
#endif

#define MARK_UNREFERENCED(P) (P)

#define PREFETCH_KEY_DATA(key) _mm_prefetch(key.data(), _MM_HINT_T0)
#define PREFETCH_NEXT_PAGE(delta) _mm_prefetch((char*)(delta->next_page), _MM_HINT_T0)

// Returns true if \a x is a power of two.
#define IS_POWER_OF_TWO(x) (x && (x & (x - 1)) == 0)

// Prevents a type from being copied or moved, both by construction or by assignment.
#define DISALLOW_COPY_AND_MOVE(className) \
    className(const className&) = delete; \
    className& operator=(const className&) = delete; \
    className(className&&) = delete; \
    className& operator=(className&&) = delete

#ifdef GOOGLE_FRAMEWORK
#include <glog/logging.h>
#include <glog/raw_logging.h>
#else
#define DCHECK(...) ;
#define RAW_CHECK(...) ;
#define LOG(...) std::cout
#define LOG_IF(FATAL, ...) std::cout
#define CHECK_EQ(...) std::cout
#endif
} // namespace pmwcas
