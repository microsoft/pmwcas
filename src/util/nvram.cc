// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include "nvram.h"

namespace pmwcas {
#ifdef PMEM
uint64_t NVRAM::write_delay_cycles = 0;
double NVRAM::write_byte_per_cycle = 0;
bool NVRAM::use_clflush = false;
#endif
}  // namespace pmwcas
