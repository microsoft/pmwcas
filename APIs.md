# PMwCAS APIs

### Descriptor Pool

* Constructor:
```
DescriptorPool(pool_size,                          // Number of descriptors in the pool
               partition_count,                    // Number of descriptor partitions (number of threads) 
               desc_va,                            // Virtual address of a previously allocated pool
               enable_stats = false)               // Enable various stats for PMwCAS
```

### Descriptor operations

* Add an entry to the descriptor:
```
Descriptor::AddEntry(addr,                         // Address of the target word
                     oldval,                       // Expected value
                     newval,                       // New value
                     recycl_policy)                // Memory recycling policy (detailed later)
```

* Allocate memory and add an entry with the new value being the allocated memory block's address:
```
Descriptor::AllocateAndAddEntry(addr,              // Address of the target word
                                oldval,            // Expected value
                                size,              // Size of memory to allocate
                                recycle_policy)    // Memory recycling policy (detailed later)
```
AllocateAndAddEntry is especially useful for changing pointer values.

* Reserve an entry in the descriptor with an empty new value:
```
int Descriptor::ReserveAndAddEntry(addr,           // Address of the target word
                                   oldval,         // Expected value
                                   recycle_policy) // Memory recycling policy (detailed later)
```
`ReserveAndAddEntry` returns the index into the word descriptors in the PMwCAS descriptor. The `newval` field will be left empty. The application may fill it in using `GetNewValuePtr`:
```
GetNewValuePtr(index)  // index - index value returned by ReserveAndAddEntry
```
Note: Currently the application must call `GetNewValuePtr` before adding other new words as the index returned by `ReserveAndAddEntry` might change after adding other words.

* Execute the PMwCAS (if `PMEM` is defined) or volatile MwCAS (if `PMEM` is undefined) operation:
```
bool Descriptor::MwCAS()
```
Returns true if the (P)MwCAS succeeded, false otherwise. 

* Discard the descriptor and abort the operation (applicable only before invoking `MwCAS`):
```
Descriptor::Abort()
```

### Memory recycling policies

`kRecycleOnRecovery`   - Only free memory pointed to by [new_value] upon restart
`kRecycleNever`        - Leave the memory alone, do nothing (default)
`kRecycleAlways`       - Free the memory pointed to by [old/new_value] if the PMwCAS succeeded/failed
`kRecycleOldOnSuccess` - Free only the memory pointed to by [old value] if the PMwCAS succeeded
`kRecycleNewOnFailure` - Free only the memory pointed to by [new value] if the PMwCAS succeeded
