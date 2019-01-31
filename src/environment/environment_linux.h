// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <numa.h>
#include <sys/mman.h>
#include <sys/stat.h>

#include <cstdint>
#include <atomic>
#include <memory>
#include <string>
#include <unordered_map>

#include <glog/logging.h>
#include <glog/raw_logging.h>

#include <libpmemobj.h>

#include "include/environment.h"
#include "include/allocator.h"
#include "include/status.h"
#include "util/auto_ptr.h"
#include "util/macros.h"

namespace pmwcas {

class LinuxSharedMemorySegment : public SharedMemorySegment {
 public:
  LinuxSharedMemorySegment();
  ~LinuxSharedMemorySegment();

  static Status Create(unique_ptr_t<SharedMemorySegment>& segment);

  virtual Status Initialize(const std::string& segname, uint64_t size, bool open_existing) override;

  virtual Status Attach(void* base_address = nullptr) override;

  virtual Status Detach() override;

  virtual void* GetMapAddress() override;

  //virtual DumpToFile(const std::string& filename) override;

 private:
  std::string segment_name_;
  uint64_t size_;
  int map_fd_;
  void* map_address_;
};

class LinuxEnvironment : public IEnvironment {
 public:
  LinuxEnvironment();
  virtual ~LinuxEnvironment() {}

  static Status Create(IEnvironment*& environment) {
    int n = posix_memalign(reinterpret_cast<void**>(&environment), kCacheLineSize, sizeof(LinuxEnvironment));
    if(!environment || n != 0) return Status::Corruption("Out of memory");
    new(environment)LinuxEnvironment();
    return Status::OK();
  }

  static void Destroy(IEnvironment* e) {
    LinuxEnvironment* environment = static_cast<LinuxEnvironment*>(e);
    environment->~LinuxEnvironment();
    free(environment);
  }


  virtual uint64_t NowMicros() override;

  virtual uint64_t NowNanos() override;

  virtual uint32_t GetCoreCount() override;

  virtual void Sleep(uint32_t ms_to_sleep) override;

  virtual Status NewRandomReadWriteAsyncFile(const std::string& filename,
      const FileOptions& options, ThreadPool* threadpool, RandomReadWriteAsyncFile** file,
      bool* exists = nullptr) override ;

  virtual Status NewSharedMemorySegment(const std::string& segname, uint64_t size,
                                        bool open_existing, SharedMemorySegment** seg) override;

  virtual Status NewThreadPool(uint32_t max_threads,
                               ThreadPool** pool) override;

  virtual Status SetThreadAffinity(uint64_t core, AffinityPattern affinity_pattern) override;

  virtual Status GetWorkingDirectory(std::string& directory) override;

  virtual Status GetExecutableDirectory(std::string& directory) override;

 private:
  Status SetThreadAffinity(pthread_t thread, uint64_t core, AffinityPattern affinity_pattern);
};

/// A simple thread-local allocator that implements the IAllocator interface.
/// Memory is never returned to the OS, but always retained in thread-local
/// sets to be reused later. Each piece of user-facing memory is accompanied by
/// header that describes the size of the memory block. All memory blocks of
/// the same size are chained together in a hash table that maps memory block
/// sizes to memory block chains of the specified size.
class TlsAllocator : public IAllocator {
 public:
  static const uint64_t MB = 1024 * 1024;
  static const uint64_t kNumaMemorySize = 1000 * MB;
  char** numa_memory_;
  uint64_t* numa_allocated_;

  // The hidden part of each allocated block of memory
  struct Header {
    uint64_t size;
    Header* next;
    char padding[kCacheLineSize - sizeof(size) - sizeof(next)];
    Header() : size(0), next(nullptr) {}
    inline void* GetData() { return (void*)((char*)this + sizeof(*this)); }
  };

  // Chain of all memory blocks of the same size
  struct BlockList {
    Header* head;
    Header* tail;
    BlockList() : head(nullptr), tail(nullptr) {}
    BlockList(Header* h, Header* t) : head(h), tail(t) {}

    inline void* Get() {
      if(head) {
        Header* alloc = head;
        if(alloc == tail) {
          head = tail = nullptr;
        } else {
          head = head->next;
        }
        return alloc->GetData();
      }
      return nullptr;
    }

    inline void Put(Header* header) {
      if(!head) {
        DCHECK(!tail);
        head = tail = header;
      } else {
        Header* old_tail = tail;
        old_tail->next = header;
        tail = header;
        header->next = nullptr;
      }
      DCHECK(head->size == header->size);
    }
  };

  inline Header* ExtractHeader(void* pBytes) {
    return (Header*)((char*)pBytes - sizeof(Header));
  }

  inline std::unordered_map<size_t, BlockList>& GetTlsMap() {
    thread_local std::unordered_map<size_t, BlockList> tls_blocks;
    return tls_blocks;
  }

  struct Slab {
    static const uint64_t kSlabSize = 512 * 1024 * 1024;  // 512MB
    TlsAllocator* tls_allocator;
    uint64_t allocated;
    void* memory;
    Slab() : allocated(0), memory(nullptr) {}
    ~Slab() {}
    inline void* Allocate(size_t n) {
    retry:
      if(memory && allocated + n <= kSlabSize) {
        uint64_t off = allocated;
        allocated += n;
        return (void*)((char*)memory + off);
      } else {
        // Slab full or not initialized yet
        auto node = numa_node_of_cpu(sched_getcpu());
        uint64_t off = __atomic_fetch_add(&tls_allocator->numa_allocated_[node], kSlabSize, __ATOMIC_SEQ_CST);
        memory = tls_allocator->numa_memory_[node] + off;
        LOG_IF(FATAL, off >= tls_allocator->kNumaMemorySize) << "Not enough memory";
        allocated = 0;
        goto retry;
      }
      DCHECK(false);
      return nullptr;
    }
  };

  inline Slab& GetTlsSlab() {
    thread_local Slab slab;
    thread_local bool initialized = false;
    if(!initialized) {
      slab.tls_allocator = this;
      initialized = true;
    }
    return slab;
  }

  /// Try to get something from the TLS set
  inline void* TlsAllocate(size_t nSize) {
    // Align to cache line size
    nSize = (nSize + sizeof(Header) + kCacheLineSize - 1) / kCacheLineSize * kCacheLineSize;
    auto& tls_map = GetTlsMap();
    auto block_list = tls_map.find(nSize - sizeof(Header));
    void* pBytes = nullptr;
    if(block_list != tls_map.end()) {
      pBytes = block_list->second.Get();
    }

    if(!pBytes) {
      // Nothing in the map, try my local memory
      auto& tls_slab = GetTlsSlab();
      pBytes = tls_slab.Allocate(nSize);
      if(pBytes) {
        ((Header*)pBytes)->size = nSize - sizeof(Header);
        ((Header*)pBytes)->next = nullptr;
        pBytes = (void*)((char*)pBytes + sizeof(Header));
      }
    }
    DCHECK(pBytes);
    return pBytes;
  }

 public:
  TlsAllocator() {
    int nodes = numa_max_node() + 1;
    numa_memory_ = (char**)malloc(sizeof(char*) * nodes);
    numa_allocated_ = (uint64_t*)malloc(sizeof(uint64_t) * nodes);
    for(int i = 0; i < nodes; ++i) {
      numa_set_preferred(i);
      numa_memory_[i] = (char *)mmap(
          nullptr, kNumaMemorySize, PROT_READ | PROT_WRITE,
          MAP_ANONYMOUS | MAP_PRIVATE | MAP_HUGETLB | MAP_POPULATE, -1, 0);

      numa_allocated_[i] = 0;
    }
  }

  ~TlsAllocator() {
    int nodes = numa_max_node();
    for(int i = 0; i < nodes; ++i) {
      numa_free(numa_memory_[i], kNumaMemorySize);
    }
  }

  static Status Create(IAllocator*& allocator) {
    int n = posix_memalign(reinterpret_cast<void**>(&allocator), kCacheLineSize, sizeof(TlsAllocator));
    if(n || !allocator) return Status::Corruption("Out of memory");
    new(allocator) TlsAllocator();
    //uint64_t MB = 1024 * 1024;
    //void *m = malloc(8192 * MB);
    //free(m);
    return Status::OK();
  }

  static void Destroy(IAllocator* a) {
    TlsAllocator* allocator = static_cast<TlsAllocator*>(a);
    allocator->~TlsAllocator();
    free(allocator);
  }

  void* Allocate(size_t nSize) {
    void* mem = nullptr;
    void* pBytes = TlsAllocate(nSize);
    DCHECK(pBytes);
    return pBytes;
  }

  void* CAlloc(size_t count, size_t size) {
    /// TODO(tzwang): not implemented yet
    return nullptr;
  }

  void Free(void* pBytes) {
    auto& tls_map = GetTlsMap();
    // Extract the hidden size info
    Header* pHeader = ExtractHeader(pBytes);
    pHeader->next = nullptr;
    DCHECK(pHeader->size);
    auto block_list = tls_map.find(pHeader->size);
    if(block_list == tls_map.end()) {
      tls_map.emplace(pHeader->size, BlockList(pHeader, pHeader));
    } else {
      block_list->second.Put(pHeader);
    }
  }

  void* AllocateAligned(size_t nSize, uint32_t nAlignment) {
    /// TODO(tzwang): take care of aligned allocations
    RAW_CHECK(nAlignment == kCacheLineSize, "unsupported alignment.");
    return Allocate(nSize);
  }

  void FreeAligned(void* pBytes) {
    /// TODO(tzwang): take care of aligned allocations
    return Free(pBytes);
  }

  void* AllocateAlignedOffset(size_t size, size_t alignment, size_t offset) {
    /// TODO(tzwang): not implemented yet
    return nullptr;
  }

  void* AllocateHuge(size_t size) {
    /// TODO(tzwang): not implemented yet
    return nullptr;
  }

  Status Validate(void* pBytes) {
    /// TODO(tzwang): not implemented yet
    return Status::OK();
  }

  uint64_t GetAllocatedSize(void* pBytes) {
    /// TODO(tzwang): not implemented yet
    return 0;
  }

  int64_t GetTotalAllocationCount() {
    /// TODO(tzwang): not implemented yet
    return 0;
  }
};

// A simple wrapper for posix_memalign
class DefaultAllocator : IAllocator {
 public:
  DefaultAllocator() {}
  ~DefaultAllocator() {}

  static Status Create(IAllocator*& allocator) {
    int n = posix_memalign(reinterpret_cast<void**>(&allocator), kCacheLineSize, sizeof(DefaultAllocator));
    if(n || !allocator) return Status::Corruption("Out of memory");
    new(allocator) DefaultAllocator();
    return Status::OK();
  }

  static void Destroy(IAllocator* a) {
    DefaultAllocator * allocator = static_cast<DefaultAllocator*>(a);
    allocator->~DefaultAllocator();
    free(allocator);
  }

  void* Allocate(size_t nSize) {
    void* mem = nullptr;
    int n = posix_memalign(&mem, kCacheLineSize, nSize);
    mem = malloc(nSize);
    return mem;
  }

  void* CAlloc(size_t count, size_t size) {
    /// TODO(tzwang): not implemented yet
    return nullptr;
  }

  void Free(void* pBytes) {
    free(pBytes);
  }

  void* AllocateAligned(size_t nSize, uint32_t nAlignment) {
    RAW_CHECK(nAlignment == kCacheLineSize, "unsupported alignment.");
    return Allocate(nSize);
  }

  void FreeAligned(void* pBytes) {
    return Free(pBytes);
  }

  void* AllocateAlignedOffset(size_t size, size_t alignment, size_t offset) {
    /// TODO(tzwang): not implemented yet
    return nullptr;
  }

  void* AllocateHuge(size_t size) {
    /// TODO(tzwang): not implemented yet
    return nullptr;
  }

  Status Validate(void* pBytes) {
    /// TODO(tzwang): not implemented yet
    return Status::OK();
  }

  uint64_t GetAllocatedSize(void* pBytes) {
    /// TODO(tzwang): not implemented yet
    return 0;
  }

  int64_t GetTotalAllocationCount() {
    /// TODO(tzwang): not implemented yet
    return 0;
  }

};

#define CREATE_MODE_RW (S_IWUSR | S_IRUSR)
POBJ_LAYOUT_BEGIN(allocator);
POBJ_LAYOUT_TOID(allocator, char);
POBJ_LAYOUT_END(allocator);

class PMDKAllocator : IAllocator {
 public:
  PMDKAllocator(PMEMobjpool *pop, const char *file_name): pop(pop), file_name(file_name) {}
  ~PMDKAllocator() {
    pmemobj_close(pop);
  }

  // FIXME: make it configurable
  static constexpr const char * pool_name="pmwcas_pmdk_pool";
  static constexpr const char * layout_name="pmwcas_pmdk_layout";

  static Status Create(IAllocator*& allocator) {
    int n = posix_memalign(reinterpret_cast<void**>(&allocator), kCacheLineSize, sizeof(DefaultAllocator));
    if(n || !allocator) return Status::Corruption("Out of memory");

    PMEMobjpool *tmp_pool;
    if(!FileExists(pool_name)) {
     tmp_pool = pmemobj_create(pool_name, layout_name,
                               (1024*1024*1024) , CREATE_MODE_RW);
     LOG_ASSERT(tmp_pool != nullptr);
    } else {
      tmp_pool = pmemobj_open(pool_name, layout_name);
      LOG_ASSERT(tmp_pool != nullptr);
    }

    new(allocator) PMDKAllocator(tmp_pool, pool_name);
    return Status::OK();
  }

  static bool FileExists(const char *pool_path) {
    struct stat buffer;
    return (stat(pool_path, &buffer) == 0);
  }

  static void Destroy(IAllocator *a) {
    auto* allocator= static_cast<PMDKAllocator*>(a);
    allocator->~PMDKAllocator();
    free(allocator);
  }

  void* Allocate(size_t nSize) override {
    PMEMoid ptr;
    if(pmemobj_zalloc(pop, &ptr, sizeof(char) * nSize, TOID_TYPE_NUM(char))){
      LOG(FATAL) << "POBJ_ALLOC error";
    }
    return pmemobj_direct(ptr);
  }

  void* GetRoot(size_t nSize) {
    return pmemobj_direct(pmemobj_root(pop, nSize));
  }

  PMEMobjpool *GetPool(){
    return pop;
  }

  void PersistPtr(void *ptr, uint64_t size){
    pmemobj_persist(pop, ptr, size);
  }

  void* CAlloc(size_t count, size_t size) {
    return nullptr;
  }

  void Free(void* pBytes) override {
    auto oid_ptr = pmemobj_oid(pBytes);
    TOID(char) ptr_cpy;
    TOID_ASSIGN(ptr_cpy, oid_ptr);
    POBJ_FREE(&ptr_cpy);
  }

  void* AllocateAligned(size_t nSize, uint32_t nAlignment) override {
    RAW_CHECK(nAlignment == kCacheLineSize, "unsupported alignment.");
    return Allocate(nSize);
  }

  void FreeAligned(void* pBytes) override {
    return Free(pBytes);
  }

  void* AllocateAlignedOffset(size_t size, size_t alignment, size_t offset) {
    return nullptr;
  }

  void* AllocateHuge(size_t size) {
    return nullptr;
  }

  Status Validate(void* pBytes) {
    return Status::OK();
  }

  uint64_t GetAllocatedSize(void* pBytes) {
    return 0;
  }

  int64_t GetTotalAllocationCount() {
    return 0;
  }

 private:
  PMEMobjpool *pop;
  const char *file_name;
};

}
