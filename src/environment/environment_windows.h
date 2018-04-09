// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#define NOMINMAX
#include <Windows.h>
#undef ERROR // Avoid collision of ERROR definition in Windows.h with glog 
#include <cstdint>
#include <atomic>
#include <memory>
#include <string>
#include <unordered_map>

#include <glog/logging.h>
#include <glog/raw_logging.h>

#include "include/environment.h"
#include "include/allocator.h"
#include "include/status.h"
#include "util/auto_ptr.h"
#include "util/macros.h"

namespace pmwcas {

class WindowsAsyncIOHandler : public AsyncIOHandler {
 public:
  WindowsAsyncIOHandler();
  ~WindowsAsyncIOHandler();

  Status Initialize(ThreadPool* threadpool, HANDLE file_handle,
      PTP_CALLBACK_ENVIRON environment, ThreadPoolPriority priority);

  virtual Status ScheduleRead(uint8_t* buffer, size_t offset, uint32_t length,
      AsyncIOHandler::AsyncCallback callback, IAsyncContext* context);

  virtual Status ScheduleWrite(uint8_t* buffer, size_t offset, uint32_t length,
      AsyncIOHandler::AsyncCallback callback, IAsyncContext* context);

 private:
  struct IOCallbackContext {
    IOCallbackContext()
      : overlapped{}
      , handler{}
      , caller_context{}
      , callback{}
      , bytes_transferred{}  {
    }

    // WARNING: overlapped must be the first field in IOCallbackContext. It is
    // used for abusive pointer casting in IOCompletionSpringboard().

    /// The overlapped structure for Windows IO
    OVERLAPPED overlapped;

    /// The IO threadpool handler used for this IO
    WindowsAsyncIOHandler* handler;

    /// Caller callback context forwarded by #callback
    IAsyncContext* caller_context;

    /// The caller's asynchronous callback function
    AsyncIOHandler::AsyncCallback callback;

    /// Used in the case that an IO might finish sychronously. Used to relay the
    /// correct amount of bytes transferred by sync IO to the task that finishes
    /// the IO asynchronously.
    size_t bytes_transferred;
  };

  enum class OperationType : uint8_t { Read, Write };

  Status ScheduleOperation(OperationType operationType, void* buffer,
      size_t offset, uint32_t length, AsyncIOHandler::AsyncCallback callback,
      IAsyncContext* context);

  /// Invoked whenever an asynchronous IO completes; needed because Windows
  /// asynchronous IOs are tied to a specific TP_IO object. As a result,
  /// we allocate pointers for a per-operation callback along with its
  /// OVERLAPPED structure. This allows us to call a specific function in
  /// response to each IO, without having to create a TP_IO for each of them.
  static void CALLBACK IOCompletionCallback(PTP_CALLBACK_INSTANCE instance,
      PVOID context, PVOID overlapped, ULONG ioResult, ULONG_PTR bytesTransferred,
      PTP_IO io);

  /// Used as a springboard to complete an IO asynchronously in the case that
  /// scheduling the IO finishes synchronously. Windows ReadFile/WriteFile could
  /// finish synchronously even when using the PTP IO threadpool. We have seen
  /// this happen on VMs but never on raw hardware. To enforce the
  /// "always-async" contract schedule the completion on a separate thread using
  /// this function.
  static Status FinishSyncIOAsyncTask(void* context);

 private:
  /// Reference to the file handle for IO
  HANDLE file_handle_;

  /// The windows PTP threadpool used for IO completion port polling and windows
  /// async IO
  PTP_IO io_object_;

  /// Back reference to the parent threadpool. Used to schedule an async
  /// completion in case the IO should finish synchronously
  ThreadPool* threadpool_;

  /// Priority at which IOs are scheduled on this handler
  ThreadPoolPriority io_priority_;
};

class WindowsPtpThreadPool : public ThreadPool {
 public:
  WindowsPtpThreadPool();
  ~WindowsPtpThreadPool();

  static Status Create(uint32_t max_threads,
      unique_ptr_t<ThreadPool>& threadpool);

  Status Initialize(uint32_t max_threads);

  virtual Status Schedule(ThreadPoolPriority priority, Task task,
      void* task_argument);

  virtual Status ScheduleTimer(ThreadPoolPriority priority, Task task, 
      void* task_argument, uint32_t ms_period, void** timer_handle);

  virtual Status CreateAsyncIOHandler(ThreadPoolPriority priority,
      const File& file, unique_ptr_t<AsyncIOHandler>& async_io);

 private:
  /// Describes a task that should be invoked. Created and enqueued in
   /// ScheduleTask(); dispatched and freed in TaskStartSpringboard().
  struct TaskInfo {
    TaskInfo()
      : task{}
      , task_parameters{} {
    }

    /// The task to be invoked when the work item is issued by the pool.
    Task task;

    /// Argument passed into #m_task when it is called.
    void* task_parameters;
  };

  /// Called asynchronously by a thread from #m_pool whenever the thread pool
  /// starts to execute a task scheduled via ScheduleTask(). Just determines
  /// which routine was requested for execution and calls it.
  static void CALLBACK TaskStartSpringboard(PTP_CALLBACK_INSTANCE instance,
      PVOID parameter, PTP_WORK work);

  TP_CALLBACK_PRIORITY TranslatePriority(ThreadPoolPriority priority);

  /// A Window Thread Pool object that is used to run asynchronous IO
  /// operations (and callbacks) and other tasks  (scheduled via
  /// ScheduleTask()).
  PTP_POOL pool_;

  /// An environment that associates Windows Thread Pool IO and Task objects
  /// to #m_pool. There is one environment for each priority in the
  /// work queue. AsyncIOFileWrappers and scheduled tasks are associated
  /// with one of these environments to schedule them for execution and
  /// to assign them a relative priority to other enqueued tasks.
  TP_CALLBACK_ENVIRON callback_environments_[size_t(ThreadPoolPriority::Last)];

  /// The cleanup group associated with all environments and the thread pool.
  PTP_CLEANUP_GROUP cleanup_group_;

  /// Maximum number of threads the thread pool should allocate.
  uint64_t max_threads_;
};

class WindowsRandomReadRandomWriteFile : public RandomReadWriteAsyncFile {
 public:
  WindowsRandomReadRandomWriteFile();
  ~WindowsRandomReadRandomWriteFile();

  static Status Create(unique_ptr_t<RandomReadWriteAsyncFile>& file);

  uint64_t GetFileIdentifier() const;

  virtual bool DirectIO() override;

  virtual size_t GetAlignment() override;

  virtual Status Open(const std::string& filename, const FileOptions& options,
      ThreadPool* threadpool) override;

  virtual Status Close() override;

  virtual Status Delete() override;

  virtual Status Read(size_t offset, uint32_t length, uint8_t* buffer,
      const IAsyncContext& context,
      RandomReadWriteAsyncFile::AsyncCallback callback) override;

  virtual Status Write(size_t offset, uint32_t length, uint8_t* buffer,
      const IAsyncContext& context,
      RandomReadWriteAsyncFile::AsyncCallback callback) override;

 private:
  struct AsyncIOContext : public IAsyncContext {
    IAsyncContext* context;

    RandomReadWriteAsyncFile::AsyncCallback callback;

    uint8_t* buffer;

    size_t bytes_to_transfer;

    WindowsRandomReadRandomWriteFile* file;

    AsyncIOContext(IAsyncContext* caller_context,
        RandomReadWriteAsyncFile::AsyncCallback caller_callback,
        uint8_t* caller_buffer, size_t caller_bytes_to_transfer,
        WindowsRandomReadRandomWriteFile* caller_file)
        : IAsyncContext()
        , context{ caller_context }
        , callback{ caller_callback }
        , buffer{ caller_buffer }
        , bytes_to_transfer { caller_bytes_to_transfer }
        , file{ caller_file } {
    }

    virtual Status DeepCopy(IAsyncContext** context_copy) {
      if(from_deep_copy_) {
        *context_copy = this;
        return Status::OK();
      }
      *context_copy = nullptr;

      unique_ptr_t<AsyncIOContext> io_context =
        alloc_unique<AsyncIOContext>(sizeof(AsyncIOContext));
      if(!io_context.get()) return Status::OutOfMemory();

      IAsyncContext* caller_context_copy{};
      RETURN_NOT_OK(context->DeepCopy(&caller_context_copy));

      io_context->file = file;
      io_context->bytes_to_transfer = bytes_to_transfer;
      io_context->from_deep_copy_ = true;
      io_context->context = caller_context_copy;
      io_context->callback = callback;
      io_context->buffer = buffer;
      *context_copy = io_context.release();
      return Status::OK();
    }

    virtual Status DeepDelete() {
      if(!from_deep_copy_) {
        return Status::OK();
      }
      Status s = context->DeepDelete();
      if(!s.ok()) return s;
      Allocator::Get()->Free(this);
      return Status::OK();
    }
  };

  struct BlockingIOContext : public IAsyncContext {
    HANDLE continuation_handle;
    WindowsRandomReadRandomWriteFile* file_instance;
    Status status;
    IAllocator* deep_copy_allocator;

    BlockingIOContext(HANDLE caller_handle,
                      WindowsRandomReadRandomWriteFile* caller_file)
      : continuation_handle{ caller_handle }
      , file_instance{ caller_file }
      , status{} {
    }

    virtual Status DeepCopy(IAsyncContext** context_copy,
                            IAllocator* allocator) {
      if(from_deep_copy_) {
        *context_copy = this;
        return Status::OK();
      }
      *context_copy = nullptr;

      BlockingIOContext* io_context = reinterpret_cast<BlockingIOContext*>(
          allocator->Allocate(sizeof(BlockingIOContext)));
      if(!io_context) return Status::OutOfMemory();
      unique_ptr_t<BlockingIOContext> context_guard(
      io_context, [=](BlockingIOContext* c) {
        allocator->Free(c);
      });

      context_guard->deep_copy_allocator = allocator;
      context_guard->file_instance = file_instance;
      context_guard->continuation_handle = continuation_handle;
      context_guard->from_deep_copy_ = true;
      *context_copy = context_guard.release();

      return Status::OK();
    }

    virtual Status DeepDelete() {
      if(!from_deep_copy_) {
        return Status::OK();
      }
      deep_copy_allocator->Free(this);
      return Status::OK();
    }
  };

  Status GetDeviceAlignment(const std::string& filename, size_t& alignment);

  static void AsyncReadCallback(IAsyncContext* context, Status status,
      size_t transfer_size);

  static void AsyncWriteCallback(IAsyncContext* context, Status status,
      size_t transfer_size);

  static void BlockingCallback(IAsyncContext* context, Status status,
      size_t transfer_size);

  bool direct_io_;
  HANDLE file_handle_;
  HANDLE map_handle_;
  uint8_t* map_address_;
  size_t device_alignment_;
  std::string filename_;
  ThreadPool* threadpool_;
  unique_ptr_t<AsyncIOHandler> async_io_handler_;
};

class WindowsSharedMemorySegment : public SharedMemorySegment {
 public:
  WindowsSharedMemorySegment();
  ~WindowsSharedMemorySegment();

  /// Allocate memory for a Windows shared memory segment and return it
  /// attached to a unique_ptr
  static Status Create(unique_ptr_t<SharedMemorySegment>& segment);

  /// Initialize or create a named shared memory segment. If open_existing
  /// is true, attempts to open an existing segment, otherwise creates a new
  /// segment. This method should be used for creating a new DRAM mapping or
  /// attaching to a an existing mapping on a DAX volume. See CreateDax for
  /// creating a new segment on a DAX volume.
  virtual Status Initialize(const std::string& segname, uint64_t size,
      bool open_existing) override;

  /// Creates a new segment on a DAX volume. Will query volume information
  /// (using filename) to check if the volume is indeed DAX. Existing DAX
  /// segments should be opened using the Initialize function.
  virtual Status CreateDax(const std::string& segname,
    const std::string& filename, uint64_t size) override;

  /// Attach to a segment that was opened using Initialize or CreateDax.
  virtual Status Attach(void* base_address = nullptr) override;

  /// Detach from a segment.
  virtual Status Detach() override;

  /// Retrieve the start address for the segment's address space.
  virtual void* GetMapAddress() override;

 private:
  /// Name (identifier) of the shared memory segment.
  std::string segment_name_;

  /// Size of the shared memory segment.
  uint64_t size_;

  /// Handle to the mapped space.
  HANDLE map_handle_;

  /// If mapped space is a DAX volume, stores the handle of the file on the
  /// volume.
  HANDLE map_file_handle_;

  /// The start address of the mapped memory space.
  void* map_address_;
};

class WindowsEnvironment : public IEnvironment {
 public:
  WindowsEnvironment();
  virtual ~WindowsEnvironment() {}

  static Status Create(IEnvironment*& environment) {
    environment = reinterpret_cast<WindowsEnvironment*>(_aligned_malloc(
                    sizeof(WindowsEnvironment), kCacheLineSize));
    if(!environment) return Status::Corruption("Out of memory");
    new(environment)WindowsEnvironment();

    return Status::OK();
  }

  static void Destroy(IEnvironment* e) {
    WindowsEnvironment* environment = static_cast<WindowsEnvironment*>(e);
    environment->~WindowsEnvironment();
    _aligned_free(environment);
  }


  virtual uint64_t NowMicros() override;

  virtual uint64_t NowNanos() override;

  virtual uint32_t GetCoreCount() override;

  virtual void Sleep(uint32_t ms_to_sleep) override;

  virtual Status NewRandomReadWriteAsyncFile(const std::string& filename, 
      const FileOptions& options, ThreadPool* threadpool,
      RandomReadWriteAsyncFile** file, bool* exists = nullptr) override ;

  virtual Status NewSharedMemorySegment(const std::string& segname,
      uint64_t size, bool open_existing, SharedMemorySegment** seg) override;

  virtual Status NewDaxSharedMemorySegment(const std::string& segname,
      const std::string& filename, uint64_t size,
      SharedMemorySegment** seg) override;

  virtual Status NewThreadPool(uint32_t max_threads,
      ThreadPool** pool) override;

  virtual Status SetThreadAffinity(uint64_t core,
      AffinityPattern affinity_pattern) override;

  virtual Status GetWorkingDirectory(std::string& directory) override;

  virtual Status GetExecutableDirectory(std::string& directory) override;

  virtual Status AllocateTlsIndex(uint32_t& index) override;

  virtual Status FreeTlsIndex(uint32_t index) override;

  virtual Status GetTlsValue(uint32_t index, void** value) override;

  virtual Status SetTlsValue(uint32_t index, void* value) override;

 private:
  Status SetThreadAffinity(HANDLE thread, uint64_t core,
      AffinityPattern affinity_pattern);

 private:
  /// Holds the windows performance counter frequency used for timing methods
  uint64_t perf_counter_frequency_;

  /// Struct that contains information about the current computer system. We
  /// call GetSystemInfo() in the constructor, and then read the number of
  /// processors off this field later.
  SYSTEM_INFO sys_info_;
};

/// A default heap allocator implementing the IMemoryManager interace. To be
/// used if no user-provided allocator is passed into the pmwcas librarty during
// initialization
class DefaultAllocator : public IAllocator {
 public:
  DefaultAllocator()
    : heap_(NULL),
      allocation_count_{} {
  }

  ~DefaultAllocator() { }

  static Status Create(IAllocator*& allocator) {
    allocator = reinterpret_cast<DefaultAllocator*>(_aligned_malloc(
                  sizeof(DefaultAllocator), kCacheLineSize));
    if(!allocator) return Status::Corruption("Out of memory");
    new(allocator)DefaultAllocator();

    static_cast<DefaultAllocator*>(allocator)->heap_ = ::GetProcessHeap();
    if(!static_cast<DefaultAllocator*>(allocator)->heap_) {
      return Status::Aborted("GetProcessHeap error: " + GetLastError());
    }

    return Status::OK();
  }

  static void Destroy(IAllocator* a) {
    DefaultAllocator* allocator = static_cast<DefaultAllocator*>(a);
    allocator->~DefaultAllocator();
    _aligned_free(allocator);
  }

  __checkReturn void* Allocate(size_t nSize) {
#ifdef _DEBUG
    allocation_count_ += nSize;
#endif
    return ::HeapAlloc(heap_, 0, nSize);
  }

  void* CAlloc(size_t count, size_t size) {
    return calloc(count, size);
  }

  __checkReturn void Free(void* pBytes) {
#ifdef _DEBUG
    uint64_t size = GetAllocatedSize(pBytes);
    allocation_count_ -= size;
#endif
    ::HeapFree(heap_, 0, pBytes);
  }

  __checkReturn void* AllocateAligned(size_t nSize, uint32_t nAlignment) {
    return _aligned_malloc(nSize, nAlignment);
  }

  __checkReturn void FreeAligned(void* pBytes) {
    _aligned_free(pBytes);
  }

  __checkReturn uint64_t GetAllocatedSize(void* pBytes) {
    return ::HeapSize(heap_, 0, pBytes);
  }

  void* AllocateAlignedOffset(size_t size, size_t alignment, size_t offset) {
    return _aligned_offset_malloc(size, alignment, offset);
  }

  void* AllocateHuge(size_t size) {
    return nullptr;
  }

  __checkReturn Status Validate(__in void* pBytes) {
    BOOL bIsValue = ::HeapValidate(heap_, 0, pBytes);
    if(!bIsValue) {
      // Return E_FAIL, GetLastError will not work.
      // From http://msdn.microsoft.com/en-us/library/windows/desktop/aa366708(v=vs.85).aspx
      //
      // "The HeapValidate function does not set the thread's last error value.
      // There is no extended error information for this function; do not call
      // GetLastError."
      return Status::Aborted("HeapValidate error");
    }

    return Status::OK();
  }

  __int64 GetTotalAllocationCount() {
    return allocation_count_;
  }

 private:
  HANDLE heap_;
  std::atomic<uint64_t> allocation_count_;
};

/// A simple thread-local allocator that overlays the default heap allocator.
/// Memory is never returned to the OS, but always retained in thread-local
/// sets to be reused later. Each piece of user-facing memory is accompanied by
/// header that describes the size of the memory block. All memory blocks of
/// the same size are chained together in a hash table that maps memory block
/// sizes to memory block chains of the specified size.
class TlsAllocator : public IAllocator {
 private:
  // The hidden part of each allocated block of memory
  struct Header {
    uint64_t size;
    Header* next;
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
    uint64_t allocated;
    uint64_t size;
    void* memory;
    Slab() : allocated(0), size(0), memory(nullptr) {}
    ~Slab() {
      /*
      if(memory) {
        VirtualFree(memory, 0, MEM_RELEASE);
      }
      */
    }
    inline void Initialize(uint64_t s) {
      size = s;
      uint16_t node;
      PROCESSOR_NUMBER pnum;
      GetCurrentProcessorNumberEx(&pnum);
      bool success = GetNumaProcessorNodeEx(&pnum, &node);
      memory = (void*)VirtualAllocExNuma(GetCurrentProcess(),
        nullptr, s, MEM_RESERVE | MEM_COMMIT, PAGE_READWRITE, node);
      memset(memory, 0, s);
      LOG(INFO) << "Initialized slab";
    }
    inline void* Allocate(size_t n) {
      if(allocated + n <= size) {
        uint64_t off = allocated;
        allocated += n;
        return (void*)((char*)memory + off);
      }
      return nullptr;
    }
  };

  inline Slab& GetTlsSlab() {
    thread_local Slab slab;
    static const uint64_t kSlabSize = 512 * 1024 * 1024;  // 512MB
    if(slab.size == 0) {
      //slab.Initialize(kSlabSize);
      slab.size = kSlabSize;
      slab.memory = default_->AllocateAligned(kSlabSize, kCacheLineSize);
      memset(slab.memory, 0, kSlabSize);
    }
    return slab;
  }

  /// Try to get something from the TLS set
  inline void* TlsAllocate(size_t nSize) {
    // Align to cache line size
    nSize = (nSize + sizeof(Header) + kCacheLineSize - 1) /
      kCacheLineSize * kCacheLineSize;
    auto& tls_map = GetTlsMap();
    auto& block_list = tls_map.find(nSize - sizeof(Header));
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

    return pBytes;
  }

  // Allocate from the default allocator, when there's nothing in the TLS set
  inline void* AllocateAndInitialize(size_t nSize) {
    void* pBytes = default_->AllocateAligned(nSize + sizeof(Header),
        kCacheLineSize);
    ((Header*)pBytes)->size = nSize;
    ((Header*)pBytes)->next = nullptr;
    return (void*)((char*)pBytes + sizeof(Header));
  }

  DefaultAllocator* default_;

 public:
  TlsAllocator() {
    Status s = DefaultAllocator::Create((IAllocator*&)default_);
    DCHECK(s.ok());
  }

  ~TlsAllocator() { DefaultAllocator::Destroy(default_); }

  static Status Create(IAllocator*& allocator) {
    allocator = reinterpret_cast<TlsAllocator*>(_aligned_malloc(
                  sizeof(TlsAllocator), kCacheLineSize));
    if(!allocator) return Status::Corruption("Out of memory");
    new(allocator) TlsAllocator();
    return Status::OK();
  }

  static void Destroy(IAllocator* a) {
    TlsAllocator* allocator = static_cast<TlsAllocator*>(a);
    allocator->~TlsAllocator();
    _aligned_free(allocator);
  }

  __checkReturn void* Allocate(size_t nSize) {
    void* pBytes = TlsAllocate(nSize);
    if(!pBytes) {
      // Nothing in the tls list, allocate from the OS
      pBytes = AllocateAndInitialize(nSize);
    }
    DCHECK(pBytes);
    return pBytes;
  }

  void* CAlloc(size_t count, size_t size) {
    return calloc(count, size);
  }

  __checkReturn void Free(void* pBytes) {
    auto& tls_map = GetTlsMap();
    // Extract the hidden size info
    Header* pHeader = ExtractHeader(pBytes);
    pHeader->next = nullptr;
    DCHECK(pHeader->size);
    auto& block_list = tls_map.find(pHeader->size);
    if(block_list == tls_map.end()) {
      tls_map.emplace(pHeader->size, BlockList(pHeader, pHeader));
    } else {
      block_list->second.Put(pHeader);
    }
  }

  __checkReturn void* AllocateAligned(size_t nSize, uint32_t nAlignment) {
    RAW_CHECK(nAlignment == kCacheLineSize, "unsupported alignment.");
    return Allocate(nSize);
  }

  __checkReturn void FreeAligned(void* pBytes) {
    return Free(pBytes);
  }

  void* AllocateAlignedOffset(size_t size, size_t alignment, size_t offset) {
    return nullptr;
  }

  void* AllocateHuge(size_t size) {
    return nullptr;
  }

  __checkReturn Status Validate(__in void* pBytes) {
    return default_->Validate(pBytes);
  }

  __checkReturn uint64_t GetAllocatedSize(void* pBytes) {
    return default_->GetAllocatedSize(pBytes);
  }

  __int64 GetTotalAllocationCount() {
    return 0;
  }
};

}
