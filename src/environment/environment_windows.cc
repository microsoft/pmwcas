// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#define NOMINMAX

#define GLOG_NO_ABBREVIATED_SEVERITIES

#include <Windows.h>
#undef ERROR // Avoid collision of ERROR definition in Windows.h with glog
#include <Shlwapi.h>
#include <tchar.h>
#include <chrono>
#include <vector>
#include <string>
#include <iomanip>
#include <ios>
#include <glog/logging.h>
#include "common/allocator_internal.h"
#include "environment/environment_windows.h"
#include "util/auto_ptr.h"
#include "util/macros.h"
#include <glog/logging.h>

namespace pmwcas {

std::string FormatWin32AndHRESULT(DWORD win32_result) {
  std::stringstream ss;
  ss << "Win32(" << win32_result << ") HRESULT("
     << std::showbase << std::uppercase << std::setfill('0') << std::hex
     << HRESULT_FROM_WIN32(win32_result) << ")";
  return ss.str();
}

WindowsEnvironment::WindowsEnvironment()
  : perf_counter_frequency_{} {
  LARGE_INTEGER qpf;
  BOOL ret = QueryPerformanceFrequency(&qpf);
  DCHECK(ret);
  perf_counter_frequency_ = qpf.QuadPart;
  ::GetSystemInfo(&sys_info_);
}

uint64_t WindowsEnvironment::NowMicros() {
  // all std::chrono clocks on windows proved to return
  // values that may repeat that is not good enough for some uses.
  const int64_t c_UnixEpochStartTicks = 116444736000000000i64;
  const int64_t c_FtToMicroSec = 10;

  // This interface needs to return system time and not
  // just any microseconds because it is often used as an argument
  // to TimedWait() on condition variable
  FILETIME ftSystemTime;
  GetSystemTimePreciseAsFileTime(&ftSystemTime);

  LARGE_INTEGER li;
  li.LowPart = ftSystemTime.dwLowDateTime;
  li.HighPart = ftSystemTime.dwHighDateTime;
  // Subtract unix epoch start
  li.QuadPart -= c_UnixEpochStartTicks;
  // Convert to microsecs
  li.QuadPart /= c_FtToMicroSec;
  return li.QuadPart;
}

uint64_t WindowsEnvironment::NowNanos() {
  // all std::chrono clocks on windows have the same resolution that is only
  // good enough for microseconds but not nanoseconds
  // On Windows 8 and Windows 2012 Server
  // GetSystemTimePreciseAsFileTime(&current_time) can be used
  LARGE_INTEGER li;
  QueryPerformanceCounter(&li);
  // Convert to nanoseconds first to avoid loss of precision
  // and divide by frequency
  li.QuadPart *= std::nano::den;
  li.QuadPart /= perf_counter_frequency_;
  return li.QuadPart;
}

typedef BOOL(WINAPI* LPFN_GLPI)(
  PSYSTEM_LOGICAL_PROCESSOR_INFORMATION,
  PDWORD);

/// Counts the number of set bits in a mask. Helper function for GetCoreCount.
static size_t CountSetBits(ULONG_PTR bitMask) {
  DWORD LSHIFT = sizeof(ULONG_PTR) * 8 - 1;
  size_t bitSetCount = 0;
  ULONG_PTR bitTest = (ULONG_PTR)1 << LSHIFT;
  DWORD i;

  for(i = 0; i <= LSHIFT; ++i) {
    bitSetCount += ((bitMask & bitTest) ? 1 : 0);
    bitTest /= 2;
  }

  return bitSetCount;
}

/// Returns the core count on the test machine
uint32_t WindowsEnvironment::GetCoreCount() {
  LPFN_GLPI glpi;
  BOOL done = FALSE;
  PSYSTEM_LOGICAL_PROCESSOR_INFORMATION buffer = NULL;
  PSYSTEM_LOGICAL_PROCESSOR_INFORMATION ptr = NULL;
  DWORD returnLength = 0;
  DWORD byteOffset = 0;

  glpi = (LPFN_GLPI)::GetProcAddress(GetModuleHandle(TEXT("kernel32")),
      "GetLogicalProcessorInformation");
  if(!glpi) {
    return 0;
  }

  while(!done) {
    DWORD rc = glpi(buffer, &returnLength);
    if(FALSE == rc) {
      if(GetLastError() == ERROR_INSUFFICIENT_BUFFER) {
        if(buffer) {
          free(buffer);
        }

        buffer = (PSYSTEM_LOGICAL_PROCESSOR_INFORMATION)malloc(
                   returnLength);
        if(NULL == buffer) {
          return 0;
        }
      } else {
        return 0;
      }
    } else {
      done = TRUE;
    }
  }

  uint32_t count = 0;
  ptr = buffer;
  while(byteOffset + sizeof(SYSTEM_LOGICAL_PROCESSOR_INFORMATION)
        <= returnLength) {
    switch(ptr->Relationship) {
    case RelationProcessorCore:
      // A hyperthreaded core supplies more than one logical processor.
      count += CountSetBits(ptr->ProcessorMask);
      break;

    default:
      break;
    }
    byteOffset += sizeof(SYSTEM_LOGICAL_PROCESSOR_INFORMATION);
    ptr++;
  }

  free(buffer);
  return count;
}

void WindowsEnvironment::Sleep(uint32_t ms_to_sleep) {
  ::Sleep(ms_to_sleep);
}

Status WindowsEnvironment::NewRandomReadWriteAsyncFile(
    const std::string& filename, const FileOptions& options,
    ThreadPool* threadpool, RandomReadWriteAsyncFile** file, bool* exists) {
  if(exists) {
    *exists = PathFileExists(filename.c_str());
  }

  *file = nullptr;
  unique_ptr_t<RandomReadWriteAsyncFile> alloc_guard;
  RETURN_NOT_OK(WindowsRandomReadRandomWriteFile::Create(alloc_guard));

  RETURN_NOT_OK(alloc_guard->Open(filename, options, threadpool));

  *file = alloc_guard.release();
  return Status::OK();
}

Status WindowsEnvironment::NewSharedMemorySegment(const std::string& segname,
    uint64_t size, bool open_existing, SharedMemorySegment** seg) {
  *seg = nullptr;
  unique_ptr_t<SharedMemorySegment> alloc_guard;
  RETURN_NOT_OK(WindowsSharedMemorySegment::Create(alloc_guard));

  RETURN_NOT_OK(alloc_guard->Initialize(segname, size, open_existing));

  *seg = alloc_guard.release();
  return Status::OK();
}

Status WindowsEnvironment::NewThreadPool(uint32_t max_threads,
    ThreadPool** pool) {
  *pool = nullptr;
  unique_ptr_t<ThreadPool> pool_guard;
  RETURN_NOT_OK(WindowsPtpThreadPool::Create(max_threads, pool_guard));

  *pool = pool_guard.release();
  return Status::OK();

}

Status WindowsEnvironment::GetWorkingDirectory(std::string& directory) {
  DWORD result = ::GetCurrentDirectory(0, _T(""));
  if(result == 0) {
    return Status::IOError("error in GetCurrentDirectory",
        std::to_string(HRESULT_FROM_WIN32(::GetLastError())));
  }

  // Allocate temporary buffer. The retured length includes the
  // terminating _T('\0').  td::vector is guaranteed to be sequential,
  // thus can serve as a buffer that can be written to.
  std::vector<TCHAR> currentDirectory(result);

  // If the buffer is large enough, the return value does _not_ include
  // the terminating _T('\0')
  result = ::GetCurrentDirectory(static_cast<DWORD>(currentDirectory.size()),
                                 &currentDirectory[0]);
  if((result == 0) || (result > currentDirectory.size())) {
    return Status::IOError("error in GetCurrentDirectory",
        std::to_string(HRESULT_FROM_WIN32(::GetLastError())));
  }

  std::wstring wdirectory(currentDirectory.begin(),
      currentDirectory.begin() + static_cast<std::size_t>(result));
  directory = std::string(wdirectory.begin(), wdirectory.end());
  return Status::OK();
}

Status WindowsEnvironment::GetExecutableDirectory(std::string& directory) {
  std::vector<TCHAR> currentDirectory(MAX_PATH);
  DWORD result  = ::GetModuleFileName(NULL, &currentDirectory[0], MAX_PATH);
  if((0 == result) || (result > currentDirectory.size())) {
    return Status::IOError("error in GetExecutableDirectory",
        FormatWin32AndHRESULT(result));
  }
  PathRemoveFileSpec(&currentDirectory[0]);
  size_t str_length = _tcslen(&currentDirectory[0]) / sizeof(TCHAR);
  std::wstring wdirectory(currentDirectory.begin(),
      currentDirectory.begin() + static_cast<std::size_t>(str_length));
  directory = std::string(wdirectory.begin(), wdirectory.end());
  return Status::OK();
}

Status WindowsEnvironment::SetThreadAffinity(uint64_t core,
    AffinityPattern affinity_pattern) {
  HANDLE thread_handle = ::GetCurrentThread();
  return SetThreadAffinity(thread_handle, core, affinity_pattern);
}

Status WindowsEnvironment::SetThreadAffinity(HANDLE thread, uint64_t core,
    AffinityPattern affinity_pattern) {
  if(affinity_pattern == AffinityPattern::PhysicalCoresFirst) {
    // Recalculate "core" so that all physical cores are scheduled, 1 thread per
    // each, before we schedule any hyperthread cores.
    if(core >= sys_info_.dwNumberOfProcessors) {
      return Status::Aborted("Too few logical cores.",
                             std::to_string(sys_info_.dwNumberOfProcessors));
    }

    // Assume 2 logical cores per physical core.
    if(sys_info_.dwNumberOfProcessors % 2 != 0) {
      return Status::Aborted("Not an even number of logical cores.",
                             std::to_string(sys_info_.dwNumberOfProcessors));
    }
    uint32_t physical_core_count = sys_info_.dwNumberOfProcessors / 2;

    if(core < physical_core_count) {
      core = core * 2;
    } else {
      core = (core - physical_core_count) * 2 + 1;
    }
  } else if(affinity_pattern == AffinityPattern::BalanceNumaNodes) {
    // For now, assume that we're running on a 4-node NUMA system, where the
    // cores are numbered with the first n cores on node 0, the next n cores on
    // node 1, ... and the last n cores on node 3.
    const uint32_t numa_node_count = 4;
    CHECK_EQ(sys_info_.dwNumberOfProcessors % numa_node_count, 0) <<
        "Unexpected system configuration!";
    uint32_t logical_core_count =
      sys_info_.dwNumberOfProcessors / numa_node_count;
    // Assume 2 logical cores per physical core.
    CHECK_EQ(logical_core_count % 2, 0) << "Unexpected system configuration!";
    uint32_t physical_core_count = logical_core_count / 2;

    uint32_t numa_node = core % numa_node_count;
    uint32_t numa_core = core / numa_node_count;

    if(numa_core < physical_core_count) {
      numa_core = numa_core * 2;
    } else {
      numa_core = (numa_core - physical_core_count) * 2 + 1;
    }
    core = (numa_node * logical_core_count) + numa_core;
  }

  DWORD_PTR result = ::SetThreadAffinityMask(thread, (uint64_t)0x1 << core);

  if(result == 0) {
    DWORD error = ::GetLastError();
    return Status::Aborted("Failed to set thread affinity.",
        FormatWin32AndHRESULT(result));
  } else {
    return Status::OK();
  }
}

Status WindowsEnvironment::AllocateTlsIndex(uint32_t& index) {
  index = uint32_t{};
  uint32_t new_index = ::TlsAlloc();
  if(TLS_OUT_OF_INDEXES == new_index) {
    return Status::Aborted("No TLS indexes available",
        FormatWin32AndHRESULT(::GetLastError()));
  }
  index = new_index;
  return Status::OK();
}

Status WindowsEnvironment::FreeTlsIndex(uint32_t index) {
  BOOL ok = ::TlsFree(index);
  if(0 == ok) {
    return Status::Aborted("TlsFree error.",
        FormatWin32AndHRESULT(::GetLastError()));
  }
  return Status::OK();
}

Status WindowsEnvironment::GetTlsValue(uint32_t index, void** value) {
  *value = ::TlsGetValue(index);
  if(*value == 0) {
    DWORD error = ::GetLastError();
    if(ERROR_SUCCESS != error) {
      return Status::Aborted("TlsGetValue error", FormatWin32AndHRESULT(error));
    }
  }
  return Status::OK();
}

Status WindowsEnvironment::SetTlsValue(uint32_t index, void* value) {
  BOOL ok = ::TlsSetValue(index, value);
  if(0 == ok) {
    return Status::Aborted("TlsSetValue error.",
        FormatWin32AndHRESULT(::GetLastError()));
  }
  return Status::OK();
}

WindowsRandomReadRandomWriteFile::WindowsRandomReadRandomWriteFile()
  : RandomReadWriteAsyncFile()
  , direct_io_{ false }
  , file_handle_ { INVALID_HANDLE_VALUE }
  , map_handle_ { INVALID_HANDLE_VALUE }
  , map_address_ { nullptr }
  , device_alignment_{}
  , filename_{}
  , threadpool_{} {
}

WindowsRandomReadRandomWriteFile::~WindowsRandomReadRandomWriteFile() {
  Status s = Close();
  LOG_IF(FATAL, !s.ok()) << "File failed closing with error: " << s.ToString();
}

RandomReadWriteAsyncFile::~RandomReadWriteAsyncFile() {
}

Status WindowsRandomReadRandomWriteFile::Create(
    unique_ptr_t<RandomReadWriteAsyncFile>& file) {
  file.reset();

  file = alloc_unique<RandomReadWriteAsyncFile>(
      sizeof(WindowsRandomReadRandomWriteFile));
  if(!file.get()) return Status::OutOfMemory();
  new(file.get())WindowsRandomReadRandomWriteFile();
  return Status::OK();
}

uint64_t WindowsRandomReadRandomWriteFile::GetFileIdentifier() const {
  return reinterpret_cast<uint64_t>(file_handle_);
}

bool WindowsRandomReadRandomWriteFile::DirectIO() {
  return direct_io_;
}

size_t WindowsRandomReadRandomWriteFile::GetAlignment() {
  return device_alignment_;
}

Status WindowsRandomReadRandomWriteFile::Open(const std::string& filename,
    const FileOptions& options, ThreadPool* threadpool) {
  MARK_UNREFERENCED(options);
  DWORD desired_access = GENERIC_READ | GENERIC_WRITE;
  DWORD const flags = FILE_FLAG_RANDOM_ACCESS | FILE_FLAG_OVERLAPPED |
                      FILE_FLAG_NO_BUFFERING;
  DWORD create_disposition =
    options.truncate_if_exists ? CREATE_ALWAYS : OPEN_ALWAYS;
  DWORD shared_mode = 0;
  LPSECURITY_ATTRIBUTES const security = NULL;

  file_handle_ = ::CreateFileA(filename.c_str(), desired_access, shared_mode,
      security, create_disposition, flags, NULL);
  if(INVALID_HANDLE_VALUE == file_handle_) {
    auto error = ::GetLastError();
    return Status::IOError("Failed to create random read random write "
                           "file: " + filename, FormatWin32AndHRESULT(error));
  }

  unique_ptr_t<HANDLE> handle_guard = unique_ptr_t<HANDLE> (&file_handle_,
  [=](HANDLE* h) {
    ::CloseHandle(*h);
    *h = INVALID_HANDLE_VALUE;
  });

  RETURN_NOT_OK(GetDeviceAlignment(filename, device_alignment_));
  RETURN_NOT_OK(threadpool->CreateAsyncIOHandler(ThreadPoolPriority::Low, *this,
                async_io_handler_));

  filename_ = filename;
  threadpool_ = threadpool;
  handle_guard.release();

  LOG(INFO) << "Opened file: " << filename;

  return Status::OK();
}

Status WindowsRandomReadRandomWriteFile::Close() {
  if(INVALID_HANDLE_VALUE != file_handle_) {
    BOOL ret = ::CloseHandle(file_handle_);
    file_handle_ = INVALID_HANDLE_VALUE;
    if(!ret) {
      auto error = ::GetLastError();
      return Status::IOError(
          "Error closing file: " + FormatWin32AndHRESULT(error));
    }
  }
  return Status();
}

Status pmwcas::WindowsRandomReadRandomWriteFile::Delete() {
  BOOL ret = ::DeleteFileA(filename_.c_str());
  if(!ret) {
    auto error = ::GetLastError();
    LOG(ERROR) << "Failed to delete file: " << 
      filename_ << ": (" << FormatWin32AndHRESULT(error);
    return Status::IOError("Failed to delete file( " + filename_ + "): " +
        FormatWin32AndHRESULT(error));
  }
  return Status::OK();
}

void WindowsRandomReadRandomWriteFile::BlockingCallback(IAsyncContext* context,
    Status status, size_t transfer_size) {
  MARK_UNREFERENCED(transfer_size);
  BlockingIOContext* blocking_context = reinterpret_cast<BlockingIOContext*>(
                                          context);
  unique_ptr_t<BlockingIOContext> context_guard(blocking_context,
  [=](BlockingIOContext* c) {
    Allocator::Get()->Free(c);
  });


  if(!status.ok()) {
    LOG(ERROR) << "Blocking IO error: %s\n", status.ToString().c_str();
  }

  ::SetEvent(context_guard->continuation_handle);
}

void WindowsRandomReadRandomWriteFile::AsyncReadCallback(IAsyncContext* context,
    Status status, size_t transfer_size) {
  unique_ptr_t<AsyncIOContext> io_context(
      reinterpret_cast<AsyncIOContext*>(context),
  [](AsyncIOContext* c) {
    Allocator::Get()->Free(c);
  });

  if(transfer_size != io_context->bytes_to_transfer) {
    LOG(ERROR) <<
      "Async read error size transfer and request mismatch (transfer: " <<
      transfer_size << " request: " << io_context->bytes_to_transfer << ")";
  }
  io_context->callback(io_context->context, status, transfer_size);
}

Status WindowsRandomReadRandomWriteFile::Read(size_t offset, uint32_t length,
    uint8_t* buffer, const IAsyncContext& context,
    RandomReadWriteAsyncFile::AsyncCallback callback) {
  if((uintptr_t(buffer) & (device_alignment_ - 1)) > 0) {
    return Status::IOError("IO Buffer not aligned to device alignment :"
                           + std::to_string(device_alignment_));
  } else if((offset & (device_alignment_ - 1)) > 0) {
    return Status::IOError("IO offset not aligned to device alignment :"
                           + std::to_string(device_alignment_));
  } else if((length & (device_alignment_ - 1)) > 0) {
    return Status::IOError("Length not aligned to device alignment :"
                           + std::to_string(device_alignment_));
  }
  AsyncIOContext async_context{ const_cast<IAsyncContext*>(&context), callback,
      buffer, length, this };
  RETURN_NOT_OK(async_io_handler_->ScheduleRead(buffer, offset, length,
      AsyncReadCallback, reinterpret_cast<IAsyncContext*>(&async_context)));
  return Status::OK();
}

void WindowsRandomReadRandomWriteFile::AsyncWriteCallback(
    IAsyncContext* context, Status status, size_t transfer_size) {
  AsyncIOContext* raw_context = reinterpret_cast<AsyncIOContext*>(context);
  unique_ptr_t<AsyncIOContext> io_context(raw_context, [=](AsyncIOContext* c) {
    Allocator::Get()->Free(c);
  });

  if(transfer_size != io_context->bytes_to_transfer) {
    LOG(ERROR) <<
      "Async read error size transfer and request mismatch (transfer: " <<
      transfer_size << " request: " << io_context->bytes_to_transfer << ")";
  }

  io_context->callback(io_context->context, status, transfer_size);
}

Status WindowsRandomReadRandomWriteFile::Write(size_t offset, uint32_t length,
    uint8_t* buffer, const IAsyncContext& context,
    RandomReadWriteAsyncFile::AsyncCallback callback) {
  if((uintptr_t(buffer) & (device_alignment_ - 1)) > 0) {
    return Status::IOError("IO Buffer not aligned to device alignment :"
                           + std::to_string(device_alignment_));
  } else if((offset & (device_alignment_ - 1)) > 0) {
    return Status::IOError("IO offset not aligned to device alignment :"
                           + std::to_string(device_alignment_));
  } else if((length & (device_alignment_ - 1)) > 0) {
    return Status::IOError("Length not aligned to device alignment :"
                           + std::to_string(device_alignment_));
  }

  AsyncIOContext async_context{ const_cast<IAsyncContext*>(&context), callback,
      buffer, length, this };
  RETURN_NOT_OK(async_io_handler_->ScheduleWrite(buffer, offset, length,
      AsyncWriteCallback, reinterpret_cast<IAsyncContext*>(&async_context)));
  return Status::OK();
}

Status WindowsRandomReadRandomWriteFile::GetDeviceAlignment(
  const std::string& filename, size_t& alignment) {
  // Prepend device prefix to the "X:" portion of the file name.
  char fullname[MAX_PATH];
  GetFullPathName(filename.c_str(), MAX_PATH, fullname, NULL);
  std::string prefixed = std::string(fullname);
  prefixed = "\\\\.\\" + prefixed.substr(0, 2);

  HANDLE device = ::CreateFileA(prefixed.c_str(), 0, 0, nullptr, OPEN_EXISTING,
      FILE_ATTRIBUTE_NORMAL, nullptr);
  if(INVALID_HANDLE_VALUE == device) {
    auto error = ::GetLastError();
    return Status::IOError("Failed to create random read random write "
                           "file for alignment query: " + filename,
                           std::to_string(HRESULT_FROM_WIN32(error)));
  }

  unique_ptr_t<HANDLE> handle_guard = unique_ptr_t<HANDLE> (&device,
  [=](HANDLE* h) {
    ::CloseHandle(*h);
  });

  STORAGE_PROPERTY_QUERY spq;
  spq.PropertyId = StorageAccessAlignmentProperty;
  spq.QueryType = PropertyStandardQuery;

  STORAGE_ACCESS_ALIGNMENT_DESCRIPTOR alignmentDescriptor;
  BOOL ret = DeviceIoControl(device, IOCTL_STORAGE_QUERY_PROPERTY, &spq,
      sizeof(spq), (BYTE*)&alignmentDescriptor, sizeof(alignmentDescriptor),
      nullptr, nullptr);
  if(ret) {
    alignment = alignmentDescriptor.BytesPerLogicalSector;
  } else {
    // Many devices do not support StorageProcessAlignmentProperty.
    // Any failure here and we fall back to logical alignment
    DISK_GEOMETRY geometry;
    ret = DeviceIoControl(device, IOCTL_DISK_GET_DRIVE_GEOMETRY, nullptr, 0,
                          &geometry, sizeof(geometry), nullptr, nullptr);
    if(!ret) {
      auto error = ::GetLastError();
      return Status::IOError("Could not determine block alignment for "
        "device: " + filename, std::to_string(HRESULT_FROM_WIN32(error)));
    }
    alignment = geometry.BytesPerSector;
  }

  return Status::OK();
}

WindowsSharedMemorySegment::WindowsSharedMemorySegment()
  : SharedMemorySegment()
  , segment_name_ { "" }
  , size_ { 0 }
  , map_handle_ { INVALID_HANDLE_VALUE }
  , map_address_ { nullptr } {
}

WindowsSharedMemorySegment::~WindowsSharedMemorySegment() {}

SharedMemorySegment::~SharedMemorySegment() {}

Status WindowsSharedMemorySegment::Create(
    unique_ptr_t<SharedMemorySegment>& segment) {
  segment.reset();
  segment =
    alloc_unique<SharedMemorySegment>(sizeof(WindowsSharedMemorySegment));
  if(!segment.get()) return Status::OutOfMemory();
  new(segment.get())WindowsSharedMemorySegment();

  return Status::OK();
}

Status WindowsSharedMemorySegment::Initialize(const std::string& segname,
    uint64_t size, bool open_existing) {
  segment_name_ = segname;
  size_ = size;

  if(open_existing) {
    // Open an existing mapping to Attach() later
    map_handle_ = OpenFileMapping(FILE_MAP_ALL_ACCESS, false,
        segment_name_.c_str());
  } else {
    // Create a new mapping for others and me to Attach() later
    map_handle_ = CreateFileMapping(INVALID_HANDLE_VALUE, nullptr,
        PAGE_READWRITE, 0, size_, segment_name_.c_str());
  }
  if(INVALID_HANDLE_VALUE == map_handle_) {
    auto error = ::GetLastError();
    return Status::IOError("Failed to create file mapping",
                           std::to_string(HRESULT_FROM_WIN32(error)));
  }
  return Status::OK();
}

Status WindowsSharedMemorySegment::Attach(void* base_address) {
  map_address_ = MapViewOfFileEx(map_handle_, FILE_MAP_ALL_ACCESS, 0, 0, size_,
      base_address);
  if(nullptr == map_address_) {
    auto error = ::GetLastError();
    return Status::IOError("Failed to attach to shared memory segment " +
        segment_name_ + " of " + std::to_string(size_) +
        " bytes with base address " + std::to_string((uint64_t)base_address),
        std::to_string(HRESULT_FROM_WIN32(error)));

  }
  return Status::OK();
}

Status WindowsSharedMemorySegment::Detach() {
  UnmapViewOfFile(map_address_);
  map_address_ = nullptr;
  return Status::OK();
}

void* WindowsSharedMemorySegment::GetMapAddress() {
  return map_address_;
}

WindowsPtpThreadPool::WindowsPtpThreadPool()
  : pool_{}
  , callback_environments_{}
  , cleanup_group_{}
  , max_threads_{} {
}

WindowsPtpThreadPool::~WindowsPtpThreadPool() {
  // Wait until all callbacks have finished.
  ::CloseThreadpoolCleanupGroupMembers(cleanup_group_, FALSE, nullptr);

  for(uint8_t priority = uint8_t(ThreadPoolPriority::Low);
      priority < uint8_t(ThreadPoolPriority::Last); ++priority) {
    PTP_CALLBACK_ENVIRON env = &callback_environments_[priority];
    ::DestroyThreadpoolEnvironment(env);
  }

  ::CloseThreadpoolCleanupGroup(cleanup_group_);
  ::CloseThreadpool(pool_);

  cleanup_group_ = nullptr;
  pool_ = nullptr;
}

Status WindowsPtpThreadPool::Create(uint32_t max_threads,
    unique_ptr_t<ThreadPool>& threadpool) {
  threadpool.reset();

  threadpool = alloc_unique<ThreadPool>(sizeof(WindowsPtpThreadPool));
  if(!threadpool.get()) return Status::OutOfMemory();
  new(threadpool.get())WindowsPtpThreadPool();

  return static_cast<WindowsPtpThreadPool*>(
      threadpool.get())->Initialize(max_threads);
}

Status WindowsPtpThreadPool::Initialize(uint32_t max_threads) {
  PTP_POOL pool = ::CreateThreadpool(nullptr);
  ::SetThreadpoolThreadMaximum(pool, max_threads);
  BOOL ret = ::SetThreadpoolThreadMinimum(pool, 1);
  if(!ret) return Status::Corruption("Unable to set threadpool minimum count");

  cleanup_group_ = ::CreateThreadpoolCleanupGroup();
  if(!cleanup_group_) {
    return Status::Corruption("Unable to create threadpool cleanup group "
        "error: " + std::to_string(HRESULT_FROM_WIN32(::GetLastError())));
  }

  for(uint8_t priority = size_t(ThreadPoolPriority::Low);
      priority < size_t(ThreadPoolPriority::Last); ++priority) {
    PTP_CALLBACK_ENVIRON env = &callback_environments_[priority];
    ::InitializeThreadpoolEnvironment(env);
    ::SetThreadpoolCallbackPool(env, pool);
    ::SetThreadpoolCallbackPriority(env, TranslatePriority(
        static_cast<ThreadPoolPriority>(priority)));
    ::SetThreadpoolCallbackCleanupGroup(env, cleanup_group_, nullptr);
  }

  max_threads = max_threads;
  pool_ = pool;

  return Status::OK();
}

void CALLBACK WindowsPtpThreadPool::TaskStartSpringboard(
    PTP_CALLBACK_INSTANCE instance, PVOID parameter, PTP_WORK work) {
  MARK_UNREFERENCED(instance);
  unique_ptr_t<TaskInfo> info = make_unique_ptr_t<TaskInfo>(
      reinterpret_cast<TaskInfo*>(parameter));
  Status s = info->task(info->task_parameters);
  if(!s.ok()) {
    LOG(ERROR) << "Task callback did not return successfully: " << s.ToString();
  }
  CloseThreadpoolWork(work);
}

Status WindowsPtpThreadPool::Schedule(ThreadPoolPriority priority, Task task,
                                      void* task_parameters) {
  unique_ptr_t<TaskInfo> info = alloc_unique<TaskInfo>(sizeof(TaskInfo));
  if(!info.get()) return Status::OutOfMemory();
  new(info.get()) TaskInfo();

  info->task = task;
  info->task_parameters = task_parameters;

  PTP_CALLBACK_ENVIRON environment = &callback_environments_[size_t(priority)];
  PTP_WORK_CALLBACK ptp_callback = TaskStartSpringboard;
  PTP_WORK work = CreateThreadpoolWork(ptp_callback, info.get(), environment);
  if(!work) {
    std::stringstream ss;
    ss << "Failed to schedule work: " <<
        FormatWin32AndHRESULT(::GetLastError());
    LOG(ERROR) << ss.str();
    return Status::Aborted(ss.str());
  }
  SubmitThreadpoolWork(work);
  info.release();

  return Status::OK();
}

Status WindowsPtpThreadPool::ScheduleTimer(ThreadPoolPriority priority,
    Task task, void* task_argument, uint32_t ms_period, void** timer_handle) {
  MARK_UNREFERENCED(priority);
  MARK_UNREFERENCED(task);
  MARK_UNREFERENCED(task_argument);
  MARK_UNREFERENCED(ms_period);
  MARK_UNREFERENCED(timer_handle);
  return Status::NotSupported("Not implemented");
}

Status WindowsPtpThreadPool::CreateAsyncIOHandler(ThreadPoolPriority priority,
    const File& file, unique_ptr_t<AsyncIOHandler>& async_io) {
  async_io.reset();
  HANDLE file_handle = reinterpret_cast<HANDLE>(file.GetFileIdentifier());
  if(INVALID_HANDLE_VALUE == file_handle) {
    return Status::IOError("Invalid file handle");
  }

  async_io = alloc_unique<AsyncIOHandler>(sizeof(WindowsAsyncIOHandler));
  if(!async_io.get()) return Status::OutOfMemory();
  new(async_io.get()) WindowsAsyncIOHandler();

  return static_cast<WindowsAsyncIOHandler*>(async_io.get())->Initialize(this,
      file_handle, &callback_environments_[uint8_t(priority)], priority);
}

TP_CALLBACK_PRIORITY WindowsPtpThreadPool::TranslatePriority(
  ThreadPoolPriority priority) {
  switch(priority) {
  case ThreadPoolPriority::Low:
    return TP_CALLBACK_PRIORITY_LOW;
    break;
  case ThreadPoolPriority::Medium:
    return TP_CALLBACK_PRIORITY_NORMAL;
    break;
  case ThreadPoolPriority::High:
    return TP_CALLBACK_PRIORITY_HIGH;
    break;
  default:
    return TP_CALLBACK_PRIORITY_INVALID;
  }
}

WindowsAsyncIOHandler::WindowsAsyncIOHandler()
  : file_handle_(NULL)
  , io_object_(nullptr)
  , threadpool_{ nullptr }
  , io_priority_{ ThreadPoolPriority::Low }  {
}

WindowsAsyncIOHandler::~WindowsAsyncIOHandler() {
  if(!io_object_) return;
  ::WaitForThreadpoolIoCallbacks(io_object_, TRUE);
  ::CloseThreadpoolIo(io_object_);
  io_object_ = nullptr;
  file_handle_ = nullptr;
}

void CALLBACK WindowsAsyncIOHandler::IOCompletionCallback(
    PTP_CALLBACK_INSTANCE instance, PVOID context, PVOID overlapped,
    ULONG ioResult, ULONG_PTR bytesTransferred, PTP_IO io) {
  MARK_UNREFERENCED(instance);
  MARK_UNREFERENCED(context);
  MARK_UNREFERENCED(io);

  // context is always nullptr; state is threaded via the OVERLAPPED
  unique_ptr_t<WindowsAsyncIOHandler::IOCallbackContext> callback_context(
    reinterpret_cast<IOCallbackContext*>(overlapped),
  [](IOCallbackContext* c) {
    Allocator::Get()->Free(c);
  });

  HRESULT hr = HRESULT_FROM_WIN32(ioResult);
  Status return_status;
  if(FAILED(hr)) {
    return_status = Status::IOError("Error in async IO: " + std::to_string(hr));
  } else {
    return_status = Status::OK();
  }

  callback_context->callback(callback_context->caller_context, return_status,
                             size_t(bytesTransferred));
}

Status WindowsAsyncIOHandler::Initialize(ThreadPool* threadpool,
    HANDLE file_handle, PTP_CALLBACK_ENVIRON environment,
    ThreadPoolPriority priority) {
  if(io_object_) return Status::OK();

  io_object_= ::CreateThreadpoolIo(file_handle, IOCompletionCallback, nullptr,
      environment);
  if(!io_object_) {
    return Status::IOError("Unable to create ThreadpoolIo");
  }
  file_handle_ = file_handle;
  threadpool_ = threadpool;
  io_priority_ = priority;
  return Status::OK();
}

Status WindowsAsyncIOHandler::ScheduleRead(uint8_t* buffer, size_t offset,
    uint32_t length, AsyncIOHandler::AsyncCallback callback,
    IAsyncContext* context) {
  return ScheduleOperation(OperationType::Read, buffer, offset, length,
      callback, context);
}

Status WindowsAsyncIOHandler::ScheduleWrite(uint8_t* buffer, size_t offset,
    uint32_t length, AsyncIOHandler::AsyncCallback callback,
    IAsyncContext* context) {
  return ScheduleOperation(OperationType::Write, buffer, offset, length,
      callback, context);
}

Status WindowsAsyncIOHandler::FinishSyncIOAsyncTask(void* context) {
  unique_ptr_t<WindowsAsyncIOHandler::IOCallbackContext> io_context(
    reinterpret_cast<IOCallbackContext*>(context),
  [](IOCallbackContext* c) {
    Allocator::Get()->Free(c);
  });
  // No need to log errors or check for IO failure. This function assumes that
  // it is completing a successful synchronous IO.
  io_context->callback(io_context->caller_context, Status::OK(),
      io_context->bytes_transferred);
  return Status::OK();
}

Status WindowsAsyncIOHandler::ScheduleOperation(OperationType operationType,
    void* buffer, size_t offset, uint32_t length,
    AsyncIOHandler::AsyncCallback callback, IAsyncContext* context) {

  unique_ptr_t<IOCallbackContext> io_context(
    reinterpret_cast<IOCallbackContext*>(Allocator::Get()->Allocate(
        sizeof(IOCallbackContext))),
  [](IOCallbackContext* c) {
    Status s = c->caller_context->DeepDelete();
    LOG_IF(ERROR, !s.ok()) << "deep delete failed with status: " <<
        s.ToString();
    Allocator::Get()->Free(c);
  });

  if(!io_context.get()) return Status::OutOfMemory();
  new(io_context.get()) IOCallbackContext();

  IAsyncContext* caller_context_copy = nullptr;
  RETURN_NOT_OK(context->DeepCopy(&caller_context_copy));

  io_context->handler = this;
  io_context->callback = callback;
  io_context->caller_context = caller_context_copy;

  ::memset(&(io_context->overlapped), 0, sizeof(OVERLAPPED));
  io_context->overlapped.Offset = offset & 0xffffffffllu;
  io_context->overlapped.OffsetHigh = offset >> 32;

  ::StartThreadpoolIo(io_object_);

  BOOL success = FALSE;
  DWORD bytes_transferred = 0;
  if(OperationType::Read == operationType) {
    success = ::ReadFile(file_handle_, buffer, length, &bytes_transferred,
        &io_context->overlapped);
  } else {
    success = ::WriteFile(file_handle_, buffer, length, &bytes_transferred,
        &io_context->overlapped);
  }
  if(!success) {
    DWORD win32_result = ::GetLastError();
    // Any error other than ERROR_IO_PENDING means the IO failed. Otherwise it
     // will finish asynchronously on the threadpool
    if(ERROR_IO_PENDING != win32_result) {
      ::CancelThreadpoolIo(io_object_);
      std::stringstream ss;
      ss << "Failed to schedule async IO: " <<
          FormatWin32AndHRESULT(win32_result);
      LOG(ERROR) << ss.str();
      return Status::IOError(ss.str());
    }
  } else {
    // The IO finished syncrhonously. Even though the IO was threadpooled, NTFS
    // may finish the IO synchronously (especially on VMs). To honor the fully
    // async call contract, schedule the completion on a separate thread using
    // the threadpool.
    io_context->bytes_transferred = length;
    RETURN_NOT_OK(threadpool_->Schedule(io_priority_, FinishSyncIOAsyncTask,
        reinterpret_cast<void*>(io_context.get())));
  }
  io_context.release();
  return Status::OK();
}

} // namespace pmwcas
