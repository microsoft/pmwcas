// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include <string>
#ifdef WIN32
#include <codecvt>
#endif
#include <mutex>
#include <condition_variable>
#include <gtest/gtest.h>
#include <glog/logging.h>
#include "util/performance_test.h"
#include "util/auto_ptr.h"
#include "util/random_number_generator.h"
#include "include/status.h"
#include "include/allocator.h"
#include "include/environment.h"
#include "include/pmwcas.h"
#ifdef WIN32
#include "environment/environment_windows.h"
#else
#include "environment/environment_linux.h"
#endif

namespace pmwcas {

class SharedMemoryTest : public ::testing::Test {
 public:
  const std::string test_segment_name = "pmwcas_shm_test";

 protected:
  virtual void SetUp() {}
};
#ifdef WIN32

class FileTest : public ::testing::Test {
 public:
  const std::string test_filename = "pmwcas_test.dat";

 protected:
  unique_ptr_t<ThreadPool> threadpool_;
  unique_ptr_t<RandomReadWriteAsyncFile> file_;

  virtual void SetUp() {
    uint32_t core_count = Environment::Get()->GetCoreCount();
    ThreadPool* new_pool{};
    ASSERT_TRUE(Environment::Get()->NewThreadPool(
        static_cast<uint32_t>(core_count), &new_pool).ok());
    threadpool_ = make_unique_ptr_t(new_pool);

    std::string filename{};
    ASSERT_TRUE(Environment::Get()->GetExecutableDirectory(filename).ok());
    filename.append("\\" + test_filename);
    FileOptions options{};
    RandomReadWriteAsyncFile* new_file{};
    ASSERT_TRUE(Environment::Get()->NewRandomReadWriteAsyncFile(filename,
        options, threadpool_.get(), &new_file).ok());
    file_ = make_unique_ptr_t(new_file);
  }

  virtual void TearDown() {
    ASSERT_TRUE(file_->Close().ok());
    ASSERT_TRUE(file_->Delete().ok());
    file_.reset(nullptr);
    threadpool_.reset(nullptr);
  }
};

struct TestIOContext : public IAsyncContext {
  HANDLE continuation_handle;
  FileTest* file_test_class;

  TestIOContext(HANDLE caller_handle, FileTest* caller_test_class)
    : IAsyncContext()
    , continuation_handle{ caller_handle }
    , file_test_class{ caller_test_class } {
  }

  virtual Status DeepCopy(IAsyncContext** context_copy) {
    if(from_deep_copy_) {
      *context_copy = this;
      return Status::OK();
    }
    *context_copy = nullptr;

    unique_ptr_t<TestIOContext> io_context =
      alloc_unique<TestIOContext>(sizeof(TestIOContext));
    if(!io_context.get()) return Status::OutOfMemory();

    io_context->file_test_class = file_test_class;
    io_context->continuation_handle = continuation_handle;
    io_context->from_deep_copy_ = true;
    *context_copy = io_context.release();
    return Status::OK();
  }

  virtual Status DeepDelete() {
    if(!from_deep_copy_) {
      return Status::OK();
    }
    Allocator::Get()->Free(this);
    return Status::OK();
  }

  Status WaitOnAsync() {
    if(WAIT_FAILED == ::WaitForSingleObject(continuation_handle, INFINITE)) {
      return Status::Corruption("WaitForSingleObjectFailed");
    }
    return Status::OK();
  }
};

static void TestIOCallback(IAsyncContext* context, Status status,
                           size_t bytes_transferred) {
  unique_ptr_t<TestIOContext> io_context(
  reinterpret_cast<TestIOContext*>(context), [=](TestIOContext* c) {
    Allocator::Get()->Free(c);
  });

  ::SetEvent(io_context->continuation_handle);
}

TEST_F(FileTest, Read) {
  size_t device_alignment = file_->GetAlignment();
  uint32_t buffer_length = static_cast<uint32_t>(device_alignment);
  unique_ptr_t<uint8_t> aligned_buffer =
    alloc_unique_aligned<uint8_t>(buffer_length, device_alignment);
  memset(aligned_buffer.get(), 0, buffer_length);

  HANDLE continuation_handle = ::CreateEvent(NULL, FALSE, FALSE, NULL);
  unique_ptr_t<HANDLE> handle_guard(&continuation_handle, [=](HANDLE* h) {
    ::CloseHandle(*h);
  });

  TestIOContext async_context(continuation_handle, this);

  // Unaligned buffer error
  ASSERT_TRUE(file_->Read(0, buffer_length, aligned_buffer.get() + 1,
      async_context, TestIOCallback).IsIOError());

  // Invalid offset error
  ASSERT_TRUE(file_->Read(device_alignment / 2, buffer_length,
      aligned_buffer.get(), async_context, TestIOCallback).IsIOError());

  // Invalid offset error
  ASSERT_TRUE(file_->Read(0, buffer_length / 2, aligned_buffer.get(),
      async_context, TestIOCallback).IsIOError());

  Status s = file_->Write(0, buffer_length, aligned_buffer.get(), async_context,
      TestIOCallback);
  ASSERT_TRUE(s.ok()) << "error: " << s.ToString();
  ASSERT_TRUE(async_context.WaitOnAsync().ok());

  // Valid read
  s = file_->Read(0, buffer_length, aligned_buffer.get(), async_context,
      TestIOCallback);
  ASSERT_TRUE(s.ok()) << "error: " << s.ToString();
  ASSERT_TRUE(async_context.WaitOnAsync().ok());
}

TEST_F(FileTest, Write) {
  size_t device_alignment = file_->GetAlignment();
  uint32_t buffer_length = static_cast<uint32_t>(device_alignment);
  unique_ptr_t<uint8_t> aligned_buffer =
    alloc_unique_aligned<uint8_t>(buffer_length, device_alignment);
  memset(aligned_buffer.get(), 0, buffer_length);

  HANDLE continuation_handle = ::CreateEvent(NULL, FALSE, FALSE, NULL);
  unique_ptr_t<HANDLE> handle_guard(&continuation_handle, [=](HANDLE* h) {
    ::CloseHandle(*h);
  });
  TestIOContext async_context(continuation_handle, this);

  // Unaligned buffer error
  ASSERT_TRUE(file_->Write(0, buffer_length, aligned_buffer.get() + 1,
      async_context, TestIOCallback).IsIOError());

  // Invalid offset error
  ASSERT_TRUE(file_->Write(device_alignment / 2, buffer_length,
      aligned_buffer.get(), async_context, TestIOCallback).IsIOError());

  // Invalid offset error
  ASSERT_TRUE(file_->Write(0, buffer_length / 2, aligned_buffer.get(),
      async_context, TestIOCallback).IsIOError());

  // Valid write
  Status s = file_->Write(0, buffer_length, aligned_buffer.get(), async_context,
      TestIOCallback);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_TRUE(async_context.WaitOnAsync().ok());
}

TEST_F(FileTest, DISABLED_ReadWrite) {
  size_t device_alignment = file_->GetAlignment();
  ASSERT_TRUE(device_alignment > sizeof(uint64_t));
  uint32_t buffer_length = static_cast<uint32_t>(device_alignment);
  unique_ptr_t<uint8_t> write_buffer =
      alloc_unique_aligned<uint8_t>(buffer_length, device_alignment);
  ASSERT_TRUE(write_buffer.get()) << "null write buffer";
  memset(write_buffer.get(), 0, device_alignment);

  HANDLE continuation_handle = ::CreateEvent(NULL, FALSE, FALSE, NULL);
  unique_ptr_t<HANDLE> handle_guard(&continuation_handle, [=](HANDLE* h) {
    ::CloseHandle(*h);
  });
  uint64_t num_writes = 100;
  bool async = false;

  for(uint64_t write = 0; write < num_writes; ++write) {
    size_t offset = write * buffer_length;
    memcpy_s(write_buffer.get(), buffer_length, &write, sizeof(write));
    TestIOContext async_context(continuation_handle, this);
    ASSERT_TRUE(file_->Write(offset, buffer_length, write_buffer.get(),
        async_context, TestIOCallback).ok());
    ASSERT_TRUE(async_context.WaitOnAsync().ok());
  }
  unique_ptr_t<uint8_t> read_buffer =
    alloc_unique_aligned<uint8_t>(buffer_length, device_alignment);
  ASSERT_TRUE(nullptr != read_buffer.get()) << "null read buffer";
  memset(read_buffer.get(), 0, device_alignment);

  for(uint64_t read = 0; read < num_writes; ++read) {
    size_t offset = read * buffer_length;
    memset(read_buffer.get(), 0, buffer_length);
    TestIOContext async_context(continuation_handle, this);
    ASSERT_TRUE(file_->Read(offset, buffer_length, read_buffer.get(),
        async_context, TestIOCallback).ok());
    ASSERT_TRUE(async_context.WaitOnAsync().ok());
    uint64_t read_value = *reinterpret_cast<uint64_t*>(read_buffer.get());
    ASSERT_EQ(read_value, read);
  }
}

class ThreadpoolTest : public ::testing::Test {
 protected:
  unique_ptr_t<ThreadPool> threadpool_;

  virtual void SetUp() {
    uint32_t core_count = Environment::Get()->GetCoreCount();
    ThreadPool* new_pool{};
    ASSERT_TRUE(Environment::Get()->NewThreadPool(
        static_cast<uint32_t>(core_count), &new_pool).ok());
    threadpool_ = make_unique_ptr_t(new_pool);
  }

  virtual void TearDown() {
    threadpool_.reset();
  }
};

struct TestThreadpoolContext {
  TestThreadpoolContext(HANDLE caller_handle, uint64_t* caller_count)
    : continuation_handle{ caller_handle }
    , count{ caller_count } {
  }

  Status WaitOnAsync() {
    if(WAIT_FAILED == ::WaitForSingleObject(continuation_handle, INFINITE)) {
      return Status::Corruption("WaitForSingleObject failed");
    }
    return Status::OK();
  }

  HANDLE continuation_handle;
  uint64_t* count;
};

static Status TestThreadpoolTask(void* context) {
  // No need to free the context, it is assumed to be stack-allocated within the
  // test harness
  TestThreadpoolContext* tp_context =
    reinterpret_cast<TestThreadpoolContext*>(context);
  ++*tp_context->count;
  ::SetEvent(tp_context->continuation_handle);
  return Status::OK();
}

TEST_F(ThreadpoolTest, DISABLED_Schedule) {
  uint64_t schedule_count = 128;
  uint64_t schedules_completed = 0;
  RandomNumberGenerator rng{};
  HANDLE continuation_handle = ::CreateEvent(NULL, FALSE, FALSE, NULL);
  unique_ptr_t<HANDLE> handle_guard(&continuation_handle, [=](HANDLE* h) {
    ::CloseHandle(*h);
  });
  for(int i = 0; i < schedule_count; ++i) {
    TestThreadpoolContext context(continuation_handle, &schedules_completed);
    uint8_t priority =
      narrow<uint8_t>(rng.Generate((uint32_t)ThreadPoolPriority::High));
    ASSERT_TRUE(threadpool_->Schedule(ThreadPoolPriority(priority),
        TestThreadpoolTask, reinterpret_cast<void*>(&context)).ok());
    ASSERT_TRUE(context.WaitOnAsync().ok());
  }
  ASSERT_EQ(schedule_count, schedules_completed);
}
#endif

TEST_F(SharedMemoryTest, AttachDetach) {
  SharedMemorySegment* new_seg = nullptr;
  SharedMemorySegment* existing_seg = nullptr;

  ASSERT_TRUE(Environment::Get()->NewSharedMemorySegment(test_segment_name, 256,
      false, &new_seg).ok());
  ASSERT_NE(nullptr, new_seg);
  ASSERT_TRUE(new_seg->Attach(nullptr).ok());
  void* new_mem = new_seg->GetMapAddress();
  memcpy(new_mem, test_segment_name.c_str(), 10);

  ASSERT_TRUE(Environment::Get()->NewSharedMemorySegment(test_segment_name, 256,
      true, &existing_seg).ok());
  ASSERT_NE(nullptr, existing_seg);
  ASSERT_TRUE(existing_seg->Attach(nullptr).ok());
  void* existing_mem = existing_seg->GetMapAddress();
  for(int i = 0; i < 10; i++) {
    ASSERT_EQ(test_segment_name.c_str()[i], ((char*)existing_mem)[i]);
  }
  ASSERT_TRUE(new_seg->Detach().ok());
  ASSERT_TRUE(existing_seg->Detach().ok());
}

} // namespace pmwcas

int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
#ifdef WIN32
  pmwcas::InitLibrary(pmwcas::DefaultAllocator::Create,
                           pmwcas::DefaultAllocator::Destroy,
                           pmwcas::WindowsEnvironment::Create,
                           pmwcas::WindowsEnvironment::Destroy);
#else
  pmwcas::InitLibrary(pmwcas::TlsAllocator::Create,
                           pmwcas::TlsAllocator::Destroy,
                           pmwcas::LinuxEnvironment::Create,
                           pmwcas::LinuxEnvironment::Destroy);
#endif
  return RUN_ALL_TESTS();
}
