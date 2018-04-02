set(COMMON_HEADERS
  allocator_internal.h
  environment_internal.h
  epoch.h
  garbage_list.h
)

set(COMMON_SOURCES
  allocator_internal.cc
  pmwcas_internal.cc
  environment_internal.cc
  epoch.cc
)

add_library(common STATIC ${COMMON_SOURCES} ${COMMON_HEADERS})
target_link_libraries(common ${GFLAGS_LIB} ${GLOG_LIB})

set(COMMON_TEST_SOURCES
  allocator_internal_test.cc
  epoch_test.cc
  garbage_list_test.cc
  unordered_map_test.cc
)

ADD_PMWCAS_TEST(allocator_internal_test)
ADD_PMWCAS_TEST(epoch_test)
ADD_PMWCAS_TEST(garbage_list_test)
