set(COMMON_HEADERS
  allocator_internal.h
  environment_internal.h
  epoch.h
  garbage_list.h
)

set(COMMON_SOURCES
  ${CMAKE_CURRENT_SOURCE_DIR}/allocator_internal.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/pmwcas_internal.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/environment_internal.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/epoch.cc
)

set_property(GLOBAL APPEND PROPERTY PMWCAS_SRC ${COMMON_SOURCES})

set(COMMON_TEST_SOURCES
  allocator_internal_test.cc
  epoch_test.cc
  garbage_list_test.cc
  unordered_map_test.cc
)

ADD_PMWCAS_TEST(allocator_internal_test)
ADD_PMWCAS_TEST(epoch_test)
ADD_PMWCAS_TEST(garbage_list_test)
