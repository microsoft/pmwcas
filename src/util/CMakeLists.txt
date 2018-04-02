set(UTIL_HEADERS
  atomics.h
  auto_ptr.h
  hash.h
  macros.h
  performance_test.h
  random_number_generator.h
)

set(UTIL_SOURCES
  nvram.cc
  status.cc
)

add_library(util STATIC ${UTIL_SOURCES} ${UTIL_HEADERS})
target_link_libraries(util ${GFLAGS_LIB} ${GLOG_LIB})
