set(UTIL_HEADERS
  atomics.h
  auto_ptr.h
  hash.h
  macros.h
  performance_test.h
  random_number_generator.h
)

set(UTIL_SOURCES
  ${CMAKE_CURRENT_SOURCE_DIR}/nvram.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/status.cc
)

set_property(GLOBAL APPEND PROPERTY PMWCAS_SRC ${UTIL_SOURCES})
