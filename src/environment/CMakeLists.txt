set(ENVIRONMENT_SOURCES
  environment.cc
)

add_library(environment STATIC ${ENVIRONMENT_SOURCES})
target_link_libraries(environment ${GFLAGS_LIB} ${GLOG_LIB})

ADD_PMWCAS_TEST(environment_tests)
