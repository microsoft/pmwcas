set(BENCHMARK_HEADERS
  ../util/random_number_generator.h
)

ADD_PMWCAS_BENCHMARK(mwcas_benchmark)
ADD_PMWCAS_BENCHMARK(mwcas_shm_server)
ADD_PMWCAS_BENCHMARK(doubly_linked_list_benchmark)
