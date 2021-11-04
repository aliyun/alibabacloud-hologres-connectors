cmake_minimum_required(VERSION 3.9)
project(holo-client VERSION 1.0.1 DESCRIPTION "holo-client")
include(GNUInstallDirs)
add_library(holo-client SHARED
    src/action.c
	src/batch.c
	src/connection_holder.c
	src/direct_collector.c
	src/future.c
	src/get_collector.c
	src/holo_client.c
	src/holo_config.c
	src/logger.c
	src/lp_map.c
	src/meta_cache.c
	src/metrics.c
	src/murmur3.c
	src/mutation_collector.c
	src/mutation_map.c
	src/record.c
	src/request.c
	src/sql_builder.c
	src/table_schema.c
	src/utils.c
	src/worker_pool.c
	src/worker.c
)
set(MY_PUBLIC_HEADERS
    include/holo_client.h
    include/holo_config.h
    include/record.h
    include/request.h
    include/table_schema.h
    include/worker_pool.h
)
set_target_properties(holo-client PROPERTIES PUBLIC_HEADER "${MY_PUBLIC_HEADERS}")
find_library (LIBPQ NAMES pq libpq REQUIRED)
find_library (LOG4C NAMES log4c REQUIRED)
find_library (JEMALLOC NAMES jemalloc REQUIRED)
find_path (LIBPQ_INCLUDE_DIRS NAMES libpq-fe.h REQUIRED)
find_path (LOG4C_INCLUDE_DIRS NAMES log4c.h REQUIRED)
target_include_directories(holo-client PUBLIC
	${LIBPQ_INCLUDE_DIRS}
	${LOG4C_INCLUDE_DIRS}
    include
    PRIVATE src)
target_link_libraries(holo-client ${LIBPQ} ${LOG4C} -lpthread ${JEMALLOC})
SET(CMAKE_INSTALL_PREFIX ./)
install(TARGETS holo-client
    LIBRARY DESTINATION out/lib
    PUBLIC_HEADER DESTINATION out/include)