cmake_minimum_required(VERSION 3.9)
project(holo-client VERSION 1.1.0 DESCRIPTION "holo-client")
execute_process(
	COMMAND git tag --sort=-taggerdate
	COMMAND head -n 1
	TIMEOUT 10
	OUTPUT_VARIABLE GIT_VERSION
	OUTPUT_STRIP_TRAILING_WHITESPACE
)
message(STATUS "building from git tag ${GIT_VERSION}")
add_definitions(-DBUILD_VERSION=\"${GIT_VERSION}\")
include(GNUInstallDirs)
add_compile_options(-Wall)
add_compile_options(-Werror)
# 当前使用的c编译器
message("-- CMAKE_C_COMPILER: ${CMAKE_C_COMPILER_ID}")
# 当前使用的c编译器的版本
message("-- CMAKE_C_COMPILER_VERSION: ${CMAKE_C_COMPILER_VERSION}")
if("${CMAKE_C_COMPILER_ID}" STREQUAL "GNU")
	if(${CMAKE_C_COMPILER_VERSION} LESS 8.3.0)
		# 低版本gcc强制使用C11 mode，否则for循环内定义变量会报错
		# 低版本gcc强制使用Open 6标准（并入POSIX 2004），否则一些变量类型如int32_t没有定义
		add_compile_options(-std=c11 -D_XOPEN_SOURCE=600)
	endif()
endif()
# 若出现dwarf版本不匹配问题，可以尝试打开这个编译选项
# add_compile_options(-gdwarf-2 -gstrict-dwarf)
set(MY_PUBLIC_HEADERS
	include/defs.h
    include/holo_client.h
    include/holo_config.h
	include/logger.h
    include/record.h
    include/request.h
    include/table_schema.h
    include/worker_pool.h
)
find_library (LIBPQ NAMES pq libpq REQUIRED)
find_library (LOG4C NAMES log4c REQUIRED)
find_library (JEMALLOC NAMES jemalloc REQUIRED)
find_path (LIBPQ_INCLUDE_DIRS NAMES libpq-fe.h REQUIRED)
find_path (LOG4C_INCLUDE_DIRS NAMES log4c.h REQUIRED)
# 生成动态库
add_library(holo-client SHARED
    src/action.c
	src/batch.c
	src/connection_holder.c
	src/direct_collector.c
	src/exception.c
	src/future.c
	src/get_collector.c
	src/holo_client.c
	src/holo_config.c
	src/keywords.c
	src/logger.c
	src/logger_log4c.c
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
set_target_properties(holo-client PROPERTIES PUBLIC_HEADER "${MY_PUBLIC_HEADERS}")
target_include_directories(holo-client PUBLIC
	${LIBPQ_INCLUDE_DIRS}
	${LOG4C_INCLUDE_DIRS}
    include
    PRIVATE src)
target_link_libraries(holo-client ${LIBPQ} ${LOG4C} -lpthread ${JEMALLOC})
# 生成静态库
set(STATIC_LIB_NAME holo-client-static)
add_library(${STATIC_LIB_NAME} STATIC
    src/action.c
	src/batch.c
	src/connection_holder.c
	src/direct_collector.c
	src/exception.c
	src/future.c
	src/get_collector.c
	src/holo_client.c
	src/holo_config.c
	src/keywords.c
	src/logger.c
	src/logger_log4c.c
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
set_target_properties(${STATIC_LIB_NAME} PROPERTIES OUTPUT_NAME "holo-client")
set_target_properties(${STATIC_LIB_NAME} PROPERTIES PUBLIC_HEADER "${MY_PUBLIC_HEADERS}")
target_include_directories(${STATIC_LIB_NAME} PUBLIC
	${LIBPQ_INCLUDE_DIRS}
	${LOG4C_INCLUDE_DIRS}
    include
    PRIVATE src)
target_link_libraries(${STATIC_LIB_NAME} ${LIBPQ} ${LOG4C} -lpthread ${JEMALLOC})
SET(CMAKE_INSTALL_PREFIX ./)
install(TARGETS holo-client ${STATIC_LIB_NAME}
    LIBRARY DESTINATION out/lib
	ARCHIVE DESTINATION out/lib
    PUBLIC_HEADER DESTINATION out/include)