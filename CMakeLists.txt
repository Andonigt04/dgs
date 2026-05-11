cmake_minimum_required(VERSION 3.14)
project(DGS)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include_directories(.)

find_package(mongocxx REQUIRED)
find_package(bsoncxx REQUIRED)

set(COMMON_SRCS src/packet.cpp src/network.cpp)

add_executable(head_server_node nodes/head_server_node.cpp ${COMMON_SRCS})
add_executable(zone_node nodes/zone_node.cpp ${COMMON_SRCS})

add_executable(persistance_node nodes/persistance_node.cpp ${COMMON_SRCS})
#add_executable(cache_node nodes/cache_node.cpp ${COMMON_SRCS})
add_executable(anticheat_node nodes/anticheat_node.cpp ${COMMON_SRCS})

#add_executable(client_node nodes/client_node.cpp ${COMMON_SRCS})

# TESTS
add_executable(ping_pong_test tests/ping_pong.cpp ${COMMON_SRCS})
add_executable(thread_pool_test tests/thread_pool_test.cpp ${COMMON_SRCS})
add_executable(logger_test tests/logger_test.cpp ${COMMON_SRCS})

target_link_libraries(persistance_node
    mongo::mongocxx_shared
    mongo::bsoncxx_shared
)
