cmake_minimum_required(VERSION 3.14)
project(DGS)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include_directories(.)

set(COMMON_SRCS src/packet.cpp src/network.cpp)

add_executable(anticheat_node nodes/anticheat_node.cpp ${COMMON_SRCS})
add_executable(cache_node nodes/cache_node.cpp ${COMMON_SRCS})
add_executable(head_server_node nodes/head_server_node.cpp ${COMMON_SRCS})
add_executable(persistance_node nodes/persistance_node.cpp ${COMMON_SRCS})
add_executable(zone_node nodes/zone_node.cpp ${COMMON_SRCS})
add_executable(client_node nodes/client_node.cpp ${COMMON_SRCS})