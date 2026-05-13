cmake_minimum_required(VERSION 3.14)
project(DGS)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_package(OpenSSL REQUIRED)
add_library(httplib INTERFACE)
target_include_directories(httplib INTERFACE ${CMAKE_SOURCE_DIR}/third_party)
target_compile_definitions(httplib INTERFACE CPPHTTPLIB_OPENSSL_SUPPORT)
target_link_libraries(httplib INTERFACE OpenSSL::SSL OpenSSL::Crypto)
add_library(httplib::httplib ALIAS httplib)

include_directories(.)

find_package(mongocxx REQUIRED)
find_package(bsoncxx REQUIRED)

set(COMMON_SRCS src/packet.cpp src/network.cpp)

add_executable(head_server_node nodes/head_server_node.cpp ${COMMON_SRCS})
target_link_libraries(head_server_node httplib::httplib)

add_executable(zone_node nodes/zone_node.cpp ${COMMON_SRCS})

add_executable(persistance_node nodes/persistance_node.cpp ${COMMON_SRCS})
target_link_libraries(persistance_node
    mongo::mongocxx_shared
    mongo::bsoncxx_shared
)

add_executable(cache_node nodes/cache_node.cpp ${COMMON_SRCS})
add_executable(anticheat_node nodes/anticheat_node.cpp ${COMMON_SRCS})

add_library(dgs_client STATIC src/client.cpp ${COMMON_SRCS})
target_link_libraries(dgs_client httplib::httplib)

install(TARGETS dgs_client ARCHIVE DESTINATION external/dgs/sdk)
install(DIRECTORY include/ DESTINATION external/dgs/include)

find_package(raylib)
if(RAYLIB_FOUND)
    add_executable(dgs_viewer views/dgs_viewer.cpp ${COMMON_SRCS})
    target_link_libraries(dgs_viewer raylib)
endif()

# TESTS
add_executable(ping_pong_test tests/ping_pong.cpp ${COMMON_SRCS})
add_executable(thread_pool_test tests/thread_pool_test.cpp ${COMMON_SRCS})
add_executable(logger_test tests/logger_test.cpp ${COMMON_SRCS})

find_package(SFML 2.5 COMPONENTS graphics window system)
if(SFML_FOUND)
    add_executable(visual_client_test tests/visual_client_test.cpp ${COMMON_SRCS})
    target_link_libraries(visual_client_test sfml-graphics sfml-window sfml-system)
endif()
