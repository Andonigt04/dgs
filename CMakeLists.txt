cmake_minimum_required(VERSION 3.14)
project(DGS)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

find_package(OpenSSL REQUIRED)
find_package(Threads REQUIRED)
add_library(httplib INTERFACE)
target_include_directories(httplib INTERFACE ${CMAKE_SOURCE_DIR}/third_party)
target_compile_definitions(httplib INTERFACE CPPHTTPLIB_OPENSSL_SUPPORT)
target_link_libraries(httplib INTERFACE OpenSSL::SSL OpenSSL::Crypto)
add_library(httplib::httplib ALIAS httplib)

include_directories(.)

# Mongo is used by ONE node (`persistance_node`) and nothing else. Requiring it as REQUIRED here made
# it impossible to CONFIGURE the whole project without the driver installed: `head_server_node` — which
# only links httplib — could not be built, and therefore no network test could run on a clean machine.
# Its CI went as far as compiling mongo-cxx-driver from source just to launch a packet-format test.
# It is optional now: without it, everything except that node is built.
find_package(mongocxx QUIET)
find_package(bsoncxx QUIET)
set(DGS_HAVE_MONGO FALSE)
if (mongocxx_FOUND AND bsoncxx_FOUND)
    set(DGS_HAVE_MONGO TRUE)
else()
    message(STATUS "DGS: no mongocxx -> `persistance_node` is NOT built (everything else is)")
endif()

# ThreadSanitizer. The `ThreadPool::stop` race (writing the `cv.wait` predicate's flag without the
# mutex) does NOT reproduce by hand: 20,000 construct/destroy cycles with 8 workers passed without
# hanging on x86-64 -O2, because acquiring the mutex forces the flag to be re-read. TSan sees it on the
# first try. A concurrency bug that only shows up "sometimes" is not caught by staring at it; it is
# caught with this option.
option(DGS_TSAN "Build with ThreadSanitizer" OFF)
if (DGS_TSAN)
    add_compile_options(-fsanitize=thread -g -O1)
    add_link_options(-fsanitize=thread)
    message(STATUS "DGS: ThreadSanitizer ENABLED")
endif()

option(HARUKA_USE_IPV6 "Activate network support for IPv6" ON)
if (HARUKA_USE_IPV6)
    add_compile_definitions(HARUKA_IPV6)
endif()

set(COMMON_SRCS src/packet.cpp src/network.cpp)

add_executable(head_server_node nodes/head_server_node.cpp ${COMMON_SRCS})
target_link_libraries(head_server_node httplib::httplib)

add_executable(zone_node nodes/zone_node.cpp ${COMMON_SRCS})
target_link_libraries(zone_node ${CMAKE_DL_LIBS})   # dlopen of the per-project rules module (module->step)

if (DGS_HAVE_MONGO)
    add_executable(persistance_node nodes/persistance_node.cpp ${COMMON_SRCS})
    target_link_libraries(persistance_node
        mongo::mongocxx_shared
        mongo::bsoncxx_shared
    )
endif()

add_executable(cache_node nodes/cache_node.cpp ${COMMON_SRCS})
add_executable(social_node nodes/social_node.cpp ${COMMON_SRCS})   # §3.7 social/account plane (P7)
add_executable(validador_node nodes/validador_node.cpp ${COMMON_SRCS})
target_link_libraries(validador_node ${CMAKE_DL_LIBS})   # dlopen of the per-project rules module

add_executable(dgs tools/dgs_cli.cpp)   # §3.8 (P8): installer/deployer/CLI (run/install/up/status/logs)

# Something for the viewer to look at: a crowd of entities walking through a zone as real clients do.
# A cluster with nobody in it is a correct but empty picture, and an empty picture cannot tell a
# working viewer from a broken one.
add_executable(demo_player tools/demo_player.cpp ${COMMON_SRCS})

# The one number nobody had: how many players ONE zone actually holds, and what gives way first.
# Everything else in this project is measured; this was the gap.
add_executable(load_zone tools/load_zone.cpp ${COMMON_SRCS})
target_link_libraries(load_zone Threads::Threads)

add_library(dgs_client STATIC src/client.cpp ${COMMON_SRCS})
target_link_libraries(dgs_client httplib::httplib)
set_target_properties(dgs_client PROPERTIES POSITION_INDEPENDENT_CODE ON)

install(TARGETS dgs_client ARCHIVE DESTINATION external/dgs/sdk)
install(DIRECTORY include/ DESTINATION external/dgs/include)

# The viewer needs raylib; its DATA layer (`views/viewer_state.h`) does not, and that is on purpose:
# the feed is tested headless by `viewer_e2e` whether or not anyone can build a window here.
find_package(raylib QUIET)
if(raylib_FOUND OR RAYLIB_FOUND)
    add_executable(dgs_viewer views/dgs_viewer.cpp ${COMMON_SRCS})
    target_link_libraries(dgs_viewer raylib Threads::Threads)
    message(STATUS "DGS: raylib found -> `dgs_viewer` is built")
else()
    message(STATUS "DGS: no raylib -> `dgs_viewer` is NOT built (its feed is still tested)")
endif()

# TESTS
add_executable(ping_pong_test tests/ping_pong.cpp ${COMMON_SRCS})
add_executable(thread_pool_test tests/thread_pool_test.cpp ${COMMON_SRCS})
add_executable(logger_test tests/logger_test.cpp ${COMMON_SRCS})
add_executable(wire_test tests/wire_test.cpp ${COMMON_SRCS})
add_executable(robust_test tests/robust_test.cpp ${COMMON_SRCS})
target_link_libraries(robust_test ${CMAKE_DL_LIBS})   # dlopen of the rules module (determinism)
add_executable(spawn_parity_test tests/spawn_parity_test.cpp)   # §3.8 (P8): backend parity
target_link_libraries(spawn_parity_test httplib::httplib)

# The orchestrator's DECISION logic: lifecycle queue and priorities, handoff on a failed metric, lease
# eviction, the drain fail-safe and the drain ack. It is the largest single piece of the system and the
# one that decides for the whole cluster; `spawn_parity_test` only covered two pure config helpers.
add_executable(orchestrator_test tests/orchestrator_test.cpp ${COMMON_SRCS})
target_link_libraries(orchestrator_test httplib::httplib OpenSSL::SSL OpenSSL::Crypto)

# `DGS::Client` — the other half of the protocol. Nothing named it: every other test drives the
# servers, and a client that sent its packets to the wrong place would look identical to silence.
add_executable(client_e2e_test tests/client_e2e.cpp src/client.cpp ${COMMON_SRCS})
target_link_libraries(client_e2e_test httplib::httplib OpenSSL::SSL OpenSSL::Crypto Threads::Threads)

# The viewer's feed, driven headless against a real `zone_node`: the read-only observer subscription,
# what it shows, and — the one that matters — that watching does not put anything into the world.
add_executable(viewer_e2e_test tests/viewer_e2e.cpp ${COMMON_SRCS})
target_link_libraries(viewer_e2e_test Threads::Threads)

find_package(SFML 2.5 COMPONENTS graphics window system)
if(SFML_FOUND)
    add_executable(visual_client_test tests/visual_client_test.cpp ${COMMON_SRCS})
    target_link_libraries(visual_client_test sfml-graphics sfml-window sfml-system)
endif()

# The validator end to end. `stub_rules.so` is a TOY rules module from this same repo: if the test
# needed the game's module (`libharuka_rules.so`), the network project would stop being testable on its
# own, which is the opposite of why it is kept separate.
add_library(stub_rules SHARED tests/stub_rules.cpp)
set_target_properties(stub_rules PROPERTIES PREFIX "" SUFFIX ".so")

# The SAME module, but WITH an action rule. Two binaries rather than an environment variable because
# what has to be distinguished is a NULL `validateAction` pointer (the host's fail-closed) from one that
# exists and answers — two different paths through the validator.
add_library(stub_rules_actions SHARED tests/stub_rules.cpp)
target_compile_definitions(stub_rules_actions PRIVATE STUB_WITH_ACTIONS)
set_target_properties(stub_rules_actions PROPERTIES PREFIX "" SUFFIX ".so")

add_executable(validator_e2e_test tests/validator_e2e.cpp ${COMMON_SRCS})
target_link_libraries(validator_e2e_test Threads::Threads)

# The ZONE end to end: the other half of the anti-cheat. `validator_e2e` proves the arbiter answers;
# this proves the zone ASKS and OBEYS.
add_executable(zone_e2e_test tests/zone_e2e.cpp ${COMMON_SRCS})
target_link_libraries(zone_e2e_test Threads::Threads)

# Reconnection: the path broken by the rule-of-three violation in `TCPSocket`. Fixing it without a test
# that exercises it is a promise, not a guarantee.
add_executable(reconnect_e2e_test tests/reconnect_e2e.cpp ${COMMON_SRCS})
target_link_libraries(reconnect_e2e_test Threads::Threads)

# Actions (kind=1): the validator's FAIL-CLOSED path, which is direct attack surface.
add_executable(action_e2e_test tests/action_e2e.cpp ${COMMON_SRCS})
target_link_libraries(action_e2e_test Threads::Threads)

# Ghosts and authority handoff between zones: what separates a distributed world from a single server.
add_executable(ghost_e2e_test tests/ghost_e2e.cpp ${COMMON_SRCS})
target_link_libraries(ghost_e2e_test Threads::Threads)

# `cache_node`: the queue between the zones and the validator. 94 lines nothing touched.
add_executable(cache_e2e_test tests/cache_e2e.cpp ${COMMON_SRCS})
target_link_libraries(cache_e2e_test Threads::Threads)

# `social_node`: chat, guilds and accounts. 221 lines with no coverage.
add_executable(social_e2e_test tests/social_e2e.cpp ${COMMON_SRCS})
target_link_libraries(social_e2e_test Threads::Threads)

# The validator's UDP path + MEASUREMENT with a degraded network. The whole system had always been
# tested over loopback, the most benevolent network in existence.
add_executable(net_degraded_test tests/net_degraded.cpp ${COMMON_SRCS})
target_link_libraries(net_degraded_test Threads::Threads)

# The zone's three POLICIES: ban, lease expiry and the circuit breaker. They are not features, they are
# decisions — and a fail-open with no test is a decision nobody knows has been taken.
add_executable(zone_policy_e2e_test tests/zone_policy_e2e.cpp ${COMMON_SRCS})
target_link_libraries(zone_policy_e2e_test Threads::Threads)

# The head with TWO zones: its real job. With only one you cannot tell routing from echoing.
add_executable(head_routing_e2e_test tests/head_routing_e2e.cpp ${COMMON_SRCS})
target_link_libraries(head_routing_e2e_test Threads::Threads)

# `persistance_node`: that a database outage does NOT take the node down. Only built and registered
# when the Mongo driver is present — without it the node does not exist, so a test of it could not mean
# anything.
if (DGS_HAVE_MONGO)
    add_executable(persistence_e2e_test tests/persistence_e2e.cpp ${COMMON_SRCS})
    target_link_libraries(persistence_e2e_test Threads::Threads)
endif()

# ── TESTS ────────────────────────────────────────────────────────────────────────────────────────
# ⚠️ These were not registered with CTest: they were executables launched by hand, and in CI three of
# the four ended in `|| true`, so their verdict was thrown away. A test whose result is discarded covers
# nothing; it only creates the feeling of coverage. Now `ctest` runs them and their exit status RULES.
#
# `ping_pong_test` starts the node itself, so it needs to know where the binary is and that it
# haya construido antes.
enable_testing()

add_test(NAME wire        COMMAND wire_test)
add_test(NAME robust      COMMAND robust_test)
add_test(NAME thread_pool COMMAND thread_pool_test)
add_test(NAME logger      COMMAND logger_test)
add_test(NAME ping_pong   COMMAND ping_pong_test $<TARGET_FILE:head_server_node>)
# `spawn_parity_test` was ALREADY a real test (CHECK + a non-zero exit on failure). Simply nobody ran
# it: CTest did not register it and CI did not build it.
add_test(NAME spawn_parity COMMAND spawn_parity_test)
add_test(NAME orchestrator COMMAND orchestrator_test)
add_test(NAME client_e2e COMMAND client_e2e_test)
add_test(NAME viewer_e2e COMMAND viewer_e2e_test
         $<TARGET_FILE:zone_node> $<TARGET_FILE:stub_rules>)
add_test(NAME validator_e2e COMMAND validator_e2e_test
         $<TARGET_FILE:validador_node> $<TARGET_FILE:stub_rules>)
add_test(NAME zone_e2e COMMAND zone_e2e_test
         $<TARGET_FILE:zone_node> $<TARGET_FILE:stub_rules>)
add_test(NAME reconnect_e2e COMMAND reconnect_e2e_test
         $<TARGET_FILE:zone_node> $<TARGET_FILE:stub_rules>)
add_test(NAME action_e2e COMMAND action_e2e_test
         $<TARGET_FILE:validador_node> $<TARGET_FILE:stub_rules>
         $<TARGET_FILE:stub_rules_actions>)
add_test(NAME ghost_e2e COMMAND ghost_e2e_test
         $<TARGET_FILE:zone_node> $<TARGET_FILE:stub_rules>)
add_test(NAME cache_e2e COMMAND cache_e2e_test $<TARGET_FILE:cache_node>)
add_test(NAME social_e2e COMMAND social_e2e_test $<TARGET_FILE:social_node>)
add_test(NAME net_degraded COMMAND net_degraded_test
         $<TARGET_FILE:validador_node> $<TARGET_FILE:stub_rules>)
add_test(NAME zone_policy_e2e COMMAND zone_policy_e2e_test
         $<TARGET_FILE:zone_node> $<TARGET_FILE:stub_rules>)
add_test(NAME head_routing_e2e COMMAND head_routing_e2e_test $<TARGET_FILE:head_server_node>)
if (DGS_HAVE_MONGO)
    add_test(NAME persistence_e2e COMMAND persistence_e2e_test $<TARGET_FILE:persistance_node>)
    set_tests_properties(persistence_e2e PROPERTIES RUN_SERIAL TRUE TIMEOUT 120)
endif()

# The round trip opens a fixed port (42424): two at once would collide. `RUN_SERIAL` prevents it.
# Both spawn processes that bind fixed ports: in parallel they would collide with each other.
set_tests_properties(ping_pong validator_e2e zone_e2e reconnect_e2e action_e2e ghost_e2e cache_e2e social_e2e net_degraded zone_policy_e2e head_routing_e2e
                     PROPERTIES RUN_SERIAL TRUE TIMEOUT 180)
set_tests_properties(wire robust thread_pool logger spawn_parity PROPERTIES TIMEOUT 60)
