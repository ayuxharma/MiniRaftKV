cmake_minimum_required(VERSION 3.28)

project(
    MiniRaftKV
    VERSION 0.1.0
    DESCRIPTION "A small Raft consensus implementation in C++"
    LANGUAGES CXX
)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_library(miniraft_core
    src/raft_core.cpp
)

target_include_directories(miniraft_core
    PUBLIC
        ${PROJECT_SOURCE_DIR}/include
)

target_compile_options(miniraft_core
    PRIVATE
        -Wall
        -Wextra
        -Wpedantic
)

add_executable(miniraft_node
    apps/node_main.cpp
)

target_link_libraries(miniraft_node
    PRIVATE
        miniraft_core
)

enable_testing()

add_test(
    NAME node_starts_as_follower
    COMMAND miniraft_node node-1
)

set_tests_properties(
    node_starts_as_follower
    PROPERTIES
        PASS_REGULAR_EXPRESSION "node-1 started as follower in term 0"
)