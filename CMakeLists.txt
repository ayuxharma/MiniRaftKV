# This project requires CMake 3.28 or newer.
cmake_minimum_required(VERSION 3.28)

# Define the project's name, version, description, and language.
project(
    MiniRaftKV
    VERSION 0.2.0
    DESCRIPTION "A small Raft consensus implementation in C++"
    LANGUAGES CXX
)

# Require portable C++20 for every target.
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Apply strong compiler warnings to one target.
#
# A function avoids repeating the warning configuration for every
# library, executable, and test target.
function(enable_project_warnings target_name)
    if(MSVC)
        # Warning flags for Microsoft Visual C++.
        target_compile_options(
            ${target_name}
            PRIVATE
                /W4
                /permissive-
        )
    else()
        # Warning flags for Clang and GCC.
        target_compile_options(
            ${target_name}
            PRIVATE
                -Wall
                -Wextra
                -Wpedantic
        )
    endif()
endfunction()

# Build the consensus algorithm as a reusable library.
add_library(miniraft_core
    src/raft_core.cpp
    src/in_memory_cluster.cpp
)

# Allow users of the library to include files from include/miniraft.
target_include_directories(miniraft_core
    PUBLIC
        ${PROJECT_SOURCE_DIR}/include
)

# Enable warnings for the consensus library.
enable_project_warnings(miniraft_core)

# Build the command-line Raft node executable.
add_executable(miniraft_node
    apps/node_main.cpp
)

# Connect the executable to the consensus library.
target_link_libraries(miniraft_node
    PRIVATE
        miniraft_core
)

# Enable warnings for the executable.
enable_project_warnings(miniraft_node)

# Enable CMake's test runner.
enable_testing()

# Build our Raft core unit-test executable.
add_executable(miniraft_core_tests
    tests/raft_core_tests.cpp
)

# Tests need access to the Raft core implementation.
target_link_libraries(miniraft_core_tests
    PRIVATE
        miniraft_core
)

# Tests should follow the same warning rules as production code.
enable_project_warnings(miniraft_core_tests)

# Register the unit-test executable with CTest.
add_test(
    NAME miniraft_core_tests
    COMMAND miniraft_core_tests
)

# Build the in-memory cluster integration-test executable.
add_executable(miniraft_cluster_tests
    tests/in_memory_cluster_tests.cpp
)

# Connect the integration tests to the Raft library.
target_link_libraries(miniraft_cluster_tests
    PRIVATE
        miniraft_core
)

# Apply the same compiler warnings to integration tests.
enable_project_warnings(miniraft_cluster_tests)

# Register the integration tests with CTest.
add_test(
    NAME miniraft_cluster_tests
    COMMAND miniraft_cluster_tests
)

# Keep the original executable smoke test.
add_test(
    NAME node_starts_as_follower
    COMMAND miniraft_node node-1
)

# Verify that the node prints the expected startup message.
set_tests_properties(
    node_starts_as_follower
    PROPERTIES
        PASS_REGULAR_EXPRESSION
        "node-1 started as follower in term 0"
)