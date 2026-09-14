# This project requires CMake 3.28 or newer.
cmake_minimum_required(VERSION 3.28)

# Define the project's basic information.
project(
    MiniRaftKV
    VERSION 0.2.0
    DESCRIPTION "A small Raft consensus and file storage implementation in C++"
    LANGUAGES CXX
)

# Require portable C++20 for every target.
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Locate the installed Protocol Buffers and gRPC packages.
find_package(Protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)

# Apply strong compiler warnings to one target.
function(enable_project_warnings target_name)
    if(MSVC)
        target_compile_options(
            ${target_name}
            PRIVATE
                /W4
                /permissive-
        )
    else()
        target_compile_options(
            ${target_name}
            PRIVATE
                -Wall
                -Wextra
                -Wpedantic
        )
    endif()
endfunction()

# ---------------------------------------------------------
# gRPC and Protocol Buffer code generation
# ---------------------------------------------------------

# Source file containing the Raft network contract.
set(RAFT_PROTO_FILE
    "${PROJECT_SOURCE_DIR}/proto/raft.proto"
)

# Keep generated files in the build directory.
set(RPC_GENERATED_DIR
    "${PROJECT_BINARY_DIR}/generated"
)

file(MAKE_DIRECTORY
    "${RPC_GENERATED_DIR}"
)

# Ordinary Protocol Buffer-generated files.
set(RAFT_PROTO_SOURCE
    "${RPC_GENERATED_DIR}/raft.pb.cc"
)

set(RAFT_PROTO_HEADER
    "${RPC_GENERATED_DIR}/raft.pb.h"
)

# gRPC-generated service files.
set(RAFT_GRPC_SOURCE
    "${RPC_GENERATED_DIR}/raft.grpc.pb.cc"
)

set(RAFT_GRPC_HEADER
    "${RPC_GENERATED_DIR}/raft.grpc.pb.h"
)

# Generate C++ messages and synchronous gRPC service classes.
add_custom_command(
    OUTPUT
        "${RAFT_PROTO_SOURCE}"
        "${RAFT_PROTO_HEADER}"
        "${RAFT_GRPC_SOURCE}"
        "${RAFT_GRPC_HEADER}"

    COMMAND
        protobuf::protoc

    ARGS
        --proto_path=${PROJECT_SOURCE_DIR}/proto
        --cpp_out=${RPC_GENERATED_DIR}
        --grpc_out=${RPC_GENERATED_DIR}
        --plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
        ${RAFT_PROTO_FILE}

    DEPENDS
        "${RAFT_PROTO_FILE}"

    COMMENT
        "Generating Raft gRPC C++ files"

    VERBATIM
)

# Compile generated messages and service interfaces as
# a separate library.
add_library(miniraft_rpc
    "${RAFT_PROTO_SOURCE}"
    "${RAFT_PROTO_HEADER}"
    "${RAFT_GRPC_SOURCE}"
    "${RAFT_GRPC_HEADER}"
)

# Generated headers such as raft.pb.h will be included
# from the build/generated directory.
target_include_directories(miniraft_rpc
    PUBLIC
        "${RPC_GENERATED_DIR}"
)

# Link the generated library with Protobuf and gRPC.
target_link_libraries(miniraft_rpc
    PUBLIC
        protobuf::libprotobuf
        gRPC::grpc++
)

# Test conversion between internal and network messages.
add_executable(miniraft_rpc_conversion_tests
    tests/rpc_conversion_tests.cpp
)

target_link_libraries(miniraft_rpc_conversion_tests
    PRIVATE
        miniraft_rpc_adapter
)

enable_project_warnings(
    miniraft_rpc_conversion_tests
)

add_test(
    NAME miniraft_rpc_conversion_tests
    COMMAND miniraft_rpc_conversion_tests
)

# We deliberately do not apply project warnings to
# machine-generated Protocol Buffer code.

# ---------------------------------------------------------
# Main MiniRaft library
# ---------------------------------------------------------

# Build the consensus and file-storage logic as one library.
add_library(miniraft_core
    src/raft_core.cpp
    src/in_memory_cluster.cpp
    src/metadata_store.cpp
    src/metadata_command.cpp
    src/raft_storage.cpp
    src/block_store.cpp
    src/file_chunker.cpp
    src/file_service.cpp
)

# Allow code to include headers from include/miniraft.
target_include_directories(miniraft_core
    PUBLIC
        "${PROJECT_SOURCE_DIR}/include"
)

enable_project_warnings(miniraft_core)

# Keep translation between generated RPC messages and
# RaftCore structures in a separate adapter library.
add_library(miniraft_rpc_adapter
    src/rpc_conversion.cpp
)

target_include_directories(miniraft_rpc_adapter
    PUBLIC
        "${PROJECT_SOURCE_DIR}/include"
)

target_link_libraries(miniraft_rpc_adapter
    PUBLIC
        miniraft_core
        miniraft_rpc
)

enable_project_warnings(
    miniraft_rpc_adapter
)