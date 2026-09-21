cmake_minimum_required(VERSION 3.28)

project(
    MiniRaftKV
    VERSION 0.2.0
    DESCRIPTION "A small Raft consensus and replicated key-value store in C++"
    LANGUAGES CXX
)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(Protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)

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

# Generate C++ and gRPC code from proto/raft.proto.
set(
    RAFT_PROTO_FILE
    "${PROJECT_SOURCE_DIR}/proto/raft.proto"
)

set(
    RPC_GENERATED_DIR
    "${PROJECT_BINARY_DIR}/generated"
)

file(
    MAKE_DIRECTORY
    "${RPC_GENERATED_DIR}"
)

set(
    RAFT_PROTO_SOURCE
    "${RPC_GENERATED_DIR}/raft.pb.cc"
)

set(
    RAFT_PROTO_HEADER
    "${RPC_GENERATED_DIR}/raft.pb.h"
)

set(
    RAFT_GRPC_SOURCE
    "${RPC_GENERATED_DIR}/raft.grpc.pb.cc"
)

set(
    RAFT_GRPC_HEADER
    "${RPC_GENERATED_DIR}/raft.grpc.pb.h"
)

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

# Generated Protocol Buffer and gRPC library.
add_library(miniraft_rpc
    "${RAFT_PROTO_SOURCE}"
    "${RAFT_PROTO_HEADER}"
    "${RAFT_GRPC_SOURCE}"
    "${RAFT_GRPC_HEADER}"
)

target_include_directories(miniraft_rpc
    PUBLIC
        "${RPC_GENERATED_DIR}"
)

target_link_libraries(miniraft_rpc
    PUBLIC
        protobuf::libprotobuf
        gRPC::grpc++
)

# Raft consensus, persistence, and key-value state machine.
add_library(miniraft_core
    src/raft_core.cpp
    src/in_memory_cluster.cpp
    src/raft_storage.cpp
    src/key_value_store.cpp
)

target_include_directories(miniraft_core
    PUBLIC
        "${PROJECT_SOURCE_DIR}/include"
)

enable_project_warnings(miniraft_core)

# Networking code surrounding the pure Raft core.
add_library(miniraft_rpc_adapter
    src/rpc_conversion.cpp
    src/raft_service.cpp
    src/raft_peer_client.cpp
    src/raft_runtime.cpp
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

# Executable that runs one Raft node as a gRPC server.
add_executable(miniraft_node
    apps/node_main.cpp
)

target_link_libraries(miniraft_node
    PRIVATE
        miniraft_rpc_adapter
)

enable_project_warnings(miniraft_node)

# Small command-line client for the local three-node cluster.
add_executable(miniraft_client
    apps/client_main.cpp
)

target_link_libraries(miniraft_client
    PRIVATE
        miniraft_rpc
)

enable_project_warnings(miniraft_client)

# Enable CTest.
enable_testing()

# Helper used to avoid repeating the same configuration
# for every test executable.
function(
    add_miniraft_test
    target_name
    source_file
    linked_library
)
    add_executable(
        ${target_name}
        ${source_file}
    )

    target_link_libraries(
        ${target_name}
        PRIVATE
            ${linked_library}
    )

    enable_project_warnings(
        ${target_name}
    )

    add_test(
        NAME ${target_name}
        COMMAND ${target_name}
    )
endfunction()

add_miniraft_test(
    miniraft_core_tests
    tests/raft_core_tests.cpp
    miniraft_core
)

add_miniraft_test(
    miniraft_cluster_tests
    tests/in_memory_cluster_tests.cpp
    miniraft_core
)

add_miniraft_test(
    miniraft_storage_tests
    tests/raft_storage_tests.cpp
    miniraft_core
)

add_miniraft_test(
    miniraft_rpc_conversion_tests
    tests/rpc_conversion_tests.cpp
    miniraft_rpc_adapter
)

add_miniraft_test(
    miniraft_raft_service_tests
    tests/raft_service_tests.cpp
    miniraft_rpc_adapter
)

add_miniraft_test(
    miniraft_raft_peer_client_tests
    tests/raft_peer_client_tests.cpp
    miniraft_rpc_adapter
)

add_miniraft_test(
    miniraft_key_value_store_tests
    tests/key_value_store_tests.cpp
    miniraft_core
)

# Test the integration between Raft and KeyValueStore.
add_miniraft_test(
    miniraft_raft_key_value_tests
    tests/raft_key_value_tests.cpp
    miniraft_core
)

add_miniraft_test(
    miniraft_raft_runtime_tests
    tests/raft_runtime_tests.cpp
    miniraft_rpc_adapter
)