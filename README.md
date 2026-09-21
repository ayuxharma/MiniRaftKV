# MiniRaftKV

A compact Raft consensus and replicated key-value store implementation in C++20.

MiniRaftKV was built to learn the core ideas behind distributed consensus through a small, testable codebase. It separates the Raft algorithm, persistent storage, key-value state machine, and gRPC transport so that each layer can be understood and tested independently.

> This is an educational implementation, not a production-ready database.

## What is implemented

- Static three-node Raft cluster model
- Follower, candidate, and leader roles
- Randomized election timeouts
- `RequestVote` elections with one vote per term
- Leader heartbeats through `AppendEntries`
- Log replication and conflicting-suffix replacement
- Per-follower `next_index` and `match_index` tracking
- Majority-based commit advancement
- Current-term commit rule
- Ordered, exactly-once application of committed entries
- Replicated `SET` and `DELETE` state machine commands
- `GET` access to committed key-value state
- Persistent term, vote, log, and commit index
- Recovery and committed-log replay after restart
- Protocol Buffer messages and synchronous gRPC services
- Deadline-bound outbound peer RPC client
- Thread-safe gRPC request handling
- Deterministic in-memory cluster simulation
- Automated unit and integration tests through CTest

## Architecture

```text
Client RPCs: SET / GET / DELETE
                |
                v
        +------------------+
        | RaftServiceImpl  |  gRPC boundary and synchronization
        +------------------+
                |
                v
        +------------------+
        |    RaftCore      |  elections, replication, commit rules
        +------------------+
           |            |
           v            v
 +----------------+  +------------------+
 | KeyValueStore  |  | FileRaftStorage  |
 | applied state  |  | durable Raft data|
 +----------------+  +------------------+

Internal node RPCs:
RaftPeerClient -> RequestVote / AppendEntries -> RaftServiceImpl
```

The core algorithm does not depend on gRPC. Tests can therefore run a deterministic cluster entirely in memory, while the transport layer converts the same internal Raft messages to and from Protocol Buffers.

## How a write is committed

1. A client sends a `SET` or `DELETE` request to the leader.
2. The leader appends the command to its local Raft log.
3. The leader sends the missing entries to followers with `AppendEntries`.
4. Followers validate the previous log position before accepting entries.
5. After a majority confirms an entry from the leader's current term, the leader advances `commit_index`.
6. Every node applies committed entries in order to its `KeyValueStore`.
7. Later heartbeats propagate the leader's commit index to followers.

Uncommitted entries never modify visible key-value state.

## Technology stack

- C++20
- CMake 3.28+
- Ninja
- gRPC C++
- Protocol Buffers
- CTest
- Git

## Repository structure

```text
MiniRaftKV/
├── apps/
│   └── node_main.cpp            # gRPC node executable
├── include/miniraft/
│   ├── in_memory_cluster.hpp    # deterministic cluster simulator
│   ├── key_value_store.hpp      # replicated state machine
│   ├── raft_core.hpp            # Raft algorithm and state
│   ├── raft_peer_client.hpp     # outbound Raft RPC client
│   ├── raft_service.hpp         # inbound gRPC service
│   ├── raft_storage.hpp         # persistent Raft state
│   └── rpc_conversion.hpp       # internal/protobuf conversion
├── proto/
│   └── raft.proto               # Raft and client RPC definitions
├── src/                         # implementations
├── tests/                       # unit and integration tests
└── CMakeLists.txt
```

## Prerequisites

On macOS with Homebrew:

```bash
xcode-select --install
brew install cmake ninja protobuf grpc
```

The Xcode command can be skipped if the Command Line Tools are already installed.

## Build

From the repository root:

```bash
cmake -S . -B build -G Ninja
cmake --build build
```

CMake generates the Protocol Buffer and gRPC C++ sources inside the build directory. Generated files should not be edited manually.

## Run the tests

```bash
ctest --test-dir build --output-on-failure
```

The current suite covers:

- Election and voting rules
- Heartbeat handling
- Follower log consistency
- Leader replication progress
- Majority commit rules
- Commit propagation
- Exactly-once state-machine application
- Persistent restart recovery
- Key-value command behavior
- Protocol Buffer conversions
- Inbound gRPC handlers
- Real localhost peer RPC communication

To run an individual test executable:

```bash
./build/miniraft_core_tests
./build/miniraft_cluster_tests
./build/miniraft_raft_key_value_tests
./build/miniraft_raft_peer_client_tests
```

## Run a node server

Create a directory for persistent state:

```bash
mkdir -p data
```

Start a node:

```bash
./build/miniraft_node node-1 127.0.0.1:50051 data/node-1.state
```

Arguments:

```text
miniraft_node <node-id> <listen-address> <storage-path>
```

The process remains active and serves gRPC requests until it is stopped with `Control+C`.

## gRPC interface

Internal Raft RPCs:

- `RequestVote`
- `AppendEntries`

Client-facing RPCs:

- `Set`
- `Get`
- `Delete`

Only a leader accepts client operations. Invalid commands use `INVALID_ARGUMENT`, while requests sent to a follower use `FAILED_PRECONDITION`.

## Persistence model

Each node persists the Raft state required for safe recovery:

- Current term
- Candidate voted for in the current term
- Complete local log
- Commit index

The key-value map is not stored as a second source of truth. On restart, the node reloads its Raft state and replays committed log entries to reconstruct the state machine.

## Current scope and limitations

The repository intentionally focuses on the Raft concepts most relevant to a software engineering learning project.

- Cluster membership is fixed at three nodes.
- Log compaction and snapshots are not implemented.
- Dynamic membership changes are not implemented.
- RPC transport uses insecure credentials and has no authentication.
- The executable currently hosts the gRPC service, but the automatic multi-process timer/runtime loop is not yet wired into `node_main.cpp`.
- Kubernetes manifests, Prometheus/Grafana monitoring, and the web UI are planned but not currently implemented.

The in-memory cluster and network integration tests fully exercise leader election, replication, commit propagation, and recovery without claiming production-database completeness.

## Engineering highlights

- Consensus logic is isolated from networking and storage concerns.
- Logical time makes election tests deterministic and fast.
- Network failures are represented as missing optional responses instead of process crashes.
- RPC calls have deadlines so unavailable peers cannot block indefinitely.
- gRPC handlers synchronize access to mutable Raft state.
- Persistent and volatile Raft state are handled separately during recovery.

## Project summary

MiniRaftKV demonstrates how a distributed system elects one leader, replicates an ordered command log, commits commands through majority agreement, and rebuilds application state after a restart. The project is intentionally small enough to explain end to end while still covering consensus, networking, persistence, concurrency, and automated testing.
