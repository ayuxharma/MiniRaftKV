# MiniRaftKV

MiniRaftKV is a compact implementation of the Raft consensus algorithm and a
replicated key-value store in C++20.

The project demonstrates how multiple processes elect one leader, replicate an
ordered command log, commit commands through majority agreement, recover state
after restart, and expose the system through gRPC, a CLI, and a small browser
dashboard.

## Features

- Three-node Raft cluster with follower, candidate, and leader roles
- Randomized election timeouts and `RequestVote` elections
- Leader heartbeats and log replication through `AppendEntries`
- Conflicting-log detection and suffix replacement
- Per-follower `next_index` and `match_index` tracking
- Majority-based commit advancement with the current-term safety rule
- Ordered, exactly-once application of committed log entries
- Replicated `SET` and `DELETE` commands
- Leader-served `GET` requests against committed state
- Persistent term, vote, log, and commit index
- Recovery and committed-log replay after restart
- Protocol Buffer messages and synchronous gRPC services
- Deadline-bound peer RPCs and thread-safe service handlers
- Automatic runtime loop for elections, heartbeats, and replication
- CLI client that automatically discovers the current leader
- Cluster-status RPC and browser dashboard
- Deterministic in-memory simulation and localhost integration tests
- Docker images for the C++ node and dashboard

## Architecture

```text
Browser
   |
   | HTTP
   v
Dashboard gateway
   |
   | gRPC
   v
+-------------+       RequestVote / AppendEntries       +-------------+
| Raft node 1 | <--------------------------------------> | Raft node 2 |
+-------------+                                          +-------------+
       ^                                                        ^
       |                                                        |
       +--------------------> +-------------+ <-----------------+
                              | Raft node 3 |
                              +-------------+

Inside every node:

RaftServiceImpl -> RaftCore -> KeyValueStore
                       |
                       v
                FileRaftStorage
```

`RaftCore` contains the consensus rules and does not depend on gRPC. The
network layer converts between internal C++ messages and Protocol Buffer
messages. This separation allows the algorithm to be tested deterministically
without opening network ports.

## How a write is committed

1. A client sends `SET` or `DELETE` to the leader.
2. The leader appends the command to its local log.
3. The leader sends missing entries to its followers.
4. Followers verify the previous log position before accepting entries.
5. After a majority confirms a current-term entry, the leader advances its
   commit index.
6. Each node applies committed entries in order to its key-value store.
7. Later heartbeats propagate the commit index to followers.

Uncommitted entries never modify visible key-value state.

## Technology stack

- C++20
- CMake and Ninja
- gRPC C++
- Protocol Buffers
- CTest
- Node.js and Express for the HTTP gateway
- HTML, CSS, and JavaScript for the dashboard
- Docker
- Git and GitHub

## Repository structure

```text
MiniRaftKV/
├── apps/
│   ├── client_main.cpp         # CLI client
│   └── node_main.cpp           # Raft node process
├── include/miniraft/           # Public C++ headers
├── proto/
│   └── raft.proto              # Internal and client RPC definitions
├── src/                        # Raft, storage, runtime, and gRPC code
├── tests/                      # Unit and integration tests
├── web/
│   ├── public/                 # Browser dashboard
│   ├── server.js               # HTTP-to-gRPC gateway
│   └── Dockerfile
├── CMakeLists.txt
└── Dockerfile                  # C++ Raft node image
```

## Prerequisites

On macOS with Homebrew:

```bash
xcode-select --install
brew install cmake ninja protobuf grpc node
```

Skip the Xcode command if the Command Line Tools are already installed.

## Build

From the repository root:

```bash
cmake -S . -B build -G Ninja
cmake --build build
```

CMake generates the Protocol Buffer and gRPC C++ sources inside `build/`.
Generated files should not be edited manually.

## Run the tests

```bash
ctest --test-dir build --output-on-failure
```

The nine test executables cover:

- Election and voting rules
- Heartbeat handling
- Log consistency and conflict repair
- Leader replication progress
- Majority commit rules and commit propagation
- Exactly-once state-machine application
- Persistent restart recovery
- Key-value command behavior
- Protocol Buffer conversions
- Inbound and outbound gRPC communication
- Automatic Raft runtime behavior

## Run the local three-node cluster

Create the state directory:

```bash
mkdir -p data
```

Open three terminals from the repository root.

Terminal 1:

```bash
./build/miniraft_node \
  node-1 \
  127.0.0.1:50051 \
  data/node-1.state \
  127.0.0.1:50051 \
  127.0.0.1:50052 \
  127.0.0.1:50053
```

Terminal 2:

```bash
./build/miniraft_node \
  node-2 \
  127.0.0.1:50052 \
  data/node-2.state \
  127.0.0.1:50051 \
  127.0.0.1:50052 \
  127.0.0.1:50053
```

Terminal 3:

```bash
./build/miniraft_node \
  node-3 \
  127.0.0.1:50053 \
  data/node-3.state \
  127.0.0.1:50051 \
  127.0.0.1:50052 \
  127.0.0.1:50053
```

The nodes automatically elect one leader. Stop a process with `Control+C`.

## Use the CLI

With the three nodes running:

```bash
./build/miniraft_client status
./build/miniraft_client set language cpp
./build/miniraft_client get language
./build/miniraft_client delete language
```

The client tries each configured node until it finds the leader. The `status`
command queries every node and displays its role, term, known leader, commit
index, and last log index.

## Run the dashboard

Install the dashboard dependencies once:

```bash
cd web
npm install
```

Start the gateway:

```bash
npm start
```

Open [http://localhost:8080](http://localhost:8080). The dashboard shows live
cluster status and provides simple `SET`, `GET`, and `DELETE` controls.

The browser cannot call the native gRPC service directly, so `server.js`
provides a small HTTP-to-gRPC adapter. Consensus logic remains in C++.

## Build the Docker images

Docker is optional; the project can be built and run directly with CMake.

From the repository root:

```bash
docker build -t miniraft-node:local .

docker build \
  -f web/Dockerfile \
  -t miniraft-web:local \
  .
```

The root Dockerfile uses separate build and runtime stages. The dashboard image
installs only production Node.js dependencies.

## gRPC interface

Internal Raft RPCs:

- `RequestVote`
- `AppendEntries`

Client-facing RPCs:

- `Set`
- `Get`
- `Delete`
- `GetStatus`

Only the leader accepts key-value operations. Followers return
`FAILED_PRECONDITION`, and clients continue searching for the current leader.

## Persistence model

Every node persists:

- Current term
- Candidate voted for in the current term
- Complete local log
- Commit index

The key-value map is rebuilt by replaying committed log entries after restart,
so the Raft log remains the single source of truth.

## Scope and limitations

The project deliberately focuses on the Raft concepts most relevant to a
software engineering learning project.

- Cluster membership is fixed at three nodes.
- Snapshots and log compaction are not implemented.
- Dynamic membership changes are not implemented.
- RPC transport uses insecure credentials and has no authentication.
- Reads are served by the leader but do not implement Raft ReadIndex or leases.
- The file storage format is educational rather than production hardened.

## Engineering highlights

- Consensus logic is isolated from networking and persistence.
- Logical time makes election tests deterministic and fast.
- RPC deadlines prevent unavailable peers from blocking indefinitely.
- gRPC handlers synchronize access to mutable Raft state.
- Persistent and volatile Raft state are handled separately during recovery.
- The same core is exercised through simulation, localhost RPC tests, CLI
  commands, and the browser dashboard.

## Project summary

MiniRaftKV demonstrates leader election, replicated logs, majority commitment,
state-machine application, persistence, process-to-process RPC, failure
handling, automated testing, and a small user-facing interface in one focused
C++ project.
