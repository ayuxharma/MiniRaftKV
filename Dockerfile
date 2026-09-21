# Stage 1 compiles the C++ Raft node.
FROM ubuntu:24.04 AS builder

# Avoid interactive questions while packages are installed.
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    libgrpc++-dev \
    libprotobuf-dev \
    ninja-build \
    protobuf-compiler \
    protobuf-compiler-grpc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /source

# Copy only the files required to compile the node.
COPY CMakeLists.txt ./
COPY apps ./apps
COPY include ./include
COPY proto ./proto
COPY src ./src
COPY tests ./tests

RUN cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release \
    && cmake --build build --target miniraft_node

# Stage 2 is the smaller image that actually runs the node.
FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

# Install only the runtime libraries needed by the compiled executable.
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgrpc++-dev \
    libprotobuf-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /source/build/miniraft_node /app/miniraft_node

# Raft state can be stored here through a Docker volume.
RUN mkdir -p /data

EXPOSE 50051

# Docker Compose will provide the node ID, addresses, and storage path.
ENTRYPOINT ["/app/miniraft_node"]
