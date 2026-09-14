#pragma once

#include "miniraft/raft_core.hpp"
#include "raft.pb.h"

namespace miniraft {

// Convert vote requests between RaftCore and Protocol Buffers.
[[nodiscard]]
rpc::RequestVoteRequest to_rpc(
    const RequestVoteRequest& request
);

[[nodiscard]]
RequestVoteRequest from_rpc(
    const rpc::RequestVoteRequest& request
);

// Convert vote responses between RaftCore and Protocol Buffers.
[[nodiscard]]
rpc::RequestVoteResponse to_rpc(
    const RequestVoteResponse& response
);

[[nodiscard]]
RequestVoteResponse from_rpc(
    const rpc::RequestVoteResponse& response
);

// Convert AppendEntries requests between RaftCore
// and Protocol Buffers.
[[nodiscard]]
rpc::AppendEntriesRequest to_rpc(
    const AppendEntriesRequest& request
);

[[nodiscard]]
AppendEntriesRequest from_rpc(
    const rpc::AppendEntriesRequest& request
);

// Convert AppendEntries responses between RaftCore
// and Protocol Buffers.
[[nodiscard]]
rpc::AppendEntriesResponse to_rpc(
    const AppendEntriesResponse& response
);

[[nodiscard]]
AppendEntriesResponse from_rpc(
    const rpc::AppendEntriesResponse& response
);

}  // namespace miniraft