#pragma once

#include "miniraft/raft_core.hpp"
#include "raft.grpc.pb.h"

#include <memory>
#include <string>

namespace miniraft {

using std::string;
using std::unique_ptr;

// Sends Raft's internal RPCs to one remote peer.
class RaftPeerClient {
public:
    explicit RaftPeerClient(
        const string& address
    );

    // Empty means the peer was unavailable or the call timed out.
    [[nodiscard]]
    Optional<RequestVoteResponse> request_vote(
        const RequestVoteRequest& request
    );

    // Empty means the peer was unavailable or the call timed out.
    [[nodiscard]]
    Optional<AppendEntriesResponse> append_entries(
        const AppendEntriesRequest& request
    );

private:
    // Generated gRPC client used to call the remote Raft service.
    unique_ptr<rpc::RaftService::Stub> stub_;
};

}  // namespace miniraft