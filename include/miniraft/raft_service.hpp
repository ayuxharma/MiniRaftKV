#pragma once

#include "miniraft/raft_core.hpp"
#include "raft.grpc.pb.h"

#include <grpcpp/grpcpp.h>
#include <mutex>

namespace miniraft {

using grpc::ServerContext;
using grpc::Status;
using std::mutex;

// Receives synchronous gRPC calls and forwards them to one RaftCore object.
class RaftServiceImpl final :
    public rpc::RaftService::Service {
public:
    explicit RaftServiceImpl(
        RaftCore& raft_core
    );

    // Thread-safe methods used by the local runtime loop.
bool tick(uint64_t elapsed_ms);

[[nodiscard]]
NodeRole current_role();

void queue_heartbeats_if_leader();

[[nodiscard]]
vector<RequestVoteAction> take_request_vote_actions();

[[nodiscard]]
vector<AppendEntriesAction> take_append_entries_actions();

void receive_vote(
    const string& voter_id,
    const RequestVoteResponse& response
);

void receive_append_entries_response(
    const string& follower_id,
    const AppendEntriesResponse& response
);

    // Handle a vote request received from another node.
    Status RequestVote(
        ServerContext* context,
        const rpc::RequestVoteRequest* request,
        rpc::RequestVoteResponse* response
    ) override;

    // Handle a heartbeat or log-replication request.
    Status AppendEntries(
        ServerContext* context,
        const rpc::AppendEntriesRequest* request,
        rpc::AppendEntriesResponse* response
    ) override;

    // Append a SET command through the current leader.
    Status Set(
        ServerContext* context,
        const rpc::SetRequest* request,
        rpc::SetResponse* response
    ) override;

// Read one committed value from the current leader.
Status Get(
    ServerContext* context,
    const rpc::GetRequest* request,
    rpc::GetResponse* response
) override;

// Append a DELETE command through the current leader.
Status Delete(
    ServerContext* context,
    const rpc::DeleteRequest* request,
    rpc::DeleteResponse* response
) override;


// Return a small status snapshot from any node.
Status GetStatus(
    ServerContext* context,
    const rpc::GetStatusRequest* request,
    rpc::GetStatusResponse* response
) override;

private:
    RaftCore& raft_core_;

    // gRPC can execute requests on different threads.
    // Only one RPC handler may modify RaftCore at a time.
    mutex raft_mutex_;
};

}  // namespace miniraft