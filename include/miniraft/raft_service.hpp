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

private:
    RaftCore& raft_core_;

    // gRPC can execute requests on different threads.
    // Only one RPC handler may modify RaftCore at a time.
    mutex raft_mutex_;
};

}  // namespace miniraft