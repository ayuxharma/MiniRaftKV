#include "miniraft/raft_service.hpp"

#include "miniraft/rpc_conversion.hpp"

#include <mutex>

namespace miniraft {

using std::lock_guard;

RaftServiceImpl::RaftServiceImpl(
    RaftCore& raft_core
)
    : raft_core_{raft_core} {
}

Status RaftServiceImpl::RequestVote(
    ServerContext* context,
    const rpc::RequestVoteRequest* request,
    rpc::RequestVoteResponse* response
) {
    // We do not currently need cancellation information
    // or other details from ServerContext.
    static_cast<void>(context);

    // Protect RaftCore from simultaneous gRPC requests.
    lock_guard<mutex> lock{
        raft_mutex_
    };

    // Convert the network request into our internal type.
    const RequestVoteRequest internal_request =
        from_rpc(
            *request
        );

    // Reuse the RequestVote logic already implemented
    // inside RaftCore.
    const RequestVoteResponse internal_response =
        raft_core_.handle_request_vote(
            internal_request
        );

    // Convert the result into a network response.
    *response =
        to_rpc(
            internal_response
        );

    // The RPC itself completed successfully.
    return Status::OK;
}

Status RaftServiceImpl::AppendEntries(
    ServerContext* context,
    const rpc::AppendEntriesRequest* request,
    rpc::AppendEntriesResponse* response
) {
    static_cast<void>(context);

    lock_guard<mutex> lock{
        raft_mutex_
    };

    const AppendEntriesRequest internal_request =
        from_rpc(
            *request
        );

    // The same method handles both heartbeats and
    // log-entry replication.
    const AppendEntriesResponse internal_response =
        raft_core_.handle_append_entries(
            internal_request
        );

    *response =
        to_rpc(
            internal_response
        );

    return Status::OK;
}

}  // namespace miniraft