#include "miniraft/raft_service.hpp"

#include "miniraft/rpc_conversion.hpp"

#include <mutex>
#include <stdexcept>

namespace miniraft {

using std::lock_guard;
using grpc::StatusCode;
using std::invalid_argument;

RaftServiceImpl::RaftServiceImpl(
    RaftCore& raft_core
)
    : raft_core_{raft_core} {
}

bool RaftServiceImpl::tick(
    const uint64_t elapsed_ms
) {
    lock_guard<mutex> lock{
        raft_mutex_
    };

    return raft_core_.tick(elapsed_ms);
}

NodeRole RaftServiceImpl::current_role() {
    lock_guard<mutex> lock{
        raft_mutex_
    };

    return raft_core_.role();
}

void RaftServiceImpl::queue_heartbeats_if_leader() {
    lock_guard<mutex> lock{
        raft_mutex_
    };

    // Check and queue while holding the same lock.
    if (raft_core_.role() == NodeRole::leader) {
        raft_core_.queue_heartbeat_actions();
    }
}

vector<RequestVoteAction>
RaftServiceImpl::take_request_vote_actions() {
    lock_guard<mutex> lock{
        raft_mutex_
    };

    return raft_core_.take_request_vote_actions();
}

vector<AppendEntriesAction>
RaftServiceImpl::take_append_entries_actions() {
    lock_guard<mutex> lock{
        raft_mutex_
    };

    return raft_core_.take_append_entries_actions();
}

void RaftServiceImpl::receive_vote(
    const string& voter_id,
    const RequestVoteResponse& response
) {
    lock_guard<mutex> lock{
        raft_mutex_
    };

    raft_core_.receive_vote(
        voter_id,
        response
    );
}

void RaftServiceImpl::receive_append_entries_response(
    const string& follower_id,
    const AppendEntriesResponse& response
) {
    lock_guard<mutex> lock{
        raft_mutex_
    };

    raft_core_.receive_append_entries_response(
        follower_id,
        response
    );
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

Status RaftServiceImpl::Set(
    ServerContext* context,
    const rpc::SetRequest* request,
    rpc::SetResponse* response
) {
    static_cast<void>(context);

    lock_guard<mutex> lock{
        raft_mutex_
    };

    // Clients must send writes to the elected leader.
    if (raft_core_.role() != NodeRole::leader) {
        return Status{
            StatusCode::FAILED_PRECONDITION,
            "Only the leader accepts client requests"
        };
    }

    try {
        const uint64_t log_index =
            raft_core_.append_command(
                make_set_command(
                    request->key(),
                    request->value()
                )
            );

        response->set_log_index(log_index);
    } catch (const invalid_argument& error) {
        return Status{
            StatusCode::INVALID_ARGUMENT,
            error.what()
        };
    }

    return Status::OK;
}

Status RaftServiceImpl::Get(
    ServerContext* context,
    const rpc::GetRequest* request,
    rpc::GetResponse* response
) {
    static_cast<void>(context);

    lock_guard<mutex> lock{
        raft_mutex_
    };

    // Reading from the leader avoids an obviously stale follower read.
    if (raft_core_.role() != NodeRole::leader) {
        return Status{
            StatusCode::FAILED_PRECONDITION,
            "Only the leader accepts client requests"
        };
    }

    const string* value =
        raft_core_.key_value_store().get(
            request->key()
        );

    response->set_found(value != nullptr);

    if (value != nullptr) {
        response->set_value(*value);
    }

    return Status::OK;
}

Status RaftServiceImpl::Delete(
    ServerContext* context,
    const rpc::DeleteRequest* request,
    rpc::DeleteResponse* response
) {
    static_cast<void>(context);

    lock_guard<mutex> lock{
        raft_mutex_
    };

    if (raft_core_.role() != NodeRole::leader) {
        return Status{
            StatusCode::FAILED_PRECONDITION,
            "Only the leader accepts client requests"
        };
    }

    try {
        const uint64_t log_index =
            raft_core_.append_command(
                make_delete_command(
                    request->key()
                )
            );

        response->set_log_index(log_index);
    } catch (const invalid_argument& error) {
        return Status{
            StatusCode::INVALID_ARGUMENT,
            error.what()
        };
    }

    return Status::OK;
}

}  // namespace miniraft