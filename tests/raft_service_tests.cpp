#include "miniraft/raft_service.hpp"

#include "miniraft/rpc_conversion.hpp"

#include <grpcpp/grpcpp.h>
#include <iostream>
#include <string>

using miniraft::AppendEntriesRequest;
using miniraft::LogEntry;
using miniraft::NodeRole;
using miniraft::RaftCore;
using miniraft::RaftServiceImpl;
using miniraft::RequestVoteRequest;
using miniraft::to_rpc;
using grpc::StatusCode;

using grpc::ServerContext;
using grpc::Status;
using std::cerr;
using std::cout;
using std::string;

namespace rpc = miniraft::rpc;

namespace {

int failure_count = 0;

void expect(
    const bool condition,
    const string& message
) {
    if (condition) {
        cout
            << "[PASS] "
            << message
            << '\n';

        return;
    }

    cerr
        << "[FAIL] "
        << message
        << '\n';

    ++failure_count;
}

void test_request_vote_reaches_raft_core() {
    RaftCore node{
        "node-1",
        {
            "node-1",
            "node-2",
            "node-3"
        }
    };

    RaftServiceImpl service{
        node
    };

    ServerContext context;

    const rpc::RequestVoteRequest request =
        to_rpc(
            RequestVoteRequest{
                1,
                "node-2",
                0,
                0
            }
        );

    rpc::RequestVoteResponse response;

    // Call the gRPC handler directly.
    // Actual network communication comes next.
    const Status status =
        service.RequestVote(
            &context,
            &request,
            &response
        );

    expect(
        status.ok(),
        "RequestVote handler returns OK"
    );

    expect(
        response.vote_granted(),
        "RequestVote handler grants a valid vote"
    );

    expect(
        node.current_term() == 1,
        "RequestVote updates the RaftCore term"
    );

    expect(
        node.voted_for().value_or("") ==
            "node-2",
        "RequestVote records the candidate in RaftCore"
    );
}

void test_append_entries_reaches_raft_core() {
    RaftCore node{
        "node-1",
        {
            "node-1",
            "node-2",
            "node-3"
        }
    };

    RaftServiceImpl service{
        node
    };

    ServerContext context;

    const rpc::AppendEntriesRequest request =
        to_rpc(
            AppendEntriesRequest{
                1,
                "node-2",
                0,
                0,
                {
                    LogEntry{
                        1,
                        "set-value"
                    }
                },
                1
            }
        );

    rpc::AppendEntriesResponse response;

    const Status status =
        service.AppendEntries(
            &context,
            &request,
            &response
        );

    expect(
        status.ok(),
        "AppendEntries handler returns OK"
    );

    expect(
        response.success(),
        "AppendEntries accepts a matching log entry"
    );

    expect(
        response.matched_index() == 1,
        "AppendEntries reports the matched index"
    );

    expect(
        node.role() == NodeRole::follower,
        "AppendEntries keeps the receiver a follower"
    );

    expect(
        node.leader_id().value_or("") ==
            "node-2",
        "AppendEntries records the remote leader"
    );

    expect(
        node.log_entries().size() == 1 &&
            node.log_entries()[0].command ==
                "set-value",
        "AppendEntries writes the command into RaftCore"
    );

    expect(
        node.commit_index() == 1,
        "AppendEntries commits the verified entry"
    );
}

void test_client_can_set_get_and_delete() {
    // A one-node cluster commits every command immediately.
    RaftCore node{
        "node-1",
        {"node-1"}
    };

    node.start_election();

    RaftServiceImpl service{
        node
    };

    // Test SET.
    ServerContext set_context;
    rpc::SetRequest set_request;
    rpc::SetResponse set_response;

    set_request.set_key("project");
    set_request.set_value("MiniRaftKV");

    const Status set_status =
        service.Set(
            &set_context,
            &set_request,
            &set_response
        );

    expect(
        set_status.ok() &&
            set_response.log_index() == 1,
        "Set appends the first client command"
    );

    // Test GET.
    ServerContext get_context;
    rpc::GetRequest get_request;
    rpc::GetResponse get_response;

    get_request.set_key("project");

    const Status get_status =
        service.Get(
            &get_context,
            &get_request,
            &get_response
        );

    expect(
        get_status.ok() &&
            get_response.found() &&
            get_response.value() == "MiniRaftKV",
        "Get returns the committed value"
    );

    // Test DELETE.
    ServerContext delete_context;
    rpc::DeleteRequest delete_request;
    rpc::DeleteResponse delete_response;

    delete_request.set_key("project");

    const Status delete_status =
        service.Delete(
            &delete_context,
            &delete_request,
            &delete_response
        );

    expect(
        delete_status.ok() &&
            delete_response.log_index() == 2,
        "Delete appends the second client command"
    );

    // Confirm that the deleted key is gone.
    ServerContext missing_context;
    rpc::GetResponse missing_response;

    const Status missing_status =
        service.Get(
            &missing_context,
            &get_request,
            &missing_response
        );

    expect(
        missing_status.ok() &&
            !missing_response.found(),
        "Deleted key is no longer found"
    );
}

void test_follower_rejects_client_write() {
    RaftCore follower{
        "node-1",
        {
            "node-1",
            "node-2",
            "node-3"
        }
    };

    RaftServiceImpl service{
        follower
    };

    ServerContext context;
    rpc::SetRequest request;
    rpc::SetResponse response;

    request.set_key("project");
    request.set_value("MiniRaftKV");

    const Status status =
        service.Set(
            &context,
            &request,
            &response
        );

    expect(
        status.error_code() ==
            StatusCode::FAILED_PRECONDITION,
        "Follower rejects a client write"
    );
}

void test_status_reports_node_state() {
    RaftCore node{
        "node-1",
        {
            "node-1",
            "node-2",
            "node-3"
        },
        2,
        {
            LogEntry{
                2,
                "SET project MiniRaftKV"
            }
        }
    };

    // This heartbeat supplies a known leader and commits
    // the existing log entry.
    static_cast<void>(
        node.handle_append_entries(
            AppendEntriesRequest{
                3,
                "node-2",
                1,
                2,
                {},
                1
            }
        )
    );

    RaftServiceImpl service{
        node
    };

    ServerContext context;
    rpc::GetStatusRequest request;
    rpc::GetStatusResponse response;

    const Status status =
        service.GetStatus(
            &context,
            &request,
            &response
        );

    expect(
        status.ok(),
        "GetStatus handler returns OK"
    );

    expect(
        response.node_id() == "node-1" &&
            response.role() == "follower" &&
            response.current_term() == 3 &&
            response.leader_id() == "node-2" &&
            response.commit_index() == 1 &&
            response.last_log_index() == 1,
        "GetStatus returns the Raft status snapshot"
    );
}

}  // namespace

int main() {
    test_request_vote_reaches_raft_core();
    test_append_entries_reaches_raft_core();
    test_client_can_set_get_and_delete();
    test_follower_rejects_client_write();
    test_status_reports_node_state();
    if (failure_count == 0) {
        cout
            << "All Raft service tests passed.\n";

        return 0;
    }

    cerr
        << failure_count
        << " Raft service test(s) failed.\n";

    return 1;
}