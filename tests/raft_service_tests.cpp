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

}  // namespace

int main() {
    test_request_vote_reaches_raft_core();
    test_append_entries_reaches_raft_core();

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