#include "miniraft/raft_peer_client.hpp"
#include "miniraft/raft_service.hpp"

#include <grpcpp/grpcpp.h>

#include <iostream>
#include <memory>
#include <string>
#include <vector>

using miniraft::AppendEntriesRequest;
using miniraft::AppendEntriesResponse;
using miniraft::LogEntry;
using miniraft::Optional;
using miniraft::RaftCore;
using miniraft::RaftPeerClient;
using miniraft::RaftServiceImpl;
using miniraft::RequestVoteRequest;
using miniraft::RequestVoteResponse;

using grpc::InsecureServerCredentials;
using grpc::Server;
using grpc::ServerBuilder;
using std::cerr;
using std::cout;
using std::string;
using std::to_string;
using std::unique_ptr;
using std::vector;

namespace {

int failure_count = 0;

void expect(
    const bool condition,
    const string& message
) {
    if (condition) {
        cout << "[PASS] " << message << '\n';
        return;
    }

    cerr << "[FAIL] " << message << '\n';
    ++failure_count;
}

void test_peer_client_sends_raft_rpcs() {
    RaftCore follower{
        "node-1",
        vector<string>{
            "node-1",
            "node-2",
            "node-3"
        }
    };

    RaftServiceImpl service{
        follower
    };

    ServerBuilder builder;
    int selected_port = 0;

    // Port zero asks the operating system to select a free test port.
    builder.AddListeningPort(
        "127.0.0.1:0",
        InsecureServerCredentials(),
        &selected_port
    );

    builder.RegisterService(
        &service
    );

    unique_ptr<Server> server =
        builder.BuildAndStart();

    expect(
        server != nullptr &&
            selected_port > 0,
        "Test gRPC server starts"
    );

    if (
        server == nullptr ||
        selected_port == 0
    ) {
        return;
    }

    RaftPeerClient client{
        "127.0.0.1:" +
        to_string(selected_port)
    };

    // Ask the remote node to vote for node-2.
    const Optional<RequestVoteResponse> vote_response =
        client.request_vote(
            RequestVoteRequest{
                1,
                "node-2",
                0,
                0
            }
        );

    expect(
        vote_response.has_value() &&
            vote_response->vote_granted,
        "Peer client sends RequestVote"
    );

    // Send one committed log entry to the remote follower.
    const Optional<AppendEntriesResponse> append_response =
        client.append_entries(
            AppendEntriesRequest{
                1,
                "node-2",
                0,
                0,
                {
                    LogEntry{
                        1,
                        "SET project MiniRaftKV"
                    }
                },
                1
            }
        );

    expect(
        append_response.has_value() &&
            append_response->success &&
            append_response->matched_index == 1,
        "Peer client sends AppendEntries"
    );

    const string* value =
        follower.key_value_store().get(
            "project"
        );

    expect(
        value != nullptr &&
            *value == "MiniRaftKV",
        "Remote follower applies the committed command"
    );

    server->Shutdown();
}

}  // namespace

int main() {
    test_peer_client_sends_raft_rpcs();

    if (failure_count == 0) {
        cout
            << "All Raft peer client tests passed.\n";

        return 0;
    }

    cerr
        << failure_count
        << " Raft peer client test(s) failed.\n";

    return 1;
}