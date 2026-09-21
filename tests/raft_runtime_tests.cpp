#include "miniraft/raft_runtime.hpp"

#include <grpcpp/grpcpp.h>

#include <iostream>
#include <memory>
#include <string>
#include <vector>

using miniraft::NodeRole;
using miniraft::PeerConfiguration;
using miniraft::RaftCore;
using miniraft::RaftRuntime;
using miniraft::RaftServiceImpl;

using grpc::InsecureServerCredentials;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using std::cerr;
using std::cout;
using std::string;
using std::to_string;
using std::unique_ptr;
using std::vector;

namespace rpc = miniraft::rpc;

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

unique_ptr<Server> start_test_server(
    RaftServiceImpl& service,
    int& selected_port
) {
    ServerBuilder builder;

    // Port zero asks the operating system for a free port.
    builder.AddListeningPort(
        "127.0.0.1:0",
        InsecureServerCredentials(),
        &selected_port
    );

    builder.RegisterService(
        &service
    );

    return builder.BuildAndStart();
}

void test_runtime_elects_and_replicates() {
    const vector<string> members{
        "node-1",
        "node-2",
        "node-3"
    };

    RaftCore node_1{
        "node-1",
        members
    };

    RaftCore node_2{
        "node-2",
        members
    };

    RaftCore node_3{
        "node-3",
        members
    };

    RaftServiceImpl service_1{
        node_1
    };

    RaftServiceImpl service_2{
        node_2
    };

    RaftServiceImpl service_3{
        node_3
    };

    int port_2 = 0;
    int port_3 = 0;

    unique_ptr<Server> server_2 =
        start_test_server(
            service_2,
            port_2
        );

    unique_ptr<Server> server_3 =
        start_test_server(
            service_3,
            port_3
        );

    expect(
        server_2 != nullptr &&
            server_3 != nullptr,
        "Follower test servers start"
    );

    if (
        server_2 == nullptr ||
        server_3 == nullptr
    ) {
        return;
    }

    // Node 1 will send RPCs to nodes 2 and 3.
    RaftRuntime runtime{
        service_1,
        {
            PeerConfiguration{
                "node-2",
                "127.0.0.1:" +
                    to_string(port_2)
            },
            PeerConfiguration{
                "node-3",
                "127.0.0.1:" +
                    to_string(port_3)
            }
        }
    };

    // Moving beyond the maximum timeout starts an election.
    runtime.run_once(301);

    expect(
        node_1.role() == NodeRole::leader,
        "Runtime delivers votes and elects node 1"
    );

    expect(
        node_2.leader_id().value_or("") == "node-1" &&
            node_3.leader_id().value_or("") == "node-1",
        "Runtime delivers leader heartbeats"
    );

    // Submit a client write through the synchronized service.
    ServerContext context;
    rpc::SetRequest request;
    rpc::SetResponse response;

    request.set_key("project");
    request.set_value("MiniRaftKV");

    const Status status =
        service_1.Set(
            &context,
            &request,
            &response
        );

    expect(
        status.ok(),
        "Leader accepts a client SET"
    );

    // First round replicates and commits on the leader.
    runtime.run_once(50);

    // Second round sends the new commit index to followers.
    runtime.run_once(50);

    const string* value_1 =
        node_1.key_value_store().get(
            "project"
        );

    const string* value_2 =
        node_2.key_value_store().get(
            "project"
        );

    const string* value_3 =
        node_3.key_value_store().get(
            "project"
        );

    expect(
        value_1 != nullptr &&
            value_2 != nullptr &&
            value_3 != nullptr &&
            *value_1 == "MiniRaftKV" &&
            *value_2 == "MiniRaftKV" &&
            *value_3 == "MiniRaftKV",
        "Runtime replicates and applies the committed value"
    );

    server_2->Shutdown();
    server_3->Shutdown();
}

}  // namespace

int main() {
    test_runtime_elects_and_replicates();

    if (failure_count == 0) {
        cout
            << "All Raft runtime tests passed.\n";

        return 0;
    }

    cerr
        << failure_count
        << " Raft runtime test(s) failed.\n";

    return 1;
}