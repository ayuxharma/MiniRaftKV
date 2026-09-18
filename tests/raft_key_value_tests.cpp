#include "miniraft/in_memory_cluster.hpp"
#include "miniraft/key_value_store.hpp"
#include "miniraft/raft_core.hpp"

#include <filesystem>
#include <iostream>
#include <string>
#include <vector>

using miniraft::InMemoryCluster;
using miniraft::NodeRole;
using miniraft::RaftCore;
using miniraft::make_set_command;

using std::cerr;
using std::cout;
using std::filesystem::remove;
using std::filesystem::temp_directory_path;
using std::string;
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

string test_state_path() {
    return (
        temp_directory_path() /
        "miniraft_raft_key_value_tests.state"
    ).string();
}

void remove_test_state() {
    static_cast<void>(
        remove(test_state_path())
    );
}

void test_committed_value_reaches_every_node() {
    InMemoryCluster cluster{
        vector<string>{
            "node-1",
            "node-2",
            "node-3"
        }
    };

    static_cast<void>(
        cluster.start_election("node-1")
    );

    RaftCore& leader =
        cluster.node("node-1");

    expect(
        leader.role() == NodeRole::leader,
        "Node 1 becomes leader"
    );

    // The leader first adds the command to its local Raft log.
    static_cast<void>(
        leader.append_command(
            make_set_command(
                "project",
                "MiniRaftKV"
            )
        )
    );

    // Replicate the command and let the leader commit it.
    const bool replicated =
        cluster.replicate_until_caught_up(
            "node-1",
            5
        );

    // Followers learn the new commit_index through this heartbeat.
    static_cast<void>(
        cluster.send_heartbeats("node-1")
    );

    expect(
        replicated,
        "SET command is replicated to every node"
    );

    // Every state machine should now contain the same value.
    for (
        const string& node_id :
        vector<string>{
            "node-1",
            "node-2",
            "node-3"
        }
    ) {
        const string* value =
            cluster.node(node_id)
                .key_value_store()
                .get("project");

        expect(
            value != nullptr &&
                *value == "MiniRaftKV",
            node_id + " applies the committed value"
        );
    }
}

void test_committed_value_survives_restart() {
    remove_test_state();

    {
        // One node is enough because this test focuses on restart.
        RaftCore node{
            "node-1",
            vector<string>{"node-1"},
            0,
            {},
            150,
            300,
            1,
            test_state_path()
        };

        node.start_election();

        static_cast<void>(
            node.append_command(
                make_set_command(
                    "language",
                    "C++"
                )
            )
        );

        expect(
            node.commit_index() == 1,
            "Single-node leader commits the SET command"
        );
    }

    // Creating another object with the same storage path
    // simulates stopping and restarting the node.
    const RaftCore restarted_node{
        "node-1",
        vector<string>{"node-1"},
        0,
        {},
        150,
        300,
        1,
        test_state_path()
    };

    const string* value =
        restarted_node
            .key_value_store()
            .get("language");

    expect(
        value != nullptr &&
            *value == "C++",
        "Restart rebuilds the committed key-value state"
    );

    remove_test_state();
}

}  // namespace

int main() {
    test_committed_value_reaches_every_node();
    test_committed_value_survives_restart();

    if (failure_count == 0) {
        cout
            << "All Raft key-value tests passed.\n";

        return 0;
    }

    cerr
        << failure_count
        << " Raft key-value test(s) failed.\n";

    return 1;
}