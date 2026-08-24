#include "miniraft/in_memory_cluster.hpp"

#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

// MiniRaft types used in these tests.
using miniraft::InMemoryCluster;
using miniraft::NodeRole;

// Standard-library names used in these tests.
using std::cerr;
using std::cout;
using std::invalid_argument;
using std::string;
using std::vector;
using std::logic_error;

namespace {

int failure_count = 0;

// Check one expected condition and print a readable result.
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

// Return the membership used by the three-node tests.
vector<string> three_node_cluster() {
    return {
        "node-1",
        "node-2",
        "node-3"
    };
}

void test_cluster_creates_all_nodes_as_followers() {
    const InMemoryCluster cluster{
        three_node_cluster()
    };

    expect(
        cluster.size() == 3,
        "Cluster contains three nodes"
    );

    expect(
        cluster.node("node-1").role() == NodeRole::follower,
        "Node 1 starts as a follower"
    );

    expect(
        cluster.node("node-2").role() == NodeRole::follower,
        "Node 2 starts as a follower"
    );

    expect(
        cluster.node("node-3").role() == NodeRole::follower,
        "Node 3 starts as a follower"
    );
}

void test_cluster_elects_a_leader() {
    InMemoryCluster cluster{
        three_node_cluster()
    };

    // Node 1 starts an election, and the simulator delivers
    // its requests to node 2 and node 3.
    const auto delivered_request_count =
        cluster.start_election("node-1");

    expect(
        delivered_request_count == 2,
        "Candidate sends one vote request to each peer"
    );

    expect(
        cluster.node("node-1").role() == NodeRole::leader,
        "Candidate becomes leader after receiving a majority"
    );

    expect(
        cluster.node("node-1").current_term() == 1,
        "The first election runs in term one"
    );

    expect(
        cluster.node("node-1").votes_received() >= 2,
        "Leader received a majority of votes"
    );
}

void test_followers_record_their_vote() {
    InMemoryCluster cluster{
        three_node_cluster()
    };

    const auto delivered_request_count =
        cluster.start_election("node-1");

    // The value was already tested elsewhere in this file.
    static_cast<void>(delivered_request_count);

    expect(
        cluster.node("node-2").current_term() == 1,
        "Node 2 updates to the candidate's term"
    );

    expect(
        cluster.node("node-3").current_term() == 1,
        "Node 3 updates to the candidate's term"
    );

    expect(
        cluster.node("node-2").voted_for().value_or("") == "node-1",
        "Node 2 records its vote for node 1"
    );

    expect(
        cluster.node("node-3").voted_for().value_or("") == "node-1",
        "Node 3 records its vote for node 1"
    );
}

void test_delivery_clears_outbound_actions() {
    InMemoryCluster cluster{
        three_node_cluster()
    };

    const auto delivered_request_count =
        cluster.start_election("node-1");

    static_cast<void>(delivered_request_count);

    expect(
        cluster.node("node-1").pending_request_vote_count() == 0,
        "Delivered vote requests are removed from the queue"
    );
}

void test_newer_election_replaces_old_leader() {
    InMemoryCluster cluster{
        three_node_cluster()
    };

    const auto first_election_requests =
        cluster.start_election("node-1");

    static_cast<void>(first_election_requests);

    expect(
        cluster.node("node-1").role() == NodeRole::leader,
        "Node 1 wins the first election"
    );

    // Node 2 begins another election in a newer term.
    const auto second_election_requests =
        cluster.start_election("node-2");

    static_cast<void>(second_election_requests);

    expect(
        cluster.node("node-2").role() == NodeRole::leader,
        "Node 2 wins the newer election"
    );

    expect(
        cluster.node("node-2").current_term() == 2,
        "The newer election runs in term two"
    );

    expect(
        cluster.node("node-1").role() == NodeRole::follower,
        "Old leader steps down after seeing the newer term"
    );

    expect(
        cluster.node("node-1").current_term() == 2,
        "Old leader updates to the newer term"
    );
}

void test_one_node_cluster_elects_itself() {
    InMemoryCluster cluster{
        vector<string>{
            "node-1"
        }
    };

    const auto delivered_request_count =
        cluster.start_election("node-1");

    expect(
        delivered_request_count == 0,
        "One-node cluster sends no vote requests"
    );

    expect(
        cluster.node("node-1").role() == NodeRole::leader,
        "One-node cluster elects itself immediately"
    );
}

void test_unknown_node_is_rejected() {
    InMemoryCluster cluster{
        three_node_cluster()
    };

    bool exception_was_thrown = false;

    try {
        static_cast<void>(
            cluster.node("node-99")
        );
    } catch (const invalid_argument&) {
        exception_was_thrown = true;
    }

    expect(
        exception_was_thrown,
        "Looking up an unknown node throws an exception"
    );
}

void test_election_delivers_initial_heartbeats() {
    InMemoryCluster cluster{
        three_node_cluster(),
        100,
        100,
        1
    };

    const auto delivered_vote_requests =
        cluster.start_election("node-1");

    static_cast<void>(delivered_vote_requests);

    expect(
        cluster.node("node-1").role() == NodeRole::leader,
        "Node 1 becomes leader before heartbeat delivery"
    );

    expect(
        cluster.node("node-2").leader_id().value_or("") ==
            "node-1",
        "Initial heartbeat tells node 2 about the leader"
    );

    expect(
        cluster.node("node-3").leader_id().value_or("") ==
            "node-1",
        "Initial heartbeat tells node 3 about the leader"
    );

    expect(
        cluster.node("node-1").pending_append_entries_count() == 0,
        "Initial heartbeat actions are delivered and cleared"
    );
}

void test_repeated_heartbeats_reset_follower_timers() {
    InMemoryCluster cluster{
        three_node_cluster(),
        100,
        100,
        1
    };

    const auto delivered_vote_requests =
        cluster.start_election("node-1");

    static_cast<void>(delivered_vote_requests);

    // Move both followers close to their election deadlines.
    cluster.node("node-2").advance_time(90);
    cluster.node("node-3").advance_time(90);

    expect(
        cluster.node("node-2").election_deadline_ms() == 100,
        "Node 2 is close to its original election deadline"
    );

    expect(
        cluster.node("node-3").election_deadline_ms() == 100,
        "Node 3 is close to its original election deadline"
    );

    const auto delivered_heartbeats =
        cluster.send_heartbeats("node-1");

    expect(
        delivered_heartbeats == 2,
        "Leader sends one fresh heartbeat to each follower"
    );

    expect(
        cluster.node("node-2").election_deadline_ms() == 190,
        "Heartbeat gives node 2 a fresh waiting interval"
    );

    expect(
        cluster.node("node-3").election_deadline_ms() == 190,
        "Heartbeat gives node 3 a fresh waiting interval"
    );

    expect(
        !cluster.node("node-2").election_timeout_expired(),
        "Node 2 does not begin an election after heartbeat"
    );

    expect(
        !cluster.node("node-3").election_timeout_expired(),
        "Node 3 does not begin an election after heartbeat"
    );
}

void test_non_leader_cannot_send_heartbeats() {
    InMemoryCluster cluster{
        three_node_cluster()
    };

    bool exception_was_thrown = false;

    try {
        static_cast<void>(
            cluster.send_heartbeats("node-2")
        );
    } catch (const logic_error&) {
        exception_was_thrown = true;
    }

    expect(
        exception_was_thrown,
        "Simulator rejects heartbeat request from non-leader"
    );
}

}  // namespace

int main() {
    test_cluster_creates_all_nodes_as_followers();
    test_cluster_elects_a_leader();
    test_followers_record_their_vote();
    test_delivery_clears_outbound_actions();
    test_newer_election_replaces_old_leader();
    test_one_node_cluster_elects_itself();
    test_unknown_node_is_rejected();

    // Heartbeat delivery between simulated nodes.
test_election_delivers_initial_heartbeats();
test_repeated_heartbeats_reset_follower_timers();
test_non_leader_cannot_send_heartbeats();

    if (failure_count == 0) {
        cout << "\nAll in-memory cluster tests passed.\n";
        return 0;
    }

    cerr
        << "\n"
        << failure_count
        << " in-memory cluster expectation(s) failed.\n";

    return 1;
}