#include "miniraft/in_memory_cluster.hpp"

#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

// MiniRaft types used in these tests.
using miniraft::AppendEntriesRequest;
using miniraft::InMemoryCluster;
using miniraft::LogEntry;
using miniraft::NodeRole;

using miniraft::FileMetadata;

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

// Give node 1 an existing log before it starts its election.
//
// This creates a realistic situation where the future leader
// contains entries that the other nodes do not have yet.
void seed_node_one_log(
    InMemoryCluster& cluster
) {
    const AppendEntriesRequest request{
        1,
        "node-2",
        0,
        0,
        {
            LogEntry{1, "COMMAND 1"},
            LogEntry{1, "COMMAND 2"},
            LogEntry{1, "COMMAND 3"}
        },
        0
    };

    const auto response =
        cluster.node("node-1").handle_append_entries(
            request
        );

    expect(
        response.success,
        "Node 1 receives its initial three log entries"
    );
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

void test_retries_until_followers_catch_up() {
    InMemoryCluster cluster{
        three_node_cluster()
    };

    seed_node_one_log(cluster);

    expect(
        cluster.node("node-1").last_log_index() == 3,
        "Future leader starts with three log entries"
    );

    expect(
        cluster.node("node-2").last_log_index() == 0,
        "Node 2 starts without the leader's entries"
    );

    const auto delivered_vote_requests =
        cluster.start_election("node-1");

    static_cast<void>(delivered_vote_requests);

    expect(
        cluster.node("node-1").role() == NodeRole::leader,
        "Node 1 becomes leader with the newer log"
    );

    // The initial AppendEntries sent after the election is rejected.
    // The leader initially assumes that followers already have
    // entries 1 through 3, so it must move next_index backward.
    expect(
        cluster.node("node-2").last_log_index() == 0,
        "Follower rejects the leader's first optimistic request"
    );

    const bool caught_up =
        cluster.replicate_until_caught_up(
            "node-1",
            5
        );

    expect(
        caught_up,
        "Replication succeeds before the retry limit"
    );

    expect(
        cluster.node("node-2").last_log_index() == 3,
        "Node 2 receives all three missing entries"
    );

    expect(
        cluster.node("node-3").last_log_index() == 3,
        "Node 3 receives all three missing entries"
    );

    expect(
        cluster.node("node-1").match_index_for("node-2") == 3,
        "Leader records node 2's confirmed match index"
    );

    expect(
        cluster.node("node-1").match_index_for("node-3") == 3,
        "Leader records node 3's confirmed match index"
    );

    expect(
        cluster.node("node-2").log_entries()[2].command ==
            "COMMAND 3",
        "Node 2 stores the final replicated command"
    );
}

void test_catch_up_stops_at_retry_limit() {
    InMemoryCluster cluster{
        three_node_cluster()
    };

    seed_node_one_log(cluster);

    const auto delivered_vote_requests =
        cluster.start_election("node-1");

    static_cast<void>(delivered_vote_requests);

    // Two rounds move next_index backward, but are not enough
    // to send the complete log to the empty followers.
    const bool caught_up_after_two_rounds =
        cluster.replicate_until_caught_up(
            "node-1",
            2
        );

    expect(
        !caught_up_after_two_rounds,
        "Replication reports failure when retry limit is too small"
    );

    expect(
        cluster.node("node-2").last_log_index() == 0,
        "Follower remains unchanged before a matching prefix is found"
    );

    // next_index is now one, so one additional round can send
    // all three entries.
    const bool caught_up_after_final_round =
        cluster.replicate_until_caught_up(
            "node-1",
            1
        );

    expect(
        caught_up_after_final_round,
        "Replication continues from its previous progress"
    );

    expect(
        cluster.node("node-2").last_log_index() == 3,
        "Follower catches up during the final round"
    );
}

void test_committed_command_reaches_all_state_machines() {
    InMemoryCluster cluster{
        three_node_cluster()
    };

    static_cast<void>(
        cluster.start_election("node-1")
    );

    static_cast<void>(
        cluster.node("node-1").append_command(
            "UPDATE notes.txt VERSION 1"
        )
    );

    const bool caught_up =
        cluster.replicate_until_caught_up(
            "node-1",
            5
        );

    expect(
        caught_up,
        "Leader replicates the command to every follower"
    );

    expect(
        cluster.node("node-1").commit_index() == 1,
        "Leader commits the command after majority replication"
    );

    // Requests were prepared before the leader learned that the
    // entry was committed. The next heartbeat carries that decision.
    static_cast<void>(
        cluster.send_heartbeats("node-1")
    );

    expect(
        cluster.node("node-2").commit_index() == 1 &&
            cluster.node("node-3").commit_index() == 1,
        "Heartbeat propagates the commit index to both followers"
    );

    expect(
        cluster.node("node-1").applied_commands().size() == 1 &&
            cluster.node("node-2").applied_commands().size() == 1 &&
            cluster.node("node-3").applied_commands().size() == 1,
        "Every node applies the committed command exactly once"
    );
}

void test_metadata_state_converges_on_all_nodes() {
    InMemoryCluster cluster{
        three_node_cluster()
    };

    static_cast<void>(
        cluster.start_election("node-1")
    );

    static_cast<void>(
        cluster.node("node-1").append_metadata(
            FileMetadata{
                "notes.txt",
                1,
                {
                    "hash-a",
                    "hash-b"
                },
                false
            }
        )
    );

    const bool caught_up =
        cluster.replicate_until_caught_up(
            "node-1",
            5
        );

    expect(
        caught_up,
        "Metadata command replicates to every follower"
    );

    // A fresh heartbeat propagates the leader's new commit index.
    static_cast<void>(
        cluster.send_heartbeats("node-1")
    );

    const FileMetadata* leader_metadata =
        cluster.node("node-1")
            .metadata_store()
            .find("notes.txt");

    const FileMetadata* node_2_metadata =
        cluster.node("node-2")
            .metadata_store()
            .find("notes.txt");

    const FileMetadata* node_3_metadata =
        cluster.node("node-3")
            .metadata_store()
            .find("notes.txt");

    expect(
        leader_metadata != nullptr &&
            node_2_metadata != nullptr &&
            node_3_metadata != nullptr,
        "Every node contains the committed file metadata"
    );

    expect(
        leader_metadata != nullptr &&
            node_2_metadata != nullptr &&
            node_3_metadata != nullptr &&
            leader_metadata->version ==
                node_2_metadata->version &&
            leader_metadata->version ==
                node_3_metadata->version &&
            leader_metadata->block_hashes ==
                node_2_metadata->block_hashes &&
            leader_metadata->block_hashes ==
                node_3_metadata->block_hashes,
        "All nodes converge on identical metadata state"
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

    // Automatic follower log catch-up.
    test_retries_until_followers_catch_up();
    test_catch_up_stops_at_retry_limit();

    // Commit propagation and application across the cluster.
    test_committed_command_reaches_all_state_machines();

        // Replicated file-metadata state machine.
    test_metadata_state_converges_on_all_nodes();

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
