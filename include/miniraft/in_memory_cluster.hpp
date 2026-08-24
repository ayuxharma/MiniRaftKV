#pragma once

#include "miniraft/raft_core.hpp"

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace miniraft {

using std::size_t;
using std::string;
using std::uint64_t;
using std::vector;

// A deterministic cluster used for tests and local simulations.
//
// It owns several RaftCore objects and delivers messages directly
// between them without using a real network.
class InMemoryCluster {
public:
    // Create one RaftCore object for every supplied node ID.
    explicit InMemoryCluster(
        vector<string> node_ids,
        uint64_t min_election_timeout_ms = 150,
        uint64_t max_election_timeout_ms = 300,
        uint64_t random_seed = 1
    );

    // Find a node that may be modified.
    [[nodiscard]]
    RaftCore& node(const string& node_id);

    // Find a read-only node.
    [[nodiscard]]
    const RaftCore& node(const string& node_id) const;

    // Start an election and deliver its vote requests.
    //
    // If the candidate wins, its initial heartbeats are also delivered.
    [[nodiscard]]
    size_t start_election(const string& candidate_node_id);

    // Deliver pending vote requests from one candidate.
    [[nodiscard]]
    size_t deliver_request_vote_actions(
        const string& candidate_node_id
    );

    // Queue and deliver a fresh heartbeat from the leader.
    [[nodiscard]]
    size_t send_heartbeats(const string& leader_node_id);

    // Deliver heartbeats already waiting in the leader's queue.
    [[nodiscard]]
    size_t deliver_append_entries_actions(
        const string& leader_node_id
    );

    // Return the number of nodes in the cluster.
    [[nodiscard]]
    size_t size() const;

private:
    // The simulator owns every Raft node.
    vector<RaftCore> nodes_;
};

}  // namespace miniraft