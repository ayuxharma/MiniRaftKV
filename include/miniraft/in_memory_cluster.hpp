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
    //
    // All nodes receive the same cluster membership, but each node
    // receives a different random seed for its election timer.
    explicit InMemoryCluster(
        vector<string> node_ids,
        uint64_t min_election_timeout_ms = 150,
        uint64_t max_election_timeout_ms = 300,
        uint64_t random_seed = 1
    );

    // Find a node that may be modified.
    //
    // This version is used when the caller needs to start an election
    // or otherwise change the node.
    [[nodiscard]]
    RaftCore& node(const string& node_id);

    // Find a read-only node.
    //
    // This version is useful when inspecting cluster state in tests.
    [[nodiscard]]
    const RaftCore& node(const string& node_id) const;

    // Start an election and deliver all vote requests created by it.
    //
    // The return value tells us how many requests were delivered.
    [[nodiscard]]
    size_t start_election(const string& candidate_node_id);

    // Deliver the vote requests currently waiting on one node.
    //
    // This is public so future tests can separate message creation
    // from message delivery.
    [[nodiscard]]
    size_t deliver_request_vote_actions(
        const string& candidate_node_id
    );

    // Return the number of nodes in the simulated cluster.
    [[nodiscard]]
    size_t size() const;

private:
    // The simulator owns every Raft node.
    vector<RaftCore> nodes_;
};

}  // namespace miniraft