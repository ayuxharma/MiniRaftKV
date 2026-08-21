#include "miniraft/in_memory_cluster.hpp"

#include <stdexcept>

namespace miniraft {

using std::invalid_argument;

InMemoryCluster::InMemoryCluster(
    vector<string> node_ids,
    const uint64_t min_election_timeout_ms,
    const uint64_t max_election_timeout_ms,
    const uint64_t random_seed
) {
    // Without node IDs, there is no cluster to simulate.
    if (node_ids.empty()) {
        throw invalid_argument{
            "An in-memory cluster must contain at least one node"
        };
    }

    // Prevent reallocations while the nodes are being constructed.
    nodes_.reserve(node_ids.size());

    // Give every node a deterministic but different random seed.
    uint64_t next_random_seed = random_seed;

    for (const string& node_id : node_ids) {
        nodes_.emplace_back(
            node_id,
            node_ids,
            0,
            vector<LogEntry>{},
            min_election_timeout_ms,
            max_election_timeout_ms,
            next_random_seed
        );

        ++next_random_seed;
    }
}

RaftCore& InMemoryCluster::node(
    const string& node_id
) {
    // A linear search is simple and sufficient for our small cluster.
    for (RaftCore& current_node : nodes_) {
        if (current_node.node_id() == node_id) {
            return current_node;
        }
    }

    throw invalid_argument{
        "Requested node does not exist in the cluster"
    };
}

const RaftCore& InMemoryCluster::node(
    const string& node_id
) const {
    // This loop returns a read-only reference because this entire
    // member function is marked const.
    for (const RaftCore& current_node : nodes_) {
        if (current_node.node_id() == node_id) {
            return current_node;
        }
    }

    throw invalid_argument{
        "Requested node does not exist in the cluster"
    };
}

size_t InMemoryCluster::start_election(
    const string& candidate_node_id
) {
    // Ask the selected node to become a candidate.
    node(candidate_node_id).start_election();

    // Deliver all vote requests created by that election.
    return deliver_request_vote_actions(
        candidate_node_id
    );
}

size_t InMemoryCluster::deliver_request_vote_actions(
    const string& candidate_node_id
) {
    RaftCore& candidate = node(candidate_node_id);

    // Taking the actions also clears the candidate's outbound queue.
    vector<RequestVoteAction> actions =
        candidate.take_request_vote_actions();

    for (const RequestVoteAction& action : actions) {
        // Locate the follower that should receive this request.
        RaftCore& target =
            node(action.target_node_id);

        // Deliver the request directly and collect its response.
        const RequestVoteResponse response =
            target.handle_request_vote(action.request);

        // Deliver the response back to the candidate.
        candidate.receive_vote(
            action.target_node_id,
            response
        );
    }

    return actions.size();
}

size_t InMemoryCluster::size() const {
    return nodes_.size();
}

}  // namespace miniraft