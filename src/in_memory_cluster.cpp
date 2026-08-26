#include "miniraft/in_memory_cluster.hpp"

#include <stdexcept>

namespace miniraft {

using std::invalid_argument;
using std::logic_error;

InMemoryCluster::InMemoryCluster(
    vector<string> node_ids,
    const uint64_t min_election_timeout_ms,
    const uint64_t max_election_timeout_ms,
    const uint64_t random_seed
) {
    // A simulation needs at least one node.
    if (node_ids.empty()) {
        throw invalid_argument{
            "An in-memory cluster must contain at least one node"
        };
    }

    // Prevent vector reallocations while constructing the nodes.
    nodes_.reserve(node_ids.size());

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
    RaftCore& candidate =
        node(candidate_node_id);

    candidate.start_election();

    const size_t delivered_vote_requests =
        deliver_request_vote_actions(
            candidate_node_id
        );

    // A successful election queues an immediate heartbeat.
    if (candidate.role() == NodeRole::leader) {
        static_cast<void>(
            deliver_append_entries_actions(
                candidate_node_id
            )
        );
    }

    return delivered_vote_requests;
}

size_t InMemoryCluster::deliver_request_vote_actions(
    const string& candidate_node_id
) {
    RaftCore& candidate =
        node(candidate_node_id);

    vector<RequestVoteAction> actions =
        candidate.take_request_vote_actions();

    for (const RequestVoteAction& action : actions) {
        RaftCore& target =
            node(action.target_node_id);

        const RequestVoteResponse response =
            target.handle_request_vote(
                action.request
            );

        candidate.receive_vote(
            action.target_node_id,
            response
        );
    }

    return actions.size();
}

size_t InMemoryCluster::send_heartbeats(
    const string& leader_node_id
) {
    RaftCore& leader =
        node(leader_node_id);

    // Create a fresh action for every follower.
    leader.queue_heartbeat_actions();

    return deliver_append_entries_actions(
        leader_node_id
    );
}

bool InMemoryCluster::replicate_until_caught_up(
    const string& leader_node_id,
    const size_t max_rounds
) {
    RaftCore& leader =
        node(leader_node_id);

    if (leader.role() != NodeRole::leader) {
        throw logic_error{
            "Only a leader can replicate log entries"
        };
    }

    // No network round is needed when all followers already match.
    if (all_followers_caught_up(leader)) {
        return true;
    }

    for (size_t round = 0; round < max_rounds; ++round) {
        // queue_heartbeat_actions() creates an individualized
        // AppendEntries request using each follower's next_index.
        static_cast<void>(
            send_heartbeats(leader_node_id)
        );

        // A response with a newer term may cause this node
        // to step down while messages are being delivered.
        if (leader.role() != NodeRole::leader) {
            return false;
        }

        if (all_followers_caught_up(leader)) {
            return true;
        }
    }

    // The retry limit prevents a broken or unreachable cluster
    // from creating an infinite loop.
    return false;
}

size_t InMemoryCluster::deliver_append_entries_actions(
    const string& leader_node_id
) {
    RaftCore& leader =
        node(leader_node_id);

    vector<AppendEntriesAction> actions =
        leader.take_append_entries_actions();

    for (const AppendEntriesAction& action : actions) {
        RaftCore& target =
            node(action.target_node_id);

        const AppendEntriesResponse response =
            target.handle_append_entries(
                action.request
            );

        leader.receive_append_entries_response(
            action.target_node_id,
            response
        );
    }

    return actions.size();
}

bool InMemoryCluster::all_followers_caught_up(
    const RaftCore& leader
) const {
    const uint64_t leader_last_index =
        leader.last_log_index();

    for (const RaftCore& current_node : nodes_) {
        // A leader does not replicate entries to itself.
        if (current_node.node_id() == leader.node_id()) {
            continue;
        }

        if (
            leader.match_index_for(current_node.node_id()) <
            leader_last_index
        ) {
            return false;
        }
    }

    return true;
}

size_t InMemoryCluster::size() const {
    return nodes_.size();
}

}  // namespace miniraft