#include "miniraft/raft_core.hpp"

#include <stdexcept>
#include <utility>

namespace miniraft
{
    using std::invalid_argument ;
    using std::logic_error ;
    using std::move ;

    string_view to_string(const NodeRole role) {
        // convert enum value into a human readable string
        switch (role)
        {
        case NodeRole::follower:
            return "follower" ;
        
        case NodeRole::candidate:
            return "candidate";

        case NodeRole::leader:
            return "leader";
        }

        // yahan code aagya means role contains an invalid value
        throw logic_error{"Unknown raft node role"} ;
    }

RaftCore::RaftCore(
    string node_id ,
    vector<string> cluster_members 
)
    // moving unsupplied node ID into this object instead of copying it
    : node_id_{move(node_id)} {
        
        // every node must have a non-empty identity
        if (node_id_.empty()) {
            throw invalid_argument{"Node ID cannot be empty"} ;
        }

        if (cluster_members.empty() || cluster_members.size()%2==0) {
            throw invalid_argument{
                "Cluster must contain a non-zero odd number of nodes"
            } ;
        }

        // validate and copy each configured member into the set
        for (const string& member_id : cluster_members) {
            if (member_id.empty()) {
                throw invalid_argument {"Cluster member ID cannot be empty"} ;
            }
            
            // insert() returns a pair
            // the second value is false if ID already existed
            const auto [iterator, was_inserted] = cluster_members_.insert(member_id) ;

            // the iterator is intentionally unused
            // the cast makes that explicit to the compiler
            static_cast<void> (iterator) ;

            if (!was_inserted) {
                throw invalid_argument{"Cluster member IDs must be unique"} ;
            }
        }

        // a node cannot belong to a cluster config that omits itseld
        // find() returns end() when the requested node ID does not exist.
if (
    cluster_members_.find(node_id_) ==
    cluster_members_.end()
) {
    throw invalid_argument{
        "Cluster membership must contain this node's ID"
    };
}
    }
    const string& RaftCore::node_id() const {
    return node_id_;
}

NodeRole RaftCore::role() const {
    return role_;
}

uint64_t RaftCore::current_term() const {
    return current_term_;
}

// Return the candidate this node voted for.
// The Optional is empty when this node has not voted.
const Optional<string>& RaftCore::voted_for() const {
    return voted_for_;
}

size_t RaftCore::votes_received() const {
    return votes_received_.size();
}

size_t RaftCore::cluster_size() const {
    return cluster_members_.size();
}

size_t RaftCore::majority_size() const {
    // Integer division intentionally removes any fraction.
    //
    // Three nodes:
    // 3 / 2 + 1 = 2
    //
    // Five nodes:
    // 5 / 2 + 1 = 3
    return cluster_size() / 2 + 1;
}

void RaftCore::start_election() {
    // Every new election begins in a new term.
    ++current_term_;

    // The follower becomes a candidate.
    role_ = NodeRole::candidate;

    // Clear votes left over from any previous election.
    votes_received_.clear();

    // A candidate always votes for itself.
    voted_for_ = node_id_;
    votes_received_.insert(node_id_);

    // A one-node cluster already has a majority after voting for itself.
    if (votes_received() >= majority_size()) {
        role_ = NodeRole::leader;
    }
}

void RaftCore::receive_vote(
    const string& voter_id,
    const uint64_t response_term,
    const bool vote_granted
) {
    // Only configured cluster members are allowed to vote.
    // Reject votes from IDs that are not part of this cluster.
if (
    cluster_members_.find(voter_id) ==
    cluster_members_.end()
) {
    throw invalid_argument{
        "Received a vote from an unknown cluster member"
    };
}

    // A higher term means this node's information is outdated.
    //
    // Raft requires a node to update its term and become a follower
    // whenever it discovers a newer term.
    if (response_term > current_term_) {
        become_follower(response_term);
        return;
    }

    // A response from an older term is stale and must be ignored.
    if (response_term < current_term_) {
        return;
    }

    // Only a candidate is currently collecting votes.
    if (role_ != NodeRole::candidate) {
        return;
    }

    // A rejected vote does not contribute to the majority.
    if (!vote_granted) {
        return;
    }

    // Because this is a set, duplicate votes do not increase the count.
    votes_received_.insert(voter_id);

    // Once a majority has voted for this candidate, it becomes leader.
    if (votes_received() >= majority_size()) {
        role_ = NodeRole::leader;
    }
}

void RaftCore::become_follower(const uint64_t new_term) {
    // This private method should only be used with a newer term.
    if (new_term <= current_term_) {
        throw logic_error{
            "Cannot become follower using an old or equal term"
        };
    }

    // Record the newer term.
    current_term_ = new_term;

    // Return to follower state.
    role_ = NodeRole::follower;

    // This node has not voted in the new term yet.
    voted_for_.reset();

    // Votes from an older election are no longer relevant.
    votes_received_.clear();
}

}  // namespace miniraft