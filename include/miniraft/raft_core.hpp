#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

namespace miniraft {

// Import the standard library names used frequently in this header.
using std::size_t;
using std::string;
using std::string_view;
using std::uint64_t;
using std::unordered_set;
using std::vector;

// Create our own readable alias for the optional template.
template <typename ValueType>
using Optional = std::optional<ValueType>;

    // every raft node must be exactly in one of these roles
enum class NodeRole {
    follower ,
    candidate ,
    leader
} ;

// Convert a NodeRole into readable text for logs and terminal output.
string_view to_string(NodeRole role) ;

class RaftCore {

    public :
    // Create a node with its own ID and the complete cluster membership list.
    //
    // Example:
    // node_id = "node-1"
    // cluster_members = {"node-1", "node-2", "node-3"}
    RaftCore(string node_id, vector<string> cluster_members);

    // Read-only accessors for inspecting the node's current state.
    [[nodiscard]] const string& node_id() const;
    [[nodiscard]] NodeRole role() const;
    [[nodiscard]] uint64_t current_term() const;
    [[nodiscard]] const Optional<string>& voted_for() const;
    [[nodiscard]] size_t votes_received() const;
    [[nodiscard]] size_t cluster_size() const;

    // begin a new election
    //
    // the node :
    // 1. increment its term
    // 2. becomes a candidate
    // 3. votes for itself
    void start_election() ;

    // process a vote received from another cluster member
    // 
    // in a later stage, this method will be called after receiving a RequestVoteResponse over the network
    void receive_vote(
        const string& voter_id ,
        uint64_t response_term ,
        bool vote_granted
    ) ;

    private :
    // move the node back to follower state
    //
    // this happens when the node discovers a newer term
    void become_follower(uint64_t new_term) ;

    // calculate how many votes are required to win
    [[nodiscard]] size_t majority_size() const ;

    // unique identity of this node
    string node_id_ ;

    // all valid node IDs are in the cluster, incuding this node
    unordered_set<string> cluster_members_ ;

    // all nodes begin as followers
    NodeRole role_{NodeRole::follower} ;

    // terms are logical election generations.
    // a newly created cluster begins at term zero
    uint64_t current_term_{0} ;
    
    // empty means this node has not voted in the current term
    Optional<string> voted_for_ ;

    // set of node IDs whose votes this candidate has received
    // 
    // an unordered_set automatically prevents duplicate votes
    unordered_set<string> votes_received_ ;

} ;

} // namespace miniraft