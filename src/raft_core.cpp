#include "miniraft/raft_core.hpp"

// We include standard exception libraries so we can throw errors if something goes wrong
#include <stdexcept> 
// We include utility for std::move, which helps with performance
#include <utility>   

namespace miniraft {

// This function converts the enum into readable text. 
// In Raft, you will be writing a lot of logs (e.g., "Node A changed from Follower to Candidate").
// This helper makes generating those log messages very easy.
std::string_view to_string(const NodeRole role) {
    switch(role) {
        case NodeRole::follower:
            return "follower";

        case NodeRole::candidate:
            return "candidate";

        case NodeRole::leader:
            return "leader";
    }

    // std::logic_error is thrown if the role doesn't match the three defined states.
    throw std::logic_error{"Unknown Raft node role"}; 
}

// Constructor Implementation
RaftCore::RaftCore(std::string node_id)
    // std::move is a C++ performance trick. Instead of copying the string 
    // character by character, it "steals" the memory address of the string 
    // and gives it to node_id_. 
    : node_id_{std::move(node_id)} {
        
        // Raft nodes must be able to uniquely identify each other to cast votes 
        // and send data. If a node has no name, the system breaks. 
        // Here, we are safely preventing the program from starting with bad data.
        if (node_id_.empty()) {
            throw std::invalid_argument{"Node ID cannot be empty"};
        }
    }

// --- Getters ---
// These functions simply return the current state of the node. 
// They are marked 'const' at the end, which is a promise to the C++ compiler 
// that calling these functions will NEVER modify the internal variables.

const std::string& RaftCore::node_id() const {
    return node_id_;
}

NodeRole RaftCore::role() const {
    return role_;
}

std::uint64_t RaftCore::current_term() const {
    return current_term_;
}

}   // namespace miniraft