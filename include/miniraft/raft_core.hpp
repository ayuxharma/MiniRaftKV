#pragma once // Prevents this header file from being included multiple times in the same file

#include <cstdint>
#include <string>
#include <string_view>

namespace miniraft {

// In Raft, every server (node) is always in exactly one of these three states.
enum class NodeRole {
    follower,  // Listens to the Leader. All nodes start as Followers.
    candidate, // Trying to become a Leader by asking other nodes for votes.
    leader     // The boss. Handles all client requests and sends updates to Followers.
};

// A helper function to easily print the NodeRole (e.g., turning NodeRole::leader into "leader")
std::string_view to_string(NodeRole role);

// The core class that represents a single server/node in your Raft cluster.
class RaftCore {
    public:
        // Constructor: When you create a node, you must give it a unique name/ID.
        explicit RaftCore(std::string node_id);

        // Getters to safely read the node's current state from outside the class.
        // [[nodiscard]] is a C++ feature that throws a compiler warning if you call 
        // this function but forget to save or use the returned value.
        [[nodiscard]] const std::string& node_id() const;
        [[nodiscard]] NodeRole role() const;
        [[nodiscard]] std::uint64_t current_term() const;

    private:
        // The unique identifier for this specific server (e.g., "Server_A" or "192.168.1.5").
        std::string node_id_; 
        
        // When a Raft node boots up, it ALWAYS starts as a Follower.
        NodeRole role_{NodeRole::follower}; 
        
        // The "Term" acts like a logical clock or an election number in Raft. 
        // It starts at 0. Every time an election starts, this number goes up by 1.
        // It helps nodes figure out if they are looking at outdated information.
        std::uint64_t current_term_{0};
};

} // namespace miniraft