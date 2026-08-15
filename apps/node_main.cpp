#include "miniraft/raft_core.hpp"

#include <exception>
#include <iostream>

// Used for node IDs and the cluster membership list.
#include <string>
#include <vector>

// Import only the names used by this file.
using miniraft::RaftCore;
using miniraft::to_string;
using std::cerr;
using std::cout;
using std::exception;
using std::string;
using std::vector;

int main(int argc, char* argv[]) {
    // The program expects exactly one argument after the executable name.
    //
    // Example:
    // ./miniraft_node node-1
    if (argc != 2) {
        cerr << "Usage: miniraft_node <node-id>\n";
        return 1;
    }

    try {
        // This is our first static three-node cluster configuration.
        const vector<string> cluster_members{
            "node-1",
            "node-2",
            "node-3"
        };

        // Construct one Raft node using the supplied command-line ID.
        RaftCore node{
            string{argv[1]},
            cluster_members
        };

        // Display the node's initial state.
        cout
            << node.node_id()
            << " started as "
            << to_string(node.role())
            << " in term "
            << node.current_term()
            << '\n';

        return 0;
    } catch (const exception& error) {
        // Convert configuration and startup failures into readable messages.
        cerr
            << "Failed to start node: "
            << error.what()
            << '\n';

        return 1;
    }
}