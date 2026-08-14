#include "miniraft/raft_core.hpp"

// We include standard libraries for handling errors (exception), 
// printing to the terminal (iostream), and working with text (string).
#include <exception>
#include <iostream>
#include <string>

// The main function is where the program begins. 
// argc (argument count) and argv (argument vector) allow us to pass 
// configuration data to the program right when we start it from the terminal.
int main(int argc, char* argv[]) {
    
    // In C++, the first argument (argv[0]) is always the name of the program itself.
    // We want the user to provide exactly one extra argument: the node's ID.
    // So, we need argc to be exactly 2.
    if (argc != 2) {
        // std::cerr is used for printing errors. It ensures the message 
        // gets displayed immediately, even if the system is busy.
        std::cerr << "Usage : miniraft_node <node-id>\n";
        
        // Returning 1 tells the operating system that the program failed to start correctly.
        return 1; 
    }

    // A try-catch block is our safety net. If the node creation fails, 
    // we want to shut down gracefully and log the error, rather than crashing violently.
    try {
        // This is the moment your Raft server is born!
        // We take the ID the user typed in the terminal (argv[1]) and use it 
        // to create our RaftCore object.
        miniraft::RaftCore node{std::string{argv[1]}};

        // Now, we announce to the world that this node is alive and ready.
        // Because of the rules we wrote in our class, we know this will ALWAYS 
        // print that the node is a "follower" in term "0".
        std::cout
            << node.node_id()
            << " started as " 
            << miniraft::to_string(node.role())
            << " in term " 
            << node.current_term()
            << '\n';

        // Returning 0 tells the operating system everything went perfectly.
        return 0; 
    } 
    // If our RaftCore constructor threw an error (for example, the invalid_argument 
    // error we wrote for empty strings), it gets caught right here.
    catch (const std::exception& error) {
        // error.what() prints the exact message we wrote in our throw statement.
        std::cerr << "Failed to start node: " << error.what() << '\n';
        return 1;
    }
}