#include "miniraft/raft_core.hpp"
#include "miniraft/raft_runtime.hpp"
#include "miniraft/raft_service.hpp"

#include <grpcpp/grpcpp.h>

#include <chrono>
#include <cstdint>
#include <exception>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

using miniraft::NodeRole;
using miniraft::PeerConfiguration;
using miniraft::RaftCore;
using miniraft::RaftRuntime;
using miniraft::RaftServiceImpl;
using miniraft::to_string;

using grpc::InsecureServerCredentials;
using grpc::Server;
using grpc::ServerBuilder;
using std::cerr;
using std::chrono::duration_cast;
using std::chrono::milliseconds;
using std::chrono::steady_clock;
using std::cout;
using std::endl;
using std::exception;
using std::runtime_error;
using std::string;
using std::this_thread::sleep_for;
using std::uint64_t;
using std::unique_ptr;
using std::vector;

namespace {

// Different seeds give the nodes different election timeouts.
uint64_t random_seed_for(
    const string& node_id
) {
    if (node_id == "node-1") {
        return 1;
    }

    if (node_id == "node-2") {
        return 2;
    }

    if (node_id == "node-3") {
        return 3;
    }

    throw runtime_error{
        "Node ID must be node-1, node-2, or node-3"
    };
}

}  // namespace

int main(int argc, char* argv[]) {
    // Example:
    // ./miniraft_node node-1 127.0.0.1:50051 data/node-1.state \
    //     127.0.0.1:50051 127.0.0.1:50052 127.0.0.1:50053
    if (argc != 7) {
        cerr
            << "Usage: miniraft_node "
            << "<node-id> <listen-address> <storage-path> "
            << "<node-1-address> <node-2-address> <node-3-address>\n";

        return 1;
    }

    try {
        const string node_id{
            argv[1]
        };

        const string listen_address{
            argv[2]
        };

        const string storage_path{
            argv[3]
        };

        const vector<string> cluster_members{
            "node-1",
            "node-2",
            "node-3"
        };

        // Every process receives the same address list.
        const vector<PeerConfiguration> all_nodes{
            PeerConfiguration{
                "node-1",
                argv[4]
            },
            PeerConfiguration{
                "node-2",
                argv[5]
            },
            PeerConfiguration{
                "node-3",
                argv[6]
            }
        };

        // The runtime needs clients only for the other two nodes.
        vector<PeerConfiguration> peers;
        peers.reserve(2);

        for (const PeerConfiguration& node : all_nodes) {
            if (node.node_id != node_id) {
                peers.push_back(node);
            }
        }

        RaftCore node{
            node_id,
            cluster_members,
            0,
            {},
            150,
            300,
            random_seed_for(node_id),
            storage_path
        };

        RaftServiceImpl service{
            node
        };

        ServerBuilder builder;

        builder.AddListeningPort(
            listen_address,
            InsecureServerCredentials()
        );

        builder.RegisterService(
            &service
        );

        unique_ptr<Server> server =
            builder.BuildAndStart();

        if (server == nullptr) {
            throw runtime_error{
                "gRPC server could not start"
            };
        }

        RaftRuntime runtime{
            service,
            peers
        };

        cout
            << node_id
            << " started as "
            << to_string(node.role())
            << " and is listening on "
            << listen_address
            << endl;

        NodeRole previous_role =
            service.current_role();

        auto previous_time =
            steady_clock::now();

        while (true) {
            // Prevent the runtime loop from consuming an entire CPU.
            sleep_for(
                milliseconds{10}
            );

            const auto current_time =
                steady_clock::now();

            // Use real elapsed time instead of assuming that every
            // runtime iteration takes exactly ten milliseconds.
            const auto elapsed =
                duration_cast<milliseconds>(
                    current_time - previous_time
                );

            previous_time = current_time;

            runtime.run_once(
                static_cast<uint64_t>(
                    elapsed.count()
                )
            );

            const NodeRole current_role =
                service.current_role();

            if (current_role != previous_role) {
                cout
                    << node_id
                    << " changed role to "
                    << to_string(current_role)
                    << endl;

                previous_role = current_role;
            }
        }
    } catch (const exception& error) {
        cerr
            << "Failed to start node: "
            << error.what()
            << '\n';

        return 1;
    }
}