#include "miniraft/raft_core.hpp"
#include "miniraft/raft_service.hpp"

#include <grpcpp/grpcpp.h>

#include <exception>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

using miniraft::RaftCore;
using miniraft::RaftServiceImpl;
using miniraft::to_string;

using grpc::InsecureServerCredentials;
using grpc::Server;
using grpc::ServerBuilder;
using std::cerr;
using std::cout;
using std::exception;
using std::runtime_error;
using std::string;
using std::unique_ptr;
using std::vector;

int main(int argc, char* argv[]) {
    // Example:
    // ./miniraft_node node-1 0.0.0.0:50051 data/node-1.state
    if (argc != 4) {
        cerr
            << "Usage: miniraft_node "
            << "<node-id> <listen-address> <storage-path>\n";

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

        // This project intentionally uses a fixed three-node cluster.
        const vector<string> cluster_members{
            "node-1",
            "node-2",
            "node-3"
        };

        RaftCore node{
            node_id,
            cluster_members,
            0,
            {},
            150,
            300,
            1,
            storage_path
        };

        // Connect incoming gRPC calls to this Raft node.
        RaftServiceImpl service{
            node
        };

        ServerBuilder builder;

        // Insecure credentials are sufficient for local development
        // and our internal learning Kubernetes cluster.
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

        cout
            << node.node_id()
            << " started as "
            << to_string(node.role())
            << " in term "
            << node.current_term()
            << " and is listening on "
            << listen_address
            << '\n';

        // Keep the process alive while gRPC handles requests.
        server->Wait();

        return 0;
    } catch (const exception& error) {
        cerr
            << "Failed to start node: "
            << error.what()
            << '\n';

        return 1;
    }
}