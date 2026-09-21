#include "raft.grpc.pb.h"

#include <grpcpp/grpcpp.h>

#include <chrono>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

namespace rpc = miniraft::rpc;

using grpc::ClientContext;
using grpc::CreateChannel;
using grpc::InsecureChannelCredentials;
using grpc::Status;
using grpc::StatusCode;
using std::cerr;
using std::chrono::milliseconds;
using std::chrono::system_clock;
using std::cout;
using std::string;
using std::unique_ptr;
using std::vector;

namespace {

// Keep this client focused on the local three-node demo.
vector<string> cluster_addresses() {
    return {
        "127.0.0.1:50051",
        "127.0.0.1:50052",
        "127.0.0.1:50053"
    };
}

// Prevent an unavailable node from blocking the client.
void set_deadline(
    ClientContext& context
) {
    context.set_deadline(
        system_clock::now() +
        milliseconds{500}
    );
}

// Create a gRPC client for one node.
unique_ptr<rpc::RaftService::Stub> make_stub(
    const string& address
) {
    return rpc::RaftService::NewStub(
        CreateChannel(
            address,
            InsecureChannelCredentials()
        )
    );
}

// Followers and unavailable nodes are skipped while the client searches for the current leader.
bool should_try_next_node(
    const Status& status
) {
    return
        status.error_code() ==
            StatusCode::FAILED_PRECONDITION ||
        status.error_code() ==
            StatusCode::UNAVAILABLE ||
        status.error_code() ==
            StatusCode::DEADLINE_EXCEEDED;
}

int run_set(
    const string& key,
    const string& value
) {
    for (const string& address : cluster_addresses()) {
        unique_ptr<rpc::RaftService::Stub> stub =
            make_stub(address);

        ClientContext context;
        set_deadline(context);

        rpc::SetRequest request;
        request.set_key(key);
        request.set_value(value);

        rpc::SetResponse response;

        const Status status =
            stub->Set(
                &context,
                request,
                &response
            );

        if (status.ok()) {
            cout
                << "SET accepted by leader "
                << address
                << " at log index "
                << response.log_index()
                << '\n';

            return 0;
        }

        if (!should_try_next_node(status)) {
            cerr
                << "SET failed: "
                << status.error_message()
                << '\n';

            return 1;
        }
    }

    cerr
        << "SET failed: no reachable leader was found\n";

    return 1;
}

int run_get(
    const string& key
) {
    for (const string& address : cluster_addresses()) {
        unique_ptr<rpc::RaftService::Stub> stub =
            make_stub(address);

        ClientContext context;
        set_deadline(context);

        rpc::GetRequest request;
        request.set_key(key);

        rpc::GetResponse response;

        const Status status =
            stub->Get(
                &context,
                request,
                &response
            );

        if (status.ok()) {
            if (response.found()) {
                cout
                    << key
                    << " = "
                    << response.value()
                    << '\n';
            } else {
                cout
                    << key
                    << " was not found\n";
            }

            return 0;
        }

        if (!should_try_next_node(status)) {
            cerr
                << "GET failed: "
                << status.error_message()
                << '\n';

            return 1;
        }
    }

    cerr
        << "GET failed: no reachable leader was found\n";

    return 1;
}

int run_delete(
    const string& key
) {
    for (const string& address : cluster_addresses()) {
        unique_ptr<rpc::RaftService::Stub> stub =
            make_stub(address);

        ClientContext context;
        set_deadline(context);

        rpc::DeleteRequest request;
        request.set_key(key);

        rpc::DeleteResponse response;

        const Status status =
            stub->Delete(
                &context,
                request,
                &response
            );

        if (status.ok()) {
            cout
                << "DELETE accepted by leader "
                << address
                << " at log index "
                << response.log_index()
                << '\n';

            return 0;
        }

        if (!should_try_next_node(status)) {
            cerr
                << "DELETE failed: "
                << status.error_message()
                << '\n';

            return 1;
        }
    }

    cerr
        << "DELETE failed: no reachable leader was found\n";

    return 1;
}

void print_usage() {
    cerr
        << "Usage:\n"
        << "  miniraft_client set <key> <value>\n"
        << "  miniraft_client get <key>\n"
        << "  miniraft_client delete <key>\n";
}

}  // namespace

int main(int argc, char* argv[]) {
    if (argc < 2) {
        print_usage();
        return 1;
    }

    const string operation{
        argv[1]
    };

    if (operation == "set" && argc == 4) {
        return run_set(
            argv[2],
            argv[3]
        );
    }

    if (operation == "get" && argc == 3) {
        return run_get(
            argv[2]
        );
    }

    if (operation == "delete" && argc == 3) {
        return run_delete(
            argv[2]
        );
    }

    print_usage();
    return 1;
}