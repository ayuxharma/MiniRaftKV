#include "miniraft/raft_peer_client.hpp"

#include "miniraft/rpc_conversion.hpp"

#include <grpcpp/grpcpp.h>

#include <chrono>
#include <stdexcept>

namespace miniraft {

using grpc::ClientContext;
using grpc::CreateChannel;
using grpc::InsecureChannelCredentials;
using grpc::Status;
using std::chrono::milliseconds;
using std::chrono::system_clock;
using std::invalid_argument;

namespace {

// Prevent an unavailable peer from blocking this node indefinitely.
void set_rpc_deadline(
    ClientContext& context
) {
    context.set_deadline(
        system_clock::now() +
        milliseconds{500}
    );
}

}  // namespace

RaftPeerClient::RaftPeerClient(
    const string& address
) {
    if (address.empty()) {
        throw invalid_argument{
            "Peer address cannot be empty"
        };
    }

    // A channel represents the connection to one remote node.
    stub_ = rpc::RaftService::NewStub(
        CreateChannel(
            address,
            InsecureChannelCredentials()
        )
    );
}

Optional<RequestVoteResponse>
RaftPeerClient::request_vote(
    const RequestVoteRequest& request
) {
    // Every RPC needs its own context.
    ClientContext context;
    set_rpc_deadline(context);

    const rpc::RequestVoteRequest rpc_request =
        to_rpc(request);

    rpc::RequestVoteResponse rpc_response;

    const Status status =
        stub_->RequestVote(
            &context,
            rpc_request,
            &rpc_response
        );

    if (!status.ok()) {
        // An unreachable peer is represented by an empty Optional.
        return {};
    }

    return from_rpc(rpc_response);
}

Optional<AppendEntriesResponse>
RaftPeerClient::append_entries(
    const AppendEntriesRequest& request
) {
    ClientContext context;
    set_rpc_deadline(context);

    const rpc::AppendEntriesRequest rpc_request =
        to_rpc(request);

    rpc::AppendEntriesResponse rpc_response;

    const Status status =
        stub_->AppendEntries(
            &context,
            rpc_request,
            &rpc_response
        );

    if (!status.ok()) {
        return {};
    }

    return from_rpc(rpc_response);
}

}  // namespace miniraft