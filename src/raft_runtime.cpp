#include "miniraft/raft_runtime.hpp"

#include <memory>
#include <stdexcept>

namespace miniraft {

using std::invalid_argument;
using std::make_unique;

namespace {

// Heartbeats must be more frequent than election timeouts.
constexpr uint64_t heartbeat_interval_ms = 50;

}  // namespace

RaftRuntime::RaftRuntime(
    RaftServiceImpl& service,
    const vector<PeerConfiguration>& peers
)
    : service_{service} {
    for (const PeerConfiguration& peer : peers) {
        if (peer.node_id.empty()) {
            throw invalid_argument{
                "Peer node ID cannot be empty"
            };
        }

        const auto [iterator, inserted] =
            peers_.emplace(
                peer.node_id,
                make_unique<RaftPeerClient>(
                    peer.address
                )
            );

        static_cast<void>(iterator);

        if (!inserted) {
            throw invalid_argument{
                "Peer node IDs must be unique"
            };
        }
    }
}

RaftPeerClient* RaftRuntime::find_peer(
    const string& node_id
) {
    const auto iterator =
        peers_.find(node_id);

    if (iterator == peers_.end()) {
        return nullptr;
    }

    return iterator->second.get();
}

void RaftRuntime::deliver_vote_requests() {
    // Taking the actions also removes them from the local queue.
    vector<RequestVoteAction> actions =
        service_.take_request_vote_actions();

    for (const RequestVoteAction& action : actions) {
        RaftPeerClient* peer =
            find_peer(
                action.target_node_id
            );

        if (peer == nullptr) {
            continue;
        }

        // No Raft lock is held while waiting on the network.
        const Optional<RequestVoteResponse> response =
            peer->request_vote(
                action.request
            );

        if (response.has_value()) {
            service_.receive_vote(
                action.target_node_id,
                response.value()
            );
        }
    }
}

void RaftRuntime::deliver_append_entries() {
    vector<AppendEntriesAction> actions =
        service_.take_append_entries_actions();

    for (const AppendEntriesAction& action : actions) {
        RaftPeerClient* peer =
            find_peer(
                action.target_node_id
            );

        if (peer == nullptr) {
            continue;
        }

        const Optional<AppendEntriesResponse> response =
            peer->append_entries(
                action.request
            );

        if (response.has_value()) {
            service_.receive_append_entries_response(
                action.target_node_id,
                response.value()
            );
        }
    }
}

void RaftRuntime::run_once(
    const uint64_t elapsed_ms
) {
    // tick() starts an election when the timeout expires.
    static_cast<void>(
        service_.tick(elapsed_ms)
    );

    // Deliver any vote requests created by the election.
    deliver_vote_requests();

    if (service_.current_role() == NodeRole::leader) {
        heartbeat_elapsed_ms_ += elapsed_ms;

        if (
            heartbeat_elapsed_ms_ >=
            heartbeat_interval_ms
        ) {
            service_.queue_heartbeats_if_leader();
            heartbeat_elapsed_ms_ = 0;
        }
    } else {
        heartbeat_elapsed_ms_ = 0;
    }

    // This delivers heartbeats and newly appended log entries.
    deliver_append_entries();
}

}  // namespace miniraft 