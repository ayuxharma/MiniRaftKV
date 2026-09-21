#pragma once

#include "miniraft/raft_peer_client.hpp"
#include "miniraft/raft_service.hpp"

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace miniraft {

using std::string;
using std::uint64_t;
using std::unique_ptr;
using std::unordered_map;
using std::vector;

// Identifies one remote node and its gRPC address.
struct PeerConfiguration {
    string node_id;
    string address;
};

// Drives timers and delivers Raft's queued network actions.
class RaftRuntime {
public:
    RaftRuntime(
        RaftServiceImpl& service,
        const vector<PeerConfiguration>& peers
    );

    // Advance the node and perform one round of network work.
    void run_once(uint64_t elapsed_ms);

private:
    [[nodiscard]]
    RaftPeerClient* find_peer(
        const string& node_id
    );

    void deliver_vote_requests();
    void deliver_append_entries();

    RaftServiceImpl& service_;

    // One outbound gRPC client for each remote node.
    unordered_map<
        string,
        unique_ptr<RaftPeerClient>
    > peers_;

    uint64_t heartbeat_elapsed_ms_{0};
};

}  // namespace miniraft