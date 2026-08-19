#pragma once

// Standard library types used by the Raft core.
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

namespace miniraft {

// Import only the standard library names used in this header.
using std::size_t;
using std::string;
using std::string_view;
using std::uint64_t;
using std::unordered_set;
using std::vector;

// Create a readable alias for values that may be empty.
template <typename ValueType>
using Optional = std::optional<ValueType>;

// Every Raft node must be in exactly one of these roles.
enum class NodeRole {
    follower,
    candidate,
    leader
};

// Convert a role into readable text for logs and terminal output.
string_view to_string(NodeRole role);

// One command stored in the replicated Raft log.
struct LogEntry {
    // Term in which the leader created this entry.
    uint64_t term{0};

    // Command that will eventually be applied to the state machine.
    //
    // Examples:
    // "PUT x 10"
    // "DELETE x"
    string command;
};

// Message sent by a candidate when requesting a vote.
struct RequestVoteRequest {
    // Election term of the candidate.
    uint64_t term{0};

    // Unique ID of the candidate.
    string candidate_id;

    // Logical index of the candidate's final log entry.
    uint64_t last_log_index{0};

    // Term of the candidate's final log entry.
    uint64_t last_log_term{0};
};

// Message returned after processing a vote request.
struct RequestVoteResponse {
    // Current term of the node sending this response.
    uint64_t term{0};

    // True when the vote was granted.
    bool vote_granted{false};
};

class RaftCore {
public:
    // Create a Raft node.
    //
    // initial_term and initial_log allow tests to construct nodes
    // with different log histories.
    //
    // Later, persistent storage will provide these values when
    // recovering a node after a restart.
    RaftCore(
        string node_id,
        vector<string> cluster_members,
        uint64_t initial_term = 0,
        vector<LogEntry> initial_log = {}
    );

    // Read-only accessors for the node's current state.
    [[nodiscard]] const string& node_id() const;
    [[nodiscard]] NodeRole role() const;
    [[nodiscard]] uint64_t current_term() const;
    [[nodiscard]] const Optional<string>& voted_for() const;
    [[nodiscard]] size_t votes_received() const;
    [[nodiscard]] size_t cluster_size() const;

    // Return all local log entries.
    //
    // The const reference prevents callers from changing the log.
    [[nodiscard]]
    const vector<LogEntry>& log_entries() const;

    // Return the logical index of the final local log entry.
    //
    // Empty log: index 0
    // One entry: index 1
    // Three entries: index 3
    [[nodiscard]] uint64_t last_log_index() const;

    // Return the term belonging to the final local log entry.
    //
    // An empty log has last term zero.
    [[nodiscard]] uint64_t last_log_term() const;

    // Start a new election.
    void start_election();

    // Create a RequestVote message for this candidate.
    [[nodiscard]]
    RequestVoteRequest make_request_vote_request() const;

    // Process a vote request received from another candidate.
    [[nodiscard]]
    RequestVoteResponse handle_request_vote(
        const RequestVoteRequest& request
    );

    // Process a vote response received by this candidate.
    void receive_vote(
        const string& voter_id,
        const RequestVoteResponse& response
    );

private:
    // Become a follower after discovering a newer term.
    void become_follower(uint64_t new_term);

    // Calculate the number of votes required to win.
    [[nodiscard]] size_t majority_size() const;

    // Compare a candidate's final log position with our final log position.
    [[nodiscard]]
    bool candidate_log_is_up_to_date(
        uint64_t candidate_last_log_index,
        uint64_t candidate_last_log_term
    ) const;

    // Unique ID of this node.
    string node_id_;

    // IDs of all valid cluster members.
    unordered_set<string> cluster_members_;

    // Ordered log entries stored by this node.
    vector<LogEntry> log_entries_;

    // Current role of this node.
    NodeRole role_{NodeRole::follower};

    // Current logical election term.
    uint64_t current_term_{0};

    // Candidate selected by this node in the current term.
    Optional<string> voted_for_;

    // Unique votes collected while acting as a candidate.
    unordered_set<string> votes_received_;
};

}  // namespace miniraft