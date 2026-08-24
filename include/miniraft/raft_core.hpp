#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <random>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

namespace miniraft {

// Import only commonly used standard-library type names.
using std::mt19937_64;
using std::size_t;
using std::string;
using std::string_view;
using std::uint64_t;
using std::unordered_set;
using std::vector;

// Readable alias for a value that may be absent.
template <typename ValueType>
using Optional = std::optional<ValueType>;

// Every Raft node is in exactly one of these roles.
enum class NodeRole {
    follower,
    candidate,
    leader
};

// Convert a role into readable text.
string_view to_string(NodeRole role);

// One command stored in the Raft log.
struct LogEntry {
    // Term in which the entry was created.
    uint64_t term{0};

    // Command that will eventually be applied.
    string command;
};

// Request sent by a candidate to obtain a vote.
struct RequestVoteRequest {
    // Candidate's current election term.
    uint64_t term{0};

    // Candidate requesting the vote.
    string candidate_id;

    // Candidate's final logical log index.
    uint64_t last_log_index{0};

    // Term of the candidate's final log entry.
    uint64_t last_log_term{0};
};

// Response returned by a node after processing a vote request.
struct RequestVoteResponse {
    // Responder's current term.
    uint64_t term{0};

    // True when the vote was granted.
    bool vote_granted{false};
};

// Instruction to send one vote request to one target node.
struct RequestVoteAction {
    // Node that should receive the request.
    string target_node_id;

    // Request that should be delivered.
    RequestVoteRequest request;
};

struct AppendEntriesRequest {

    uint64_t term {0} ; // leader's current term

    string leader_id ; // node that believes it is the leader

    uint64_t prev_log_index {0} ; // log position immediately before any new entries

    uint64_t prev_log_term {0} ; // term stored at prev_log_index
} ;

struct AppendEntriesResponse
{
    uint64_t term {0} ; // responder's current term

    bool success {false} ; // true when the leader's prev log position matches
};

struct AppendEntriesAction {
    string target_node_id ;

    AppendEntriesRequest request ;
};


class RaftCore {
public:
    // Construct a node with static cluster membership.
    RaftCore(
        string node_id,
        vector<string> cluster_members,
        uint64_t initial_term = 0,
        vector<LogEntry> initial_log = {},
        uint64_t min_election_timeout_ms = 150,
        uint64_t max_election_timeout_ms = 300,
        uint64_t random_seed = 1
    );

    // Basic node-state accessors.
    [[nodiscard]] const string& node_id() const;
    [[nodiscard]] NodeRole role() const;
    [[nodiscard]] uint64_t current_term() const;
    [[nodiscard]] const Optional<string>& voted_for() const;

    // Return the currently recognized leader, if one is known.
    [[nodiscard]] const Optional<string>& leader_id() const;

    [[nodiscard]] size_t votes_received() const;
    [[nodiscard]] size_t cluster_size() const;

    // Log accessors.
    [[nodiscard]]
    const vector<LogEntry>& log_entries() const;

    [[nodiscard]] uint64_t last_log_index() const;
    [[nodiscard]] uint64_t last_log_term() const;

    // Election-timer accessors.
    [[nodiscard]] uint64_t current_time_ms() const;
    [[nodiscard]] uint64_t min_election_timeout_ms() const;
    [[nodiscard]] uint64_t max_election_timeout_ms() const;
    [[nodiscard]] uint64_t election_timeout_ms() const;
    [[nodiscard]] uint64_t election_deadline_ms() const;
    [[nodiscard]] bool election_timeout_expired() const;
    [[nodiscard]] bool tick(uint64_t elapsed_ms); // // Advance logical time and start an election when timed out.

    // Advance logical time without automatically processing timeout.
    void advance_time(uint64_t elapsed_ms);

    // Start an election immediately.
    void start_election();

    // Build the vote request for the current election.
    [[nodiscard]]
    RequestVoteRequest make_request_vote_request() const;

    // Process an incoming vote request.
    [[nodiscard]]
    RequestVoteResponse handle_request_vote(
        const RequestVoteRequest& request
    );

    // Process a vote response received by this candidate.
    void receive_vote(
        const string& voter_id,
        const RequestVoteResponse& response
    );

    // Create an empty AppendEntries request.
    //
    // An empty AppendEntries request acts as a heartbeat.
    [[nodiscard]]
    AppendEntriesRequest make_heartbeat_request() const;

    // Process a heartbeat received from a leader.
    [[nodiscard]]
    AppendEntriesResponse handle_append_entries(
        const AppendEntriesRequest& request
    );

    // Create one heartbeat action for every other cluster member.
void queue_heartbeat_actions();

// Return the number of heartbeats waiting for delivery.
[[nodiscard]]
size_t pending_append_entries_count() const;

// Return all pending heartbeats and empty the queue.
[[nodiscard]]
vector<AppendEntriesAction> take_append_entries_actions();

// Process a response returned by a follower.
void receive_append_entries_response(
    const string& follower_id,
    const AppendEntriesResponse& response
);

    // Inspect the number of outbound vote requests.
    [[nodiscard]]
    size_t pending_request_vote_count() const;

    // Return all outbound vote requests and empty the queue.
    [[nodiscard]]
    vector<RequestVoteAction> take_request_vote_actions();

private:
    // Change to follower after discovering a newer term.
    void become_follower(uint64_t new_term);

    // Select a timeout and calculate a fresh deadline.
    void reset_election_deadline();

    // Create one vote-request action for every other node.
    void queue_request_vote_actions();

    // Calculate the number of votes required to win.
    [[nodiscard]] size_t majority_size() const;

    // Determine whether a candidate's log is sufficiently recent.
    [[nodiscard]]
    bool candidate_log_is_up_to_date(
        uint64_t candidate_last_log_index,
        uint64_t candidate_last_log_term
    ) const;

    // Check whether the local log contains the requested
    // previous index and term.
    [[nodiscard]]
    bool previous_log_position_matches(
        uint64_t previous_log_index,
        uint64_t previous_log_term
    ) const;

    // Unique ID of this node.
    string node_id_;

    // Complete static cluster membership.
    unordered_set<string> cluster_members_;

    // Ordered local Raft log.
    vector<LogEntry> log_entries_;

    // Current node role.
    NodeRole role_{NodeRole::follower};

    // Latest term known by this node.
    uint64_t current_term_{0};

    // Candidate selected in the current term.
    Optional<string> voted_for_;

    // Leader currently recognized by this node.
    // It is empty when no valid leader is currently known.
    Optional<string> leader_id_;

    // Unique votes collected during the current election.
    unordered_set<string> votes_received_;

    // Current deterministic logical time.
    uint64_t current_time_ms_{0};

    // Randomized timeout boundaries.
    uint64_t min_election_timeout_ms_{150};
    uint64_t max_election_timeout_ms_{300};

    // Timeout and deadline selected for the current interval.
    uint64_t election_timeout_ms_{0};
    uint64_t election_deadline_ms_{0};

    // Deterministic random engine used by tests and simulations.
    mt19937_64 random_engine_;

    // Vote requests waiting for delivery.
    vector<RequestVoteAction> pending_request_vote_actions_;

    // AppendEntries heartbeats waiting for delivery.
    vector<AppendEntriesAction> pending_append_entries_actions_;
};

}  // namespace miniraft