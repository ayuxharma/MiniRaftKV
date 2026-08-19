#include "miniraft/raft_core.hpp"

// Provides exceptions for invalid input and invalid program state.
#include <stdexcept>

// Provides move(), which helps avoid unnecessary copies.
#include <utility>

namespace miniraft {

// Import only the standard-library names used in this source file.
using std::invalid_argument;
using std::logic_error;
using std::move;

string_view to_string(const NodeRole role) {
    // Convert the enum value into readable text.
    switch (role) {
        case NodeRole::follower:
            return "follower";

        case NodeRole::candidate:
            return "candidate";

        case NodeRole::leader:
            return "leader";
    }

    // Every valid NodeRole is handled above.
    // Reaching this line means the role value is invalid.
    throw logic_error{
        "Unknown Raft node role"
    };
}

RaftCore::RaftCore(
    string node_id,
    vector<string> cluster_members,
    const uint64_t initial_term,
    vector<LogEntry> initial_log
)
    // Move large values into the object instead of copying them.
    //
    // Members are initialized in the order in which they are
    // declared in raft_core.hpp.
    : node_id_{move(node_id)},
      log_entries_{move(initial_log)},
      current_term_{initial_term} {

    // A node must always have a non-empty identity.
    if (node_id_.empty()) {
        throw invalid_argument{
            "Node ID cannot be empty"
        };
    }

    // This educational implementation accepts odd-sized clusters.
    //
    // Examples:
    // 1 node  -> majority 1
    // 3 nodes -> majority 2
    // 5 nodes -> majority 3
    if (
        cluster_members.empty() ||
        cluster_members.size() % 2 == 0
    ) {
        throw invalid_argument{
            "Cluster must contain a non-zero odd number of nodes"
        };
    }

    // Validate and copy each configured cluster member.
    for (const string& member_id : cluster_members) {
        // Every cluster member must have a usable identity.
        if (member_id.empty()) {
            throw invalid_argument{
                "Cluster member ID cannot be empty"
            };
        }

        // insert() returns:
        //
        // 1. An iterator pointing to the stored value.
        // 2. A Boolean telling us whether insertion succeeded.
        const auto [iterator, was_inserted] =
            cluster_members_.insert(member_id);

        // We only need was_inserted.
        // Explicitly mark the iterator as unused.
        static_cast<void>(iterator);

        // A failed insertion means the ID already existed.
        if (!was_inserted) {
            throw invalid_argument{
                "Cluster member IDs must be unique"
            };
        }
    }

    // The node must appear in its own cluster configuration.
    //
    // find() returns end() when the requested value is missing.
    if (
        cluster_members_.find(node_id_) ==
        cluster_members_.end()
    ) {
        throw invalid_argument{
            "Cluster membership must contain this node's ID"
        };
    }

    // Track the previous log-entry term while validating the log.
    uint64_t previous_entry_term = 0;

    // Validate every recovered or supplied log entry.
    for (const LogEntry& entry : log_entries_) {
        // Term zero represents the empty-log sentinel.
        // Real log entries must belong to term one or later.
        if (entry.term == 0) {
            throw invalid_argument{
                "A log entry must have a non-zero term"
            };
        }

        // For now, every log entry must contain a command.
        //
        // Later, we may represent leader no-op entries using
        // an explicit command type instead of an empty string.
        if (entry.command.empty()) {
            throw invalid_argument{
                "A log entry command cannot be empty"
            };
        }

        // Terms cannot move backward as log indexes increase.
        //
        // Valid:
        // 1, 1, 2, 2, 3
        //
        // Invalid:
        // 1, 3, 2
        if (entry.term < previous_entry_term) {
            throw invalid_argument{
                "Log entry terms cannot decrease"
            };
        }

        // A node cannot contain an entry created in a future term.
        if (entry.term > current_term_) {
            throw invalid_argument{
                "Log entry term cannot exceed the node's current term"
            };
        }

        // Store this term for comparison with the next entry.
        previous_entry_term = entry.term;
    }
}

const string& RaftCore::node_id() const {
    // Return a read-only reference to avoid copying the node ID.
    return node_id_;
}

NodeRole RaftCore::role() const {
    // Return the node's current Raft role.
    return role_;
}

uint64_t RaftCore::current_term() const {
    // Return the node's current election term.
    return current_term_;
}

const Optional<string>& RaftCore::voted_for() const {
    // The Optional is empty when the node has not voted.
    return voted_for_;
}

size_t RaftCore::votes_received() const {
    // The set size equals the number of unique votes received.
    return votes_received_.size();
}

size_t RaftCore::cluster_size() const {
    // Return the number of configured cluster members.
    return cluster_members_.size();
}

const vector<LogEntry>& RaftCore::log_entries() const {
    // Return a read-only reference to avoid copying the log.
    return log_entries_;
}

uint64_t RaftCore::last_log_index() const {
    // Our logical Raft indexes begin at one.
    //
    // Because a vector's size represents how many entries it has,
    // the size is also the logical index of the final entry.
    //
    // Empty log:
    // size = 0
    // last index = 0
    //
    // Three entries:
    // size = 3
    // last index = 3
    return static_cast<uint64_t>(log_entries_.size());
}

uint64_t RaftCore::last_log_term() const {
    // An empty log has the special last term zero.
    if (log_entries_.empty()) {
        return 0;
    }

    // back() returns the final entry in the vector.
    return log_entries_.back().term;
}

size_t RaftCore::majority_size() const {
    // A majority means more than half of the cluster.
    //
    // Integer division intentionally discards fractions.
    //
    // Three nodes:
    // 3 / 2 + 1 = 2
    //
    // Five nodes:
    // 5 / 2 + 1 = 3
    return cluster_size() / 2 + 1;
}

void RaftCore::start_election() {
    // Every new election must start in a new term.
    ++current_term_;

    // The node changes from follower to candidate.
    role_ = NodeRole::candidate;

    // Votes from a previous election cannot be reused.
    votes_received_.clear();

    // A candidate always votes for itself.
    voted_for_ = node_id_;

    // Store the self-vote in the set of received votes.
    votes_received_.insert(node_id_);

    // A one-node cluster already has a majority after self-voting.
    if (votes_received() >= majority_size()) {
        role_ = NodeRole::leader;
    }
}

RequestVoteRequest
RaftCore::make_request_vote_request() const {
    // Followers and leaders must not ask for election votes.
    if (role_ != NodeRole::candidate) {
        throw logic_error{
            "Only a candidate can create a vote request"
        };
    }

    // Create the outgoing request with safe default values.
    RequestVoteRequest request;

    // Include the candidate's current election term.
    request.term = current_term_;

    // Include the candidate's unique identity.
    request.candidate_id = node_id_;

    // Advertise the candidate's final log position.
    //
    // Followers use these values to decide whether the candidate's
    // log is at least as up to date as their own log.
    request.last_log_index = last_log_index();
    request.last_log_term = last_log_term();

    return request;
}

bool RaftCore::candidate_log_is_up_to_date(
    const uint64_t candidate_last_log_index,
    const uint64_t candidate_last_log_term
) const {
    // Raft compares the last log terms before comparing indexes.
    //
    // A candidate with a newer last term is considered newer,
    // even if it has fewer total entries.
    if (candidate_last_log_term > last_log_term()) {
        return true;
    }

    // A candidate with an older last term is outdated,
    // even if it has more total entries.
    if (candidate_last_log_term < last_log_term()) {
        return false;
    }

    // At this point, both final log terms are equal.
    //
    // The candidate must therefore have at least as many entries
    // as this node.
    return candidate_last_log_index >= last_log_index();
}

RequestVoteResponse
RaftCore::handle_request_vote(
    const RequestVoteRequest& request
) {
    // Start with a rejected response.
    //
    // The vote becomes granted only after every rule succeeds.
    RequestVoteResponse response;

    // Always report our current term.
    response.term = current_term_;

    // The safe default is to reject the request.
    response.vote_granted = false;

    // Reject candidates that are not configured cluster members.
    //
    // Network input should be rejected safely instead of causing
    // the process to throw an exception and terminate.
    if (
        cluster_members_.find(request.candidate_id) ==
        cluster_members_.end()
    ) {
        return response;
    }

    // Reject requests from older election terms.
    //
    // The response contains our newer term, allowing the outdated
    // candidate to update itself and return to follower state.
    if (request.term < current_term_) {
        return response;
    }

    // A request from a newer term means our state is outdated.
    //
    // We must adopt the newer term and become a follower before
    // deciding whether to grant the vote.
    if (request.term > current_term_) {
        become_follower(request.term);
    }

    // become_follower() may have changed our current term.
    // Update the response before continuing.
    response.term = current_term_;

    // The node may vote if it has not voted in this term.
    const bool has_not_voted =
        !voted_for_.has_value();

    // Repeated requests from the same candidate should be safe.
    //
    // A follower may return the same granted vote again if the
    // original network response was lost.
    const bool already_voted_for_candidate =
        voted_for_.has_value() &&
        voted_for_.value() == request.candidate_id;

    // A vote is available when either condition is true.
    const bool can_vote =
        has_not_voted ||
        already_voted_for_candidate;

    // Reject the request if another candidate already received
    // this node's vote during the current term.
    if (!can_vote) {
        return response;
    }

    // Compare the candidate's log with our local log.
    const bool log_is_up_to_date =
        candidate_log_is_up_to_date(
            request.last_log_index,
            request.last_log_term
        );

    // Reject candidates whose logs are older than our log.
    //
    // This helps prevent committed entries from disappearing
    // after a leadership change.
    if (!log_is_up_to_date) {
        return response;
    }

    // All vote-granting conditions passed.
    // Record the selected candidate.
    voted_for_ = request.candidate_id;

    // Mark the response as successful.
    response.vote_granted = true;

    return response;
}

void RaftCore::receive_vote(
    const string& voter_id,
    const RequestVoteResponse& response
) {
    // A vote response must come from a configured cluster member.
    if (
        cluster_members_.find(voter_id) ==
        cluster_members_.end()
    ) {
        throw invalid_argument{
            "Received a vote from an unknown cluster member"
        };
    }

    // A response from a newer term means this candidate is outdated.
    //
    // It must stop campaigning and return to follower state.
    if (response.term > current_term_) {
        become_follower(response.term);
        return;
    }

    // Ignore responses belonging to an older election.
    if (response.term < current_term_) {
        return;
    }

    // Followers and leaders do not collect election votes.
    if (role_ != NodeRole::candidate) {
        return;
    }

    // Rejected votes do not contribute to the majority.
    if (!response.vote_granted) {
        return;
    }

    // Store the voter's ID.
    //
    // unordered_set prevents duplicate responses from being
    // counted more than once.
    votes_received_.insert(voter_id);

    // Become leader after receiving votes from a majority
    // of the configured cluster.
    if (votes_received() >= majority_size()) {
        role_ = NodeRole::leader;
    }
}

void RaftCore::become_follower(
    const uint64_t new_term
) {
    // This helper is only valid when a genuinely newer term
    // has been discovered.
    if (new_term <= current_term_) {
        throw logic_error{
            "Cannot become follower using an old or equal term"
        };
    }

    // Adopt the newer election term.
    current_term_ = new_term;

    // Stop acting as a candidate or leader.
    role_ = NodeRole::follower;

    // This node has not voted in the newly discovered term.
    voted_for_.reset();

    // Votes collected during an older election are no longer valid.
    votes_received_.clear();
}

}  // namespace miniraft