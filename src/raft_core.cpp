#include "miniraft/raft_core.hpp"

// Provides the random distribution used for election timeouts.
#include <random>

// Provides exceptions for invalid input and state.
#include <stdexcept>

// Provides move(), which avoids unnecessary copies.
#include <utility>

namespace miniraft {

// Import only the standard-library names used in this file.
using std::invalid_argument;
using std::logic_error;
using std::move;
using std::uniform_int_distribution;

string_view to_string(const NodeRole role) {
    switch (role) {
        case NodeRole::follower:
            return "follower";

        case NodeRole::candidate:
            return "candidate";

        case NodeRole::leader:
            return "leader";
    }

    throw logic_error{
        "Unknown Raft node role"
    };
}

RaftCore::RaftCore(
    string node_id,
    vector<string> cluster_members,
    const uint64_t initial_term,
    vector<LogEntry> initial_log,
    const uint64_t min_election_timeout_ms,
    const uint64_t max_election_timeout_ms,
    const uint64_t random_seed
)
    : node_id_{move(node_id)},
      log_entries_{move(initial_log)},
      current_term_{initial_term},
      min_election_timeout_ms_{min_election_timeout_ms},
      max_election_timeout_ms_{max_election_timeout_ms},
      random_engine_{random_seed} {

    // Every node must have a non-empty identity.
    if (node_id_.empty()) {
        throw invalid_argument{
            "Node ID cannot be empty"
        };
    }

    // A zero minimum would allow an immediate timeout.
    if (min_election_timeout_ms_ == 0) {
        throw invalid_argument{
            "Minimum election timeout must be greater than zero"
        };
    }

    // The maximum boundary must not be below the minimum.
    if (max_election_timeout_ms_ < min_election_timeout_ms_) {
        throw invalid_argument{
            "Maximum election timeout cannot be less than minimum"
        };
    }

    // This project currently supports odd-sized clusters.
    if (
        cluster_members.empty() ||
        cluster_members.size() % 2 == 0
    ) {
        throw invalid_argument{
            "Cluster must contain a non-zero odd number of nodes"
        };
    }

    // Validate and store every cluster member.
    for (const string& member_id : cluster_members) {
        if (member_id.empty()) {
            throw invalid_argument{
                "Cluster member ID cannot be empty"
            };
        }

        const auto [iterator, was_inserted] =
            cluster_members_.insert(member_id);

        // We do not need the returned iterator.
        static_cast<void>(iterator);

        if (!was_inserted) {
            throw invalid_argument{
                "Cluster member IDs must be unique"
            };
        }
    }

    // The node must be present in its own cluster configuration.
    if (
        cluster_members_.find(node_id_) ==
        cluster_members_.end()
    ) {
        throw invalid_argument{
            "Cluster membership must contain this node's ID"
        };
    }

    // Validate terms in the supplied or recovered log.
    uint64_t previous_entry_term = 0;

    for (const LogEntry& entry : log_entries_) {
        // Term zero is reserved for the empty-log sentinel.
        if (entry.term == 0) {
            throw invalid_argument{
                "A log entry must have a non-zero term"
            };
        }

        // Every entry currently requires a readable command.
        if (entry.command.empty()) {
            throw invalid_argument{
                "A log entry command cannot be empty"
            };
        }

        // Log terms cannot decrease as indexes increase.
        if (entry.term < previous_entry_term) {
            throw invalid_argument{
                "Log entry terms cannot decrease"
            };
        }

        // A node cannot contain an entry from a future term.
        if (entry.term > current_term_) {
            throw invalid_argument{
                "Log entry term cannot exceed the node's current term"
            };
        }

        previous_entry_term = entry.term;
    }

    // Select the first randomized election timeout.
    //
    // This must happen after validating the timeout range.
    reset_election_deadline();
}

const string& RaftCore::node_id() const {
    return node_id_;
}

NodeRole RaftCore::role() const {
    return role_;
}

uint64_t RaftCore::current_term() const {
    return current_term_;
}

const Optional<string>& RaftCore::voted_for() const {
    return voted_for_;
}

size_t RaftCore::votes_received() const {
    return votes_received_.size();
}

size_t RaftCore::cluster_size() const {
    return cluster_members_.size();
}

const vector<LogEntry>& RaftCore::log_entries() const {
    return log_entries_;
}

uint64_t RaftCore::last_log_index() const {
    // The vector size equals the logical index of the final entry.
    return static_cast<uint64_t>(log_entries_.size());
}

uint64_t RaftCore::last_log_term() const {
    // An empty log has the special last term zero.
    if (log_entries_.empty()) {
        return 0;
    }

    return log_entries_.back().term;
}

uint64_t RaftCore::current_time_ms() const {
    return current_time_ms_;
}

uint64_t RaftCore::min_election_timeout_ms() const {
    return min_election_timeout_ms_;
}

uint64_t RaftCore::max_election_timeout_ms() const {
    return max_election_timeout_ms_;
}

uint64_t RaftCore::election_timeout_ms() const {
    // This is the timeout selected for the current interval.
    return election_timeout_ms_;
}

uint64_t RaftCore::election_deadline_ms() const {
    return election_deadline_ms_;
}

bool RaftCore::election_timeout_expired() const {
    // Leaders do not start elections.
    if (role_ == NodeRole::leader) {
        return false;
    }

    return current_time_ms_ >= election_deadline_ms_;
}

void RaftCore::advance_time(
    const uint64_t elapsed_ms
) {
    // Advance logical time without sleeping.
    current_time_ms_ += elapsed_ms;
}

void RaftCore::reset_election_deadline() {
    // Select a timeout from the inclusive configured range.
    uniform_int_distribution<uint64_t> timeout_distribution{
        min_election_timeout_ms_,
        max_election_timeout_ms_
    };

    election_timeout_ms_ =
        timeout_distribution(random_engine_);

    // Convert the duration into an absolute logical deadline.
    election_deadline_ms_ =
        current_time_ms_ + election_timeout_ms_;
}

size_t RaftCore::majority_size() const {
    // Three nodes require two votes.
    // Five nodes require three votes.
    return cluster_size() / 2 + 1;
}

void RaftCore::start_election() {
    // Every new election starts in a new term.
    ++current_term_;

    role_ = NodeRole::candidate;

    // Discard votes from any previous election.
    votes_received_.clear();

    // A candidate votes for itself.
    voted_for_ = node_id_;
    votes_received_.insert(node_id_);

    // Select a fresh timeout for this election.
    reset_election_deadline();

    // A one-node cluster immediately has a majority.
    if (votes_received() >= majority_size()) {
        role_ = NodeRole::leader;
    }
}

RequestVoteRequest
RaftCore::make_request_vote_request() const {
    if (role_ != NodeRole::candidate) {
        throw logic_error{
            "Only a candidate can create a vote request"
        };
    }

    RequestVoteRequest request;

    request.term = current_term_;
    request.candidate_id = node_id_;
    request.last_log_index = last_log_index();
    request.last_log_term = last_log_term();

    return request;
}

bool RaftCore::candidate_log_is_up_to_date(
    const uint64_t candidate_last_log_index,
    const uint64_t candidate_last_log_term
) const {
    // Compare final log terms before comparing indexes.
    if (candidate_last_log_term > last_log_term()) {
        return true;
    }

    if (candidate_last_log_term < last_log_term()) {
        return false;
    }

    // If terms match, the candidate must have at least as
    // many entries as this node.
    return candidate_last_log_index >= last_log_index();
}

RequestVoteResponse
RaftCore::handle_request_vote(
    const RequestVoteRequest& request
) {
    // Begin with a rejected response.
    RequestVoteResponse response{
        current_term_,
        false
    };

    // Reject unknown candidates.
    if (
        cluster_members_.find(request.candidate_id) ==
        cluster_members_.end()
    ) {
        return response;
    }

    // Reject requests from older terms.
    if (request.term < current_term_) {
        return response;
    }

    // Adopt a newer term before evaluating the vote.
    if (request.term > current_term_) {
        become_follower(request.term);
    }

    // become_follower() may have changed the current term.
    response.term = current_term_;

    const bool has_not_voted =
        !voted_for_.has_value();

    const bool already_voted_for_candidate =
        voted_for_.has_value() &&
        voted_for_.value() == request.candidate_id;

    const bool can_vote =
        has_not_voted ||
        already_voted_for_candidate;

    if (!can_vote) {
        return response;
    }

    const bool log_is_up_to_date =
        candidate_log_is_up_to_date(
            request.last_log_index,
            request.last_log_term
        );

    if (!log_is_up_to_date) {
        return response;
    }

    // All vote conditions passed.
    voted_for_ = request.candidate_id;
    response.vote_granted = true;

    return response;
}

void RaftCore::receive_vote(
    const string& voter_id,
    const RequestVoteResponse& response
) {
    // Reject responses from unknown members.
    if (
        cluster_members_.find(voter_id) ==
        cluster_members_.end()
    ) {
        throw invalid_argument{
            "Received a vote from an unknown cluster member"
        };
    }

    // A higher response term makes this candidate outdated.
    if (response.term > current_term_) {
        become_follower(response.term);
        return;
    }

    // Ignore responses from older elections.
    if (response.term < current_term_) {
        return;
    }

    // Only candidates collect votes.
    if (role_ != NodeRole::candidate) {
        return;
    }

    if (!response.vote_granted) {
        return;
    }

    // The set prevents duplicate vote counting.
    votes_received_.insert(voter_id);

    if (votes_received() >= majority_size()) {
        role_ = NodeRole::leader;
    }
}

void RaftCore::become_follower(
    const uint64_t new_term
) {
    if (new_term <= current_term_) {
        throw logic_error{
            "Cannot become follower using an old or equal term"
        };
    }

    current_term_ = new_term;
    role_ = NodeRole::follower;

    // The node has not voted in the new term.
    voted_for_.reset();

    // Old election votes are no longer useful.
    votes_received_.clear();
}

}  // namespace miniraft