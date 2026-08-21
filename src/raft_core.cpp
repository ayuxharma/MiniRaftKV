#include "miniraft/raft_core.hpp"

#include <random>
#include <stdexcept>
#include <utility>

namespace miniraft {

using std::invalid_argument;
using std::logic_error;
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
    : node_id_{std::move(node_id)},
      log_entries_{std::move(initial_log)},
      current_term_{initial_term},
      min_election_timeout_ms_{min_election_timeout_ms},
      max_election_timeout_ms_{max_election_timeout_ms},
      random_engine_{random_seed} {

    // Every node requires a non-empty identity.
    if (node_id_.empty()) {
        throw invalid_argument{
            "Node ID cannot be empty"
        };
    }

    // A zero timeout would expire immediately.
    if (min_election_timeout_ms_ == 0) {
        throw invalid_argument{
            "Minimum election timeout must be greater than zero"
        };
    }

    // Validate the timeout range.
    if (max_election_timeout_ms_ < min_election_timeout_ms_) {
        throw invalid_argument{
            "Maximum election timeout cannot be less than minimum"
        };
    }

    // This mini implementation uses odd-sized clusters.
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

        static_cast<void>(iterator);

        if (!was_inserted) {
            throw invalid_argument{
                "Cluster member IDs must be unique"
            };
        }
    }

    // The node must appear in its own membership list.
    if (
        cluster_members_.find(node_id_) ==
        cluster_members_.end()
    ) {
        throw invalid_argument{
            "Cluster membership must contain this node's ID"
        };
    }

    // Validate the supplied or recovered log.
    uint64_t previous_entry_term = 0;

    for (const LogEntry& entry : log_entries_) {
        if (entry.term == 0) {
            throw invalid_argument{
                "A log entry must have a non-zero term"
            };
        }

        if (entry.command.empty()) {
            throw invalid_argument{
                "A log entry command cannot be empty"
            };
        }

        if (entry.term < previous_entry_term) {
            throw invalid_argument{
                "Log entry terms cannot decrease"
            };
        }

        if (entry.term > current_term_) {
            throw invalid_argument{
                "Log entry term cannot exceed the node's current term"
            };
        }

        previous_entry_term = entry.term;
    }

    // Select the initial randomized timeout.
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
    // Vector size equals the final logical Raft index.
    return static_cast<uint64_t>(log_entries_.size());
}

uint64_t RaftCore::last_log_term() const {
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
    return election_timeout_ms_;
}

uint64_t RaftCore::election_deadline_ms() const {
    return election_deadline_ms_;
}

bool RaftCore::election_timeout_expired() const {
    // Leaders do not begin new elections.
    if (role_ == NodeRole::leader) {
        return false;
    }

    return current_time_ms_ >= election_deadline_ms_;
}

void RaftCore::advance_time(
    const uint64_t elapsed_ms
) {
    // Advance logical time without using sleep().
    current_time_ms_ += elapsed_ms;
}

bool RaftCore::tick(
    const uint64_t elapsed_ms
) {
    // Advance deterministic time.
    advance_time(elapsed_ms);

    // Nothing happens before the deadline.
    if (!election_timeout_expired()) {
        return false;
    }

    // A follower or candidate starts a new election.
    start_election();

    return true;
}

void RaftCore::reset_election_deadline() {
    // Select a timeout from the inclusive configured range.
    uniform_int_distribution<uint64_t> timeout_distribution{
        min_election_timeout_ms_,
        max_election_timeout_ms_
    };

    election_timeout_ms_ =
        timeout_distribution(random_engine_);

    // Convert the duration into an absolute deadline.
    election_deadline_ms_ =
        current_time_ms_ + election_timeout_ms_;
}

size_t RaftCore::majority_size() const {
    return cluster_size() / 2 + 1;
}

size_t RaftCore::pending_request_vote_count() const {
    return pending_request_vote_actions_.size();
}

vector<RequestVoteAction>
RaftCore::take_request_vote_actions() {
    // Move actions out so they are not copied.
    vector<RequestVoteAction> actions{
        std::move(pending_request_vote_actions_)
    };

    // Keep the moved-from queue explicitly empty.
    pending_request_vote_actions_.clear();

    return actions;
}

void RaftCore::queue_request_vote_actions() {
    // Remove messages belonging to an older election.
    pending_request_vote_actions_.clear();

    // Capture one consistent request for this election.
    const RequestVoteRequest request =
        make_request_vote_request();

    for (const string& member_id : cluster_members_) {
        // The candidate already voted for itself.
        if (member_id == node_id_) {
            continue;
        }

        pending_request_vote_actions_.push_back(
            RequestVoteAction{
                member_id,
                request
            }
        );
    }
}

void RaftCore::start_election() {
    // Every election starts in a newer term.
    ++current_term_;

    role_ = NodeRole::candidate;

    // Remove votes from the previous election.
    votes_received_.clear();

    // A candidate always votes for itself.
    voted_for_ = node_id_;
    votes_received_.insert(node_id_);

    // Select a fresh timeout.
    reset_election_deadline();

    // Queue requests for every other cluster member.
    queue_request_vote_actions();

    // A one-node cluster already has a majority.
    if (votes_received() >= majority_size()) {
        role_ = NodeRole::leader;

        // No vote requests are needed after becoming leader.
        pending_request_vote_actions_.clear();
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
    // Compare final terms before comparing indexes.
    if (candidate_last_log_term > last_log_term()) {
        return true;
    }

    if (candidate_last_log_term < last_log_term()) {
        return false;
    }

    // When terms match, the candidate needs at least as many entries.
    return candidate_last_log_index >= last_log_index();
}

RequestVoteResponse
RaftCore::handle_request_vote(
    const RequestVoteRequest& request
) {
    // Reject by default.
    RequestVoteResponse response{
        current_term_,
        false
    };

    // Reject requests from unknown nodes.
    if (
        cluster_members_.find(request.candidate_id) ==
        cluster_members_.end()
    ) {
        return response;
    }

    // Reject stale election terms.
    if (request.term < current_term_) {
        return response;
    }

    // Adopt a newer term before evaluating the request.
    if (request.term > current_term_) {
        become_follower(request.term);
    }

    response.term = current_term_;

    const bool has_not_voted =
        !voted_for_.has_value();

    const bool already_voted_for_candidate =
        voted_for_.has_value() &&
        voted_for_.value() == request.candidate_id;

    if (
        !has_not_voted &&
        !already_voted_for_candidate
    ) {
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

    // Record the vote and start a fresh waiting interval.
    voted_for_ = request.candidate_id;
    reset_election_deadline();

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

    // A newer response term makes this candidate outdated.
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

    // The set prevents duplicate votes from being counted twice.
    votes_received_.insert(voter_id);

    if (votes_received() >= majority_size()) {
        role_ = NodeRole::leader;

        // Stop sending election requests after becoming leader.
        pending_request_vote_actions_.clear();
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

    // Previous election state is no longer valid.
    votes_received_.clear();
    pending_request_vote_actions_.clear();
}

}  // namespace miniraft