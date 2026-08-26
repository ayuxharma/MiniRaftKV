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

const Optional<string>& RaftCore::leader_id() const {
    return leader_id_;
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

uint64_t RaftCore::next_index_for(
    const string& follower_id
) const {
    if (role_ != NodeRole::leader) {
        throw logic_error{
            "Only a leader has next_index values"
        };
    }

    const auto iterator =
        next_index_.find(follower_id);

    if (iterator == next_index_.end()) {
        throw invalid_argument{
            "next_index requested for unknown follower"
        };
    }

    return iterator->second;
}

uint64_t RaftCore::match_index_for(
    const string& follower_id
) const {
    if (role_ != NodeRole::leader) {
        throw logic_error{
            "Only a leader has match_index values"
        };
    }

    const auto iterator =
        match_index_.find(follower_id);

    if (iterator == match_index_.end()) {
        throw invalid_argument{
            "match_index requested for unknown follower"
        };
    }

    return iterator->second;
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

void RaftCore::initialize_leader_replication_state() {
    next_index_.clear();
    match_index_.clear();

    // A newly elected leader initially assumes that every follower
    // already contains the leader's complete log.
    const uint64_t initial_next_index =
        last_log_index() + 1;

    for (const string& member_id : cluster_members_) {
        if (member_id == node_id_) {
            continue;
        }

        next_index_[member_id] =
            initial_next_index;

        // No follower entry has been confirmed in this term yet.
        match_index_[member_id] = 0;
    }
}

void RaftCore::start_election() {
    // Every election starts in a newer term.
    ++current_term_;

    role_ = NodeRole::candidate;

    leader_id_.reset() ;

    pending_append_entries_actions_.clear() ;

    // Candidate state must not retain old leader replication progress.
    next_index_.clear();
    match_index_.clear();

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

        leader_id_ = node_id_ ;

        // No vote requests are needed after becoming leader.
        pending_request_vote_actions_.clear();

        initialize_leader_replication_state();

        queue_heartbeat_actions() ;
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

AppendEntriesRequest
RaftCore::make_heartbeat_request() const {
    // Only the elected leader may send heartbeats.
    if (role_ != NodeRole::leader) {
        throw logic_error{
            "Only a leader can create a heartbeat"
        };
    }

    AppendEntriesRequest request;

    request.term = current_term_;
    request.leader_id = node_id_;
    request.prev_log_index = last_log_index();
    request.prev_log_term = last_log_term();

    return request;
}

AppendEntriesRequest
RaftCore::make_append_entries_request(
    const string& follower_id
) const {
    if (role_ != NodeRole::leader) {
        throw logic_error{
            "Only a leader can create AppendEntries requests"
        };
    }

    const auto iterator =
        next_index_.find(follower_id);

    if (iterator == next_index_.end()) {
        throw invalid_argument{
            "AppendEntries requested for unknown follower"
        };
    }

    const uint64_t follower_next_index =
        iterator->second;

    AppendEntriesRequest request;

    request.term = current_term_;
    request.leader_id = node_id_;

    // The entry before next_index must match on both nodes.
    request.prev_log_index =
        follower_next_index - 1;

    if (request.prev_log_index == 0) {
        request.prev_log_term = 0;
    } else {
        const size_t previous_vector_index =
            static_cast<size_t>(
                request.prev_log_index - 1
            );

        request.prev_log_term =
            log_entries_[previous_vector_index].term;
    }

    // Send every entry the follower may still be missing.
    for (
        uint64_t logical_index = follower_next_index;
        logical_index <= last_log_index();
        ++logical_index
    ) {
        const size_t vector_index =
            static_cast<size_t>(
                logical_index - 1
            );

        request.entries.push_back(
            log_entries_[vector_index]
        );
    }

    // commit_index is not implemented yet.
    request.leader_commit = 0;

    return request;
}

uint64_t RaftCore::append_command(
    const string& command
) {
    if (role_ != NodeRole::leader) {
        throw logic_error{
            "Only a leader can append client commands"
        };
    }

    if (command.empty()) {
        throw invalid_argument{
            "A client command cannot be empty"
        };
    }

    // The new entry belongs to the leader's current term.
    log_entries_.push_back(
        LogEntry{
            current_term_,
            command
        }
    );

    // Prepare a fresh replication request for every follower.
    queue_heartbeat_actions();

    return last_log_index();
}

void RaftCore::queue_heartbeat_actions() {
    if (role_ != NodeRole::leader) {
        throw logic_error{
            "Only a leader can queue AppendEntries actions"
        };
    }

    // Replace any undelivered actions with fresh requests.
    pending_append_entries_actions_.clear();

    for (const string& member_id : cluster_members_) {
        if (member_id == node_id_) {
            continue;
        }

        // This request may be an empty heartbeat or may contain
        // entries that this particular follower is missing.
        const AppendEntriesRequest request =
            make_append_entries_request(member_id);

        pending_append_entries_actions_.push_back(
            AppendEntriesAction{
                member_id,
                request
            }
        );
    }
}

size_t RaftCore::pending_append_entries_count() const {
    return pending_append_entries_actions_.size();
}

vector<AppendEntriesAction>
RaftCore::take_append_entries_actions() {
    vector<AppendEntriesAction> actions;

    // swap() transfers the queue without copying every action.
    actions.swap(pending_append_entries_actions_);

    return actions;
}

void RaftCore::receive_append_entries_response(
    const string& follower_id,
    const AppendEntriesResponse& response
) {
    if (
        cluster_members_.find(follower_id) ==
        cluster_members_.end() ||
        follower_id == node_id_
    ) {
        throw invalid_argument{
            "Received AppendEntries response from unknown follower"
        };
    }

    // A newer term proves this node is no longer leader.
    if (response.term > current_term_) {
        become_follower(response.term);
        return;
    }

    // Ignore responses belonging to an older term.
    if (response.term < current_term_) {
        return;
    }

    // Only the current leader tracks replication progress.
    if (role_ != NodeRole::leader) {
        return;
    }

    auto next_iterator =
        next_index_.find(follower_id);

    auto match_iterator =
        match_index_.find(follower_id);

    if (
        next_iterator == next_index_.end() ||
        match_iterator == match_index_.end()
    ) {
        throw logic_error{
            "Leader replication state is missing a follower"
        };
    }

    if (!response.success) {
        // Move backward one position and retry in a future round.
        //
        // Index one is the lowest possible next_index because
        // logical log entry indexes begin at one.
        if (next_iterator->second > 1) {
            --next_iterator->second;
        }

        return;
    }

    // A follower cannot confirm an index beyond the leader's log.
    if (response.matched_index > last_log_index()) {
        throw invalid_argument{
            "Follower matched index exceeds leader log"
        };
    }

    // Delayed responses must not move replication progress backward.
    if (
        response.matched_index >
        match_iterator->second
    ) {
        match_iterator->second =
            response.matched_index;
    }

    const uint64_t confirmed_next_index =
        response.matched_index + 1;

    if (
        confirmed_next_index >
        next_iterator->second
    ) {
        next_iterator->second =
            confirmed_next_index;
    }
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

bool RaftCore::previous_log_position_matches(
    const uint64_t previous_log_index,
    const uint64_t previous_log_term
) const {
    // Index zero represents the position before the first entry.
    //
    // Because there is no real entry at index zero, its term must
    // also be zero.
    if (previous_log_index == 0) {
        return previous_log_term == 0;
    }

    // The follower cannot match an index it does not have.
    if (previous_log_index > last_log_index()) {
        return false;
    }

    // Raft indexes start at one, while vector indexes start at zero.
    const size_t vector_index =
        static_cast<size_t>(previous_log_index - 1);

    return
        log_entries_[vector_index].term ==
        previous_log_term;
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

AppendEntriesResponse
RaftCore::handle_append_entries(
    const AppendEntriesRequest& request
) {
    // Reject the heartbeat unless every required check succeeds.
    AppendEntriesResponse response{
        current_term_,
        false,
        0
    };

    // Ignore messages claiming to come from an unknown node.
    if (
        cluster_members_.find(request.leader_id) ==
        cluster_members_.end()
    ) {
        return response;
    }

    // A leader from an older term is no longer valid.
    if (request.term < current_term_) {
        return response;
    }

    // A newer term makes this node's current state outdated.
    if (request.term > current_term_) {
        become_follower(request.term);
    } else if (role_ != NodeRole::follower) {
        // A candidate or old leader must follow a recognized leader
        // from the same term.
        //
        // Keep voted_for_ unchanged because the term did not change.
        role_ = NodeRole::follower;
        votes_received_.clear();
        pending_request_vote_actions_.clear();
        pending_append_entries_actions_.clear();
        next_index_.clear();
        match_index_.clear();
    }

    // The term may have changed after becoming a follower.
    response.term = current_term_;

    // Remember which node is acting as leader.
    leader_id_ = request.leader_id;

    // Receiving a valid current-term leader message prevents this
    // follower from beginning an unnecessary election.
    reset_election_deadline();

    // The leader and follower must agree about the previous entry.
    if (
        !previous_log_position_matches(
            request.prev_log_index,
            request.prev_log_term
        )
    ) {
        return response;
    }

    uint64_t previous_entry_term = request.prev_log_term ;

    for (const LogEntry& entry : request.entries) {
        if (entry.term == 0) return response ;
        if (entry.command.empty()) return response ;
        if (entry.term > request.term) return response ;
        if (entry.term < previous_entry_term) return response ;

        previous_entry_term = entry.term ;
    }

    size_t incoming_offset = 0 ;

    while (incoming_offset < request.entries.size()) {
        const uint64_t logical_index = 
            request.prev_log_index + static_cast<uint64_t>(incoming_offset) + 1 ;

        const size_t vector_index = 
            static_cast<size_t>(logical_index - 1) ;

        if (vector_index >= log_entries_.size()) break ;

        const LogEntry& existing_entry = log_entries_[vector_index] ;

        const LogEntry& incoming_entry = request.entries[incoming_offset] ;

        if (existing_entry.term != incoming_entry.term) {
            log_entries_.resize(vector_index) ;
            break ;
        }

        ++incoming_offset ;
    }

    while (incoming_offset < request.entries.size()) {
        log_entries_.push_back(request.entries[incoming_offset]) ;
        ++incoming_offset ;
    }

    response.success = true ;

    response.matched_index = request.prev_log_index + static_cast<uint64_t>(request.entries.size()) ;

    static_cast<void> (request.leader_commit) ;

    return response ;
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

        leader_id_ = node_id_ ;

        // Stop sending election requests after becoming leader.
        pending_request_vote_actions_.clear();
        initialize_leader_replication_state() ;

        queue_heartbeat_actions() ;
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

    leader_id_.reset() ;

    // Previous election state is no longer valid.
    votes_received_.clear();
    pending_request_vote_actions_.clear();
    pending_append_entries_actions_.clear() ;
    next_index_.clear();
    match_index_.clear();
}

}  // namespace miniraft