#include "miniraft/raft_core.hpp"

#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

// Import the MiniRaft types used by the tests.
using miniraft::LogEntry;
using miniraft::NodeRole;
using miniraft::RaftCore;
using miniraft::RequestVoteRequest;
using miniraft::RequestVoteResponse;

// Import only the standard-library names used below.
using std::cerr;
using std::cout;
using std::invalid_argument;
using std::string;
using std::vector;

namespace {

// Count the number of failed expectations.
int failure_count = 0;

// Check one expected condition and print its result.
void expect(
    const bool condition,
    const string& message
) {
    if (condition) {
        cout << "[PASS] " << message << '\n';
        return;
    }

    cerr << "[FAIL] " << message << '\n';
    ++failure_count;
}

// Return the standard membership used by three-node tests.
vector<string> three_node_cluster() {
    return {
        "node-1",
        "node-2",
        "node-3"
    };
}

void test_node_starts_as_follower() {
    RaftCore node{
        "node-1",
        three_node_cluster()
    };

    expect(
        node.role() == NodeRole::follower,
        "A new node starts as a follower"
    );

    expect(
        node.current_term() == 0,
        "A new node starts in term zero"
    );

    expect(
        !node.voted_for().has_value(),
        "A new node has not voted"
    );

    expect(
        node.votes_received() == 0,
        "A new node has received no votes"
    );

    expect(
        node.last_log_index() == 0,
        "A new node has an empty log"
    );

    expect(
        node.last_log_term() == 0,
        "An empty log has last term zero"
    );
}

void test_starting_election_creates_candidate() {
    RaftCore node{
        "node-1",
        three_node_cluster()
    };

    node.start_election();

    expect(
        node.role() == NodeRole::candidate,
        "Starting an election makes the node a candidate"
    );

    expect(
        node.current_term() == 1,
        "Starting an election increments the term"
    );

    expect(
        node.voted_for().value_or("") == "node-1",
        "A candidate votes for itself"
    );

    expect(
        node.votes_received() == 1,
        "A candidate begins with one self-vote"
    );
}

void test_candidate_creates_vote_request() {
    RaftCore candidate{
        "node-1",
        three_node_cluster()
    };

    candidate.start_election();

    const RequestVoteRequest request =
        candidate.make_request_vote_request();

    expect(
        request.term == 1,
        "Vote request contains the candidate's current term"
    );

    expect(
        request.candidate_id == "node-1",
        "Vote request contains the candidate ID"
    );

    expect(
        request.last_log_index == 0,
        "Empty candidate log has last index zero"
    );

    expect(
        request.last_log_term == 0,
        "Empty candidate log has last term zero"
    );
}

void test_follower_grants_first_vote() {
    RaftCore follower{
        "node-2",
        three_node_cluster()
    };

    const RequestVoteRequest request{
        1,
        "node-1",
        0,
        0
    };

    const RequestVoteResponse response =
        follower.handle_request_vote(request);

    expect(
        response.vote_granted,
        "A follower grants its first valid vote"
    );

    expect(
        response.term == 1,
        "Vote response contains the follower's current term"
    );

    expect(
        follower.current_term() == 1,
        "Follower adopts the candidate's newer term"
    );

    expect(
        follower.voted_for().value_or("") == "node-1",
        "Follower records the selected candidate"
    );
}

void test_repeated_vote_request_is_granted_again() {
    RaftCore follower{
        "node-2",
        three_node_cluster()
    };

    const RequestVoteRequest request{
        1,
        "node-1",
        0,
        0
    };

    const RequestVoteResponse first_response =
        follower.handle_request_vote(request);

    const RequestVoteResponse second_response =
        follower.handle_request_vote(request);

    expect(
        first_response.vote_granted,
        "Follower grants the original vote request"
    );

    expect(
        second_response.vote_granted,
        "Follower grants the repeated request from the same candidate"
    );

    expect(
        follower.voted_for().value_or("") == "node-1",
        "Repeated request does not change the recorded candidate"
    );
}

void test_follower_rejects_second_candidate() {
    RaftCore follower{
        "node-3",
        three_node_cluster()
    };

    const RequestVoteRequest first_request{
        1,
        "node-1",
        0,
        0
    };

    const RequestVoteRequest second_request{
        1,
        "node-2",
        0,
        0
    };

    const RequestVoteResponse first_response =
        follower.handle_request_vote(first_request);

    const RequestVoteResponse second_response =
        follower.handle_request_vote(second_request);

    expect(
        first_response.vote_granted,
        "Follower grants the first vote in a term"
    );

    expect(
        !second_response.vote_granted,
        "Follower rejects another candidate in the same term"
    );

    expect(
        follower.voted_for().value_or("") == "node-1",
        "The first vote remains unchanged"
    );
}

void test_stale_vote_request_is_rejected() {
    RaftCore follower{
        "node-3",
        three_node_cluster()
    };

    const RequestVoteRequest newer_request{
        2,
        "node-1",
        0,
        0
    };

    // The result is not needed; this call moves the follower to term two.
    static_cast<void>(
        follower.handle_request_vote(newer_request)
    );

    const RequestVoteRequest stale_request{
        1,
        "node-2",
        0,
        0
    };

    const RequestVoteResponse response =
        follower.handle_request_vote(stale_request);

    expect(
        !response.vote_granted,
        "Follower rejects a stale vote request"
    );

    expect(
        response.term == 2,
        "Stale request receives the follower's newer term"
    );

    expect(
        follower.current_term() == 2,
        "A stale request cannot decrease the current term"
    );
}

void test_higher_term_request_resets_previous_vote() {
    RaftCore follower{
        "node-3",
        three_node_cluster()
    };

    const RequestVoteRequest term_one_request{
        1,
        "node-1",
        0,
        0
    };

    static_cast<void>(
        follower.handle_request_vote(term_one_request)
    );

    const RequestVoteRequest term_two_request{
        2,
        "node-2",
        0,
        0
    };

    const RequestVoteResponse response =
        follower.handle_request_vote(term_two_request);

    expect(
        response.vote_granted,
        "Follower may vote again in a newer term"
    );

    expect(
        follower.current_term() == 2,
        "Follower adopts the newer request term"
    );

    expect(
        follower.voted_for().value_or("") == "node-2",
        "Newer term replaces the previous term's vote"
    );
}

void test_unknown_candidate_is_rejected() {
    RaftCore follower{
        "node-2",
        three_node_cluster()
    };

    const RequestVoteRequest request{
        1,
        "node-99",
        0,
        0
    };

    const RequestVoteResponse response =
        follower.handle_request_vote(request);

    expect(
        !response.vote_granted,
        "Follower rejects an unknown candidate"
    );

    expect(
        follower.current_term() == 0,
        "Unknown candidate cannot change the follower's term"
    );

    expect(
        !follower.voted_for().has_value(),
        "Unknown candidate cannot receive a recorded vote"
    );
}

void test_candidate_becomes_leader() {
    RaftCore candidate{
        "node-1",
        three_node_cluster()
    };

    RaftCore follower{
        "node-2",
        three_node_cluster()
    };

    candidate.start_election();

    const RequestVoteRequest request =
        candidate.make_request_vote_request();

    const RequestVoteResponse response =
        follower.handle_request_vote(request);

    candidate.receive_vote(
        "node-2",
        response
    );

    expect(
        candidate.role() == NodeRole::leader,
        "Candidate becomes leader after receiving a majority"
    );

    expect(
        candidate.votes_received() == 2,
        "Candidate counts two unique votes"
    );
}

void test_duplicate_vote_response_is_not_counted_twice() {
    const vector<string> members{
        "node-1",
        "node-2",
        "node-3",
        "node-4",
        "node-5"
    };

    RaftCore candidate{
        "node-1",
        members
    };

    candidate.start_election();

    const RequestVoteResponse granted_response{
        1,
        true
    };

    candidate.receive_vote(
        "node-2",
        granted_response
    );

    candidate.receive_vote(
        "node-2",
        granted_response
    );

    expect(
        candidate.votes_received() == 2,
        "Duplicate vote response is counted only once"
    );

    expect(
        candidate.role() == NodeRole::candidate,
        "Two votes are not a majority in a five-node cluster"
    );

    candidate.receive_vote(
        "node-3",
        granted_response
    );

    expect(
        candidate.role() == NodeRole::leader,
        "Three unique votes form a five-node majority"
    );
}

void test_higher_response_term_stops_candidate() {
    RaftCore candidate{
        "node-1",
        three_node_cluster()
    };

    candidate.start_election();

    const RequestVoteResponse response{
        2,
        false
    };

    candidate.receive_vote(
        "node-2",
        response
    );

    expect(
        candidate.role() == NodeRole::follower,
        "Higher response term makes the candidate step down"
    );

    expect(
        candidate.current_term() == 2,
        "Candidate adopts the higher response term"
    );

    expect(
        !candidate.voted_for().has_value(),
        "Candidate clears its old vote after changing terms"
    );

    expect(
        candidate.votes_received() == 0,
        "Candidate clears votes from the old election"
    );
}

void test_vote_request_contains_log_information() {
    const vector<LogEntry> initial_log{
        {1, "PUT x 10"},
        {1, "PUT y 20"},
        {2, "DELETE x"}
    };

    RaftCore candidate{
        "node-1",
        three_node_cluster(),
        2,
        initial_log
    };

    candidate.start_election();

    const RequestVoteRequest request =
        candidate.make_request_vote_request();

    expect(
        request.term == 3,
        "Vote request contains the new election term"
    );

    expect(
        request.last_log_index == 3,
        "Vote request contains the final log index"
    );

    expect(
        request.last_log_term == 2,
        "Vote request contains the final log term"
    );

    expect(
        candidate.log_entries().size() == 3,
        "Candidate stores all supplied log entries"
    );
}

void test_candidate_with_older_log_term_is_rejected() {
    const vector<LogEntry> follower_log{
        {1, "PUT x 10"},
        {3, "PUT y 20"}
    };

    RaftCore follower{
        "node-2",
        three_node_cluster(),
        3,
        follower_log
    };

    // A longer log is still outdated when its final term is older.
    const RequestVoteRequest request{
        4,
        "node-1",
        100,
        2
    };

    const RequestVoteResponse response =
        follower.handle_request_vote(request);

    expect(
        !response.vote_granted,
        "Follower rejects a candidate with an older log term"
    );

    expect(
        follower.current_term() == 4,
        "Follower still adopts the newer election term"
    );

    expect(
        !follower.voted_for().has_value(),
        "Follower does not vote for an outdated candidate"
    );
}

void test_candidate_with_newer_log_term_is_accepted() {
    const vector<LogEntry> follower_log{
        {1, "PUT x 10"},
        {2, "PUT y 20"},
        {2, "DELETE x"}
    };

    RaftCore follower{
        "node-2",
        three_node_cluster(),
        3,
        follower_log
    };

    // A newer final term wins even when the candidate has fewer entries.
    const RequestVoteRequest request{
        4,
        "node-1",
        1,
        3
    };

    const RequestVoteResponse response =
        follower.handle_request_vote(request);

    expect(
        response.vote_granted,
        "Follower accepts a candidate with a newer log term"
    );

    expect(
        follower.voted_for().value_or("") == "node-1",
        "Follower records the vote for the newer candidate log"
    );
}

void test_shorter_log_with_equal_term_is_rejected() {
    const vector<LogEntry> follower_log{
        {1, "PUT x 10"},
        {2, "PUT y 20"},
        {2, "DELETE x"}
    };

    RaftCore follower{
        "node-2",
        three_node_cluster(),
        3,
        follower_log
    };

    // Both logs end in term two, but the candidate has fewer entries.
    const RequestVoteRequest request{
        4,
        "node-1",
        2,
        2
    };

    const RequestVoteResponse response =
        follower.handle_request_vote(request);

    expect(
        !response.vote_granted,
        "Follower rejects a shorter log with the same final term"
    );
}

void test_equal_log_is_accepted() {
    const vector<LogEntry> follower_log{
        {1, "PUT x 10"},
        {2, "PUT y 20"},
        {2, "DELETE x"}
    };

    RaftCore follower{
        "node-2",
        three_node_cluster(),
        3,
        follower_log
    };

    const RequestVoteRequest request{
        4,
        "node-1",
        3,
        2
    };

    const RequestVoteResponse response =
        follower.handle_request_vote(request);

    expect(
        response.vote_granted,
        "Follower accepts a candidate with an equally recent log"
    );
}

void test_zero_term_log_entry_is_rejected() {
    bool exception_was_thrown = false;

    try {
        const vector<LogEntry> invalid_log{
            {0, "PUT x 10"}
        };

        const RaftCore node{
            "node-1",
            three_node_cluster(),
            1,
            invalid_log
        };

        static_cast<void>(node);
    } catch (const invalid_argument&) {
        exception_was_thrown = true;
    }

    expect(
        exception_was_thrown,
        "Log entry with term zero is rejected"
    );
}

void test_decreasing_log_terms_are_rejected() {
    bool exception_was_thrown = false;

    try {
        const vector<LogEntry> invalid_log{
            {2, "PUT x 10"},
            {1, "PUT y 20"}
        };

        const RaftCore node{
            "node-1",
            three_node_cluster(),
            2,
            invalid_log
        };

        static_cast<void>(node);
    } catch (const invalid_argument&) {
        exception_was_thrown = true;
    }

    expect(
        exception_was_thrown,
        "Decreasing log-entry terms are rejected"
    );
}

void test_future_log_term_is_rejected() {
    bool exception_was_thrown = false;

    try {
        const vector<LogEntry> invalid_log{
            {2, "PUT x 10"}
        };

        // The node is in term one but the entry claims term two.
        const RaftCore node{
            "node-1",
            three_node_cluster(),
            1,
            invalid_log
        };

        static_cast<void>(node);
    } catch (const invalid_argument&) {
        exception_was_thrown = true;
    }

    expect(
        exception_was_thrown,
        "Log entry from a future term is rejected"
    );
}

void test_initial_election_deadline() {
RaftCore node{
    "node-1",
    three_node_cluster(),
    0,
    {},
    150,
    150,
    1
};

    expect(
        node.current_time_ms() == 0,
        "A new node begins at logical time zero"
    );

    expect(
        node.election_timeout_ms() == 150,
        "Node stores the configured election timeout"
    );

    expect(
        node.election_deadline_ms() == 150,
        "Initial deadline equals the configured timeout"
    );

    expect(
        !node.election_timeout_expired(),
        "Election timeout is initially inactive"
    );
}

void test_election_timeout_expires_at_deadline() {
RaftCore node{
    "node-1",
    three_node_cluster(),
    0,
    {},
    150,
    150,
    1
};
    // Move to one millisecond before the deadline.
    node.advance_time(149);

    expect(
        node.current_time_ms() == 149,
        "Logical time advances by the requested duration"
    );

    expect(
        !node.election_timeout_expired(),
        "Timeout does not expire before its deadline"
    );

    // Reach the exact deadline.
    node.advance_time(1);

    expect(
        node.current_time_ms() == 150,
        "Logical time reaches the election deadline"
    );

    expect(
        node.election_timeout_expired(),
        "Timeout expires exactly at its deadline"
    );
}

void test_starting_election_resets_deadline() {
RaftCore node{
    "node-1",
    three_node_cluster(),
    0,
    {},
    150,
    150,
    1
};
    // Reach the original timeout.
    node.advance_time(150);

    expect(
        node.election_timeout_expired(),
        "Original election timeout expires"
    );

    // Starting the election creates a new deadline.
    node.start_election();

    expect(
        node.role() == NodeRole::candidate,
        "Timed-out follower can become a candidate"
    );

    expect(
        node.election_deadline_ms() == 300,
        "Starting election creates a fresh deadline"
    );

    expect(
        !node.election_timeout_expired(),
        "New election timeout has not expired immediately"
    );

    // Move to the candidate's next deadline.
    node.advance_time(150);

    expect(
        node.election_timeout_expired(),
        "Candidate may time out if it cannot win the election"
    );
}

void test_leader_does_not_expire_election_timeout() {
    // A one-node cluster becomes leader using only its self-vote.
    const vector<string> one_node_cluster{
        "node-1"
    };

    RaftCore node{
        "node-1",
        one_node_cluster,
        0,
        {},
        150
    };

    node.start_election();

    expect(
        node.role() == NodeRole::leader,
        "One-node candidate immediately becomes leader"
    );

    // Advance far beyond the old election deadline.
    node.advance_time(1000);

    expect(
        !node.election_timeout_expired(),
        "Leader does not expire an election timeout"
    );
}

void test_zero_election_timeout_is_rejected() {
    bool exception_was_thrown = false;

    try {
        const RaftCore node{
    "node-1",
    three_node_cluster(),
    0,
    {},
    0,
    0,
    1
};

        static_cast<void>(node);
    } catch (const invalid_argument&) {
        exception_was_thrown = true;
    }

    expect(
        exception_was_thrown,
        "Zero election timeout is rejected"
    );
}

void test_timeout_is_selected_inside_range() {
    RaftCore node{
        "node-1",
        three_node_cluster(),
        0,
        {},
        150,
        300,
        42
    };

    expect(
        node.min_election_timeout_ms() == 150,
        "Node stores the minimum election timeout"
    );

    expect(
        node.max_election_timeout_ms() == 300,
        "Node stores the maximum election timeout"
    );

    expect(
        node.election_timeout_ms() >= 150,
        "Selected timeout is not below the minimum"
    );

    expect(
        node.election_timeout_ms() <= 300,
        "Selected timeout is not above the maximum"
    );

    expect(
        node.election_deadline_ms() ==
            node.election_timeout_ms(),
        "Initial deadline equals the selected timeout"
    );
}

void test_same_seed_produces_same_timeout() {
    RaftCore first_node{
        "node-1",
        three_node_cluster(),
        0,
        {},
        150,
        300,
        12345
    };

    RaftCore second_node{
        "node-1",
        three_node_cluster(),
        0,
        {},
        150,
        300,
        12345
    };

    expect(
        first_node.election_timeout_ms() ==
            second_node.election_timeout_ms(),
        "Same random seed produces repeatable timeout selection"
    );
}

void test_new_election_selects_valid_timeout() {
    RaftCore node{
        "node-1",
        three_node_cluster(),
        0,
        {},
        150,
        300,
        42
    };

    // Advance to the currently selected deadline.
    node.advance_time(
        node.election_timeout_ms()
    );

    expect(
        node.election_timeout_expired(),
        "Initial randomized timeout expires"
    );

    // Starting an election selects another timeout from the range.
    node.start_election();

    expect(
        node.election_timeout_ms() >= 150,
        "New timeout is not below the minimum"
    );

    expect(
        node.election_timeout_ms() <= 300,
        "New timeout is not above the maximum"
    );

    expect(
        node.election_deadline_ms() ==
            node.current_time_ms() +
            node.election_timeout_ms(),
        "New deadline is relative to current logical time"
    );

    expect(
        !node.election_timeout_expired(),
        "Fresh randomized election timeout is not expired"
    );
}

void test_invalid_timeout_range_is_rejected() {
    bool exception_was_thrown = false;

    try {
        const RaftCore node{
            "node-1",
            three_node_cluster(),
            0,
            {},
            300,
            150,
            1
        };

        static_cast<void>(node);
    } catch (const invalid_argument&) {
        exception_was_thrown = true;
    }

    expect(
        exception_was_thrown,
        "Timeout range with maximum below minimum is rejected"
    );
}

void test_nodes_can_use_different_timeouts() {
    // Fixed ranges make this test completely deterministic.
    RaftCore first_node{
        "node-1",
        three_node_cluster(),
        0,
        {},
        170,
        170,
        1
    };

    RaftCore second_node{
        "node-2",
        three_node_cluster(),
        0,
        {},
        225,
        225,
        2
    };

    RaftCore third_node{
        "node-3",
        three_node_cluster(),
        0,
        {},
        280,
        280,
        3
    };

    expect(
        first_node.election_deadline_ms() == 170,
        "First node has the earliest deadline"
    );

    expect(
        second_node.election_deadline_ms() == 225,
        "Second node has the middle deadline"
    );

    expect(
        third_node.election_deadline_ms() == 280,
        "Third node has the latest deadline"
    );
}

void test_tick_before_deadline_does_not_start_election() {
    RaftCore node{
        "node-1",
        three_node_cluster(),
        0,
        {},
        150,
        150,
        1
    };

    // Advance to one millisecond before the deadline.
    const bool election_started =
        node.tick(149);

    expect(
        !election_started,
        "Tick before deadline does not start an election"
    );

    expect(
        node.role() == NodeRole::follower,
        "Node remains a follower before deadline"
    );

    expect(
        node.current_term() == 0,
        "Term does not change before deadline"
    );

    expect(
        node.current_time_ms() == 149,
        "Tick advances the logical clock"
    );
}

void test_tick_at_deadline_starts_election() {
    RaftCore node{
        "node-1",
        three_node_cluster(),
        0,
        {},
        150,
        150,
        1
    };

    // Reach the exact election deadline.
    const bool election_started =
        node.tick(150);

    expect(
        election_started,
        "Tick at deadline starts an election"
    );

    expect(
        node.role() == NodeRole::candidate,
        "Timed-out follower becomes a candidate"
    );

    expect(
        node.current_term() == 1,
        "Automatic election increments the term"
    );

    expect(
        node.voted_for().value_or("") == "node-1",
        "Automatic candidate votes for itself"
    );

    expect(
        node.votes_received() == 1,
        "Automatic candidate begins with one vote"
    );

    expect(
        node.election_deadline_ms() == 300,
        "Automatic election creates a fresh deadline"
    );
}

void test_candidate_timeout_starts_new_election() {
    RaftCore node{
        "node-1",
        three_node_cluster(),
        0,
        {},
        100,
        100,
        1
    };

    // First timeout starts the term-one election.
    const bool first_election_started =
        node.tick(100);

    expect(
        first_election_started,
        "First timeout starts the first election"
    );

    expect(
        node.current_term() == 1,
        "First election uses term one"
    );

    // Move to one millisecond before the candidate's new deadline.
    const bool early_second_election =
        node.tick(99);

    expect(
        !early_second_election,
        "Candidate does not restart election before deadline"
    );

    expect(
        node.current_term() == 1,
        "Candidate remains in the same term before timeout"
    );

    // Reach the candidate's next deadline.
    const bool second_election_started =
        node.tick(1);

    expect(
        second_election_started,
        "Candidate timeout starts another election"
    );

    expect(
        node.role() == NodeRole::candidate,
        "Node remains a candidate during the new election"
    );

    expect(
        node.current_term() == 2,
        "Second election uses a newer term"
    );

    expect(
        node.votes_received() == 1,
        "New election clears old votes and keeps only self-vote"
    );

    expect(
        node.election_deadline_ms() == 300,
        "Second election creates another fresh deadline"
    );
}

void test_leader_does_not_start_another_election() {
    const vector<string> one_node_cluster{
        "node-1"
    };

    RaftCore node{
        "node-1",
        one_node_cluster,
        0,
        {},
        100,
        100,
        1
    };

    // A one-node cluster becomes leader after its first timeout
    // because its self-vote is already a majority.
    const bool first_election_started =
        node.tick(100);

    expect(
        first_election_started,
        "One-node timeout starts an election"
    );

    expect(
        node.role() == NodeRole::leader,
        "One-node candidate immediately becomes leader"
    );

    expect(
        node.current_term() == 1,
        "Leader was elected in term one"
    );

    // Leaders do not start elections when time advances.
    const bool another_election_started =
        node.tick(1000);

    expect(
        !another_election_started,
        "Leader does not start another election"
    );

    expect(
        node.role() == NodeRole::leader,
        "Node remains leader"
    );

    expect(
        node.current_term() == 1,
        "Leader's term does not change because of time"
    );

    expect(
        node.current_time_ms() == 1100,
        "Leader's logical clock still advances"
    );
}

void test_large_tick_starts_only_one_election() {
    RaftCore node{
        "node-1",
        three_node_cluster(),
        0,
        {},
        100,
        100,
        1
    };

    // Even though 1000 ms covers several timeout intervals,
    // one tick represents one processed timeout event.
    const bool election_started =
        node.tick(1000);

    expect(
        election_started,
        "Large tick starts an election"
    );

    expect(
        node.current_term() == 1,
        "One tick starts only one new election term"
    );

    expect(
        node.election_deadline_ms() == 1100,
        "New deadline is relative to the advanced current time"
    );
}

void test_granted_vote_resets_election_deadline() {
    RaftCore follower{
        "node-2",
        three_node_cluster(),
        0,
        {},
        100,
        100,
        1
    };

    // Move close to the original deadline of 100 ms.
    follower.advance_time(90);

    const RequestVoteRequest request{
        1,
        "node-1",
        0,
        0
    };

    const RequestVoteResponse response =
        follower.handle_request_vote(request);

    expect(
        response.vote_granted,
        "Follower grants the valid vote"
    );

    expect(
        follower.current_time_ms() == 90,
        "Granting a vote does not change logical time"
    );

    expect(
        follower.election_deadline_ms() == 190,
        "Granted vote creates a fresh election deadline"
    );

    expect(
        !follower.election_timeout_expired(),
        "Follower is not timed out after granting a vote"
    );
}

void test_follower_waits_until_new_deadline() {
    RaftCore follower{
        "node-2",
        three_node_cluster(),
        0,
        {},
        100,
        100,
        1
    };

    follower.advance_time(90);

    const RequestVoteRequest request{
        1,
        "node-1",
        0,
        0
    };

    static_cast<void>(
        follower.handle_request_vote(request)
    );

    // Move past the original deadline of 100 ms,
    // but remain before the new deadline of 190 ms.
    const bool early_election =
        follower.tick(99);

    expect(
        !early_election,
        "Follower does not use its old election deadline"
    );

    expect(
        follower.current_time_ms() == 189,
        "Follower reaches one millisecond before new deadline"
    );

    expect(
        follower.role() == NodeRole::follower,
        "Follower continues waiting for the elected leader"
    );

    // Reach the new deadline.
    const bool election_started =
        follower.tick(1);

    expect(
        election_started,
        "Follower starts election at its new deadline"
    );

    expect(
        follower.role() == NodeRole::candidate,
        "Follower becomes candidate after new timeout"
    );

    expect(
        follower.current_term() == 2,
        "New election advances from term one to term two"
    );
}

void test_repeated_granted_vote_resets_deadline_again() {
    RaftCore follower{
        "node-2",
        three_node_cluster(),
        0,
        {},
        100,
        100,
        1
    };

    follower.advance_time(90);

    const RequestVoteRequest request{
        1,
        "node-1",
        0,
        0
    };

    const RequestVoteResponse first_response =
        follower.handle_request_vote(request);

    expect(
        first_response.vote_granted,
        "Follower grants the first request"
    );

    expect(
        follower.election_deadline_ms() == 190,
        "First granted request resets the deadline"
    );

    // Simulate time passing before the candidate retries.
    follower.advance_time(50);

    const RequestVoteResponse repeated_response =
        follower.handle_request_vote(request);

    expect(
        repeated_response.vote_granted,
        "Follower grants repeated request from same candidate"
    );

    expect(
        follower.current_time_ms() == 140,
        "Logical time advanced before repeated request"
    );

    expect(
        follower.election_deadline_ms() == 240,
        "Repeated granted request resets the deadline again"
    );
}

void test_stale_request_does_not_reset_deadline() {
    RaftCore follower{
        "node-2",
        three_node_cluster(),
        2,
        {},
        100,
        100,
        1
    };

    follower.advance_time(90);

    const RequestVoteRequest stale_request{
        1,
        "node-1",
        0,
        0
    };

    const RequestVoteResponse response =
        follower.handle_request_vote(stale_request);

    expect(
        !response.vote_granted,
        "Follower rejects the stale vote request"
    );

    expect(
        follower.election_deadline_ms() == 100,
        "Stale request does not reset election deadline"
    );

    const bool election_started =
        follower.tick(10);

    expect(
        election_started,
        "Follower starts election at original deadline"
    );

    expect(
        follower.current_term() == 3,
        "Follower starts the new election in term three"
    );
}

void test_outdated_log_does_not_reset_deadline() {
    const vector<LogEntry> follower_log{
        {1, "PUT x 10"},
        {2, "PUT y 20"}
    };

    RaftCore follower{
        "node-2",
        three_node_cluster(),
        2,
        follower_log,
        100,
        100,
        1
    };

    follower.advance_time(90);

    // The request has a newer election term, but its log
    // ends in the older term one.
    const RequestVoteRequest request{
        3,
        "node-1",
        10,
        1
    };

    const RequestVoteResponse response =
        follower.handle_request_vote(request);

    expect(
        !response.vote_granted,
        "Follower rejects candidate with outdated log"
    );

    expect(
        follower.current_term() == 3,
        "Follower still adopts the newer request term"
    );

    expect(
        follower.election_deadline_ms() == 100,
        "Rejected outdated log does not reset deadline"
    );

    const bool election_started =
        follower.tick(10);

    expect(
        election_started,
        "Follower starts election using original deadline"
    );

    expect(
        follower.current_term() == 4,
        "Timed-out follower begins election in term four"
    );
}

void test_unknown_candidate_does_not_reset_deadline() {
    RaftCore follower{
        "node-2",
        three_node_cluster(),
        0,
        {},
        100,
        100,
        1
    };

    follower.advance_time(90);

    const RequestVoteRequest request{
        1,
        "node-99",
        0,
        0
    };

    const RequestVoteResponse response =
        follower.handle_request_vote(request);

    expect(
        !response.vote_granted,
        "Follower rejects unknown candidate"
    );

    expect(
        follower.election_deadline_ms() == 100,
        "Unknown candidate does not reset election deadline"
    );
}

}  // namespace

int main() {
    // Basic node and election behavior.
    test_node_starts_as_follower();
    test_starting_election_creates_candidate();
    test_candidate_creates_vote_request();

    // Follower-side RequestVote behavior.
    test_follower_grants_first_vote();
    test_repeated_vote_request_is_granted_again();
    test_follower_rejects_second_candidate();
    test_stale_vote_request_is_rejected();
    test_higher_term_request_resets_previous_vote();
    test_unknown_candidate_is_rejected();

    // Candidate-side vote-response behavior.
    test_candidate_becomes_leader();
    test_duplicate_vote_response_is_not_counted_twice();
    test_higher_response_term_stops_candidate();

    // Log metadata and freshness behavior.
    test_vote_request_contains_log_information();
    test_candidate_with_older_log_term_is_rejected();
    test_candidate_with_newer_log_term_is_accepted();
    test_shorter_log_with_equal_term_is_rejected();
    test_equal_log_is_accepted();

    // Invalid log validation.
    test_zero_term_log_entry_is_rejected();
    test_decreasing_log_terms_are_rejected();
    test_future_log_term_is_rejected();

    // Deterministic election-timer tests.
    test_initial_election_deadline();
    test_election_timeout_expires_at_deadline();
    test_starting_election_resets_deadline();
    test_leader_does_not_expire_election_timeout();
    test_zero_election_timeout_is_rejected();

    // Randomized election-timeout tests.
test_timeout_is_selected_inside_range();
test_same_seed_produces_same_timeout();
test_new_election_selects_valid_timeout();
test_invalid_timeout_range_is_rejected();
test_nodes_can_use_different_timeouts();

// Automatic election-timeout behavior.
test_tick_before_deadline_does_not_start_election();
test_tick_at_deadline_starts_election();
test_candidate_timeout_starts_new_election();
test_leader_does_not_start_another_election();
test_large_tick_starts_only_one_election();

// Vote-related election-timer reset behavior.
test_granted_vote_resets_election_deadline();
test_follower_waits_until_new_deadline();
test_repeated_granted_vote_resets_deadline_again();
test_stale_request_does_not_reset_deadline();
test_outdated_log_does_not_reset_deadline();
test_unknown_candidate_does_not_reset_deadline();

    if (failure_count == 0) {
        cout << "\nAll Raft core tests passed.\n";
        return 0;
    }

    cerr
        << "\n"
        << failure_count
        << " Raft core expectation(s) failed.\n";

    return 1;
}