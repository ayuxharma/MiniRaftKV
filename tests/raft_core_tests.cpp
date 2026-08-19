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