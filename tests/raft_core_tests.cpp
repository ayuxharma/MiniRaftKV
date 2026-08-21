#include "miniraft/raft_core.hpp"

#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

// MiniRaft types used by the tests.
using miniraft::AppendEntriesRequest;
using miniraft::LogEntry;
using miniraft::NodeRole;
using miniraft::RaftCore;
using miniraft::RequestVoteAction;
using miniraft::RequestVoteRequest;
using miniraft::RequestVoteResponse;


// Standard-library names used by the tests.
using std::logic_error;
using std::cerr;
using std::cout;
using std::invalid_argument;
using std::string;
using std::vector;

namespace {

int failure_count = 0;

// Check one condition and print a readable result.
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
        "Candidate votes for itself"
    );

    expect(
        node.votes_received() == 1,
        "Candidate begins with one self-vote"
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
        "Vote request contains the candidate term"
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
        "Follower grants its first valid vote"
    );

    expect(
        response.term == 1,
        "Vote response contains the follower term"
    );

    expect(
        follower.current_term() == 1,
        "Follower adopts the candidate term"
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
        "Follower grants the original request"
    );

    expect(
        second_response.vote_granted,
        "Follower grants a repeated request from same candidate"
    );

    expect(
        follower.voted_for().value_or("") == "node-1",
        "Repeated request does not change recorded candidate"
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
        "Follower grants first vote in a term"
    );

    expect(
        !second_response.vote_granted,
        "Follower rejects another candidate in same term"
    );

    expect(
        follower.voted_for().value_or("") == "node-1",
        "First vote remains unchanged"
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
        "Stale request receives follower newer term"
    );

    expect(
        follower.current_term() == 2,
        "Stale request cannot decrease current term"
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
        "Follower adopts newer request term"
    );

    expect(
        follower.voted_for().value_or("") == "node-2",
        "New term replaces previous vote"
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
        "Follower rejects unknown candidate"
    );

    expect(
        follower.current_term() == 0,
        "Unknown candidate cannot change follower term"
    );

    expect(
        !follower.voted_for().has_value(),
        "Unknown candidate cannot receive a vote"
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
        "Candidate becomes leader after majority"
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
        "Duplicate response is counted only once"
    );

    expect(
        candidate.role() == NodeRole::candidate,
        "Two votes are not majority in five-node cluster"
    );

    candidate.receive_vote(
        "node-3",
        granted_response
    );

    expect(
        candidate.role() == NodeRole::leader,
        "Three unique votes form five-node majority"
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
        "Higher response term makes candidate step down"
    );

    expect(
        candidate.current_term() == 2,
        "Candidate adopts higher response term"
    );

    expect(
        !candidate.voted_for().has_value(),
        "Candidate clears old vote after changing term"
    );

    expect(
        candidate.votes_received() == 0,
        "Candidate clears votes from old election"
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
        "Vote request contains new election term"
    );

    expect(
        request.last_log_index == 3,
        "Vote request contains final log index"
    );

    expect(
        request.last_log_term == 2,
        "Vote request contains final log term"
    );

    expect(
        candidate.log_entries().size() == 3,
        "Candidate stores supplied log entries"
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
        "Follower rejects candidate with older log term"
    );

    expect(
        follower.current_term() == 4,
        "Follower still adopts newer election term"
    );

    expect(
        !follower.voted_for().has_value(),
        "Follower does not vote for outdated candidate"
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
        "Follower accepts candidate with newer log term"
    );

    expect(
        follower.voted_for().value_or("") == "node-1",
        "Follower records vote for newer candidate log"
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
        "Follower rejects shorter log with equal final term"
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
        "Follower accepts equally recent log"
    );
}

void test_invalid_logs_are_rejected() {
    bool zero_term_rejected = false;
    bool decreasing_terms_rejected = false;
    bool future_term_rejected = false;

    try {
        const RaftCore node{
            "node-1",
            three_node_cluster(),
            1,
            {{0, "PUT x 10"}}
        };

        static_cast<void>(node);
    } catch (const invalid_argument&) {
        zero_term_rejected = true;
    }

    try {
        const RaftCore node{
            "node-1",
            three_node_cluster(),
            2,
            {
                {2, "PUT x 10"},
                {1, "PUT y 20"}
            }
        };

        static_cast<void>(node);
    } catch (const invalid_argument&) {
        decreasing_terms_rejected = true;
    }

    try {
        const RaftCore node{
            "node-1",
            three_node_cluster(),
            1,
            {{2, "PUT x 10"}}
        };

        static_cast<void>(node);
    } catch (const invalid_argument&) {
        future_term_rejected = true;
    }

    expect(
        zero_term_rejected,
        "Log entry with term zero is rejected"
    );

    expect(
        decreasing_terms_rejected,
        "Decreasing log terms are rejected"
    );

    expect(
        future_term_rejected,
        "Log entry from future term is rejected"
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
        "New node begins at logical time zero"
    );

    expect(
        node.election_timeout_ms() == 150,
        "Node stores configured election timeout"
    );

    expect(
        node.election_deadline_ms() == 150,
        "Initial deadline equals configured timeout"
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

    node.advance_time(149);

    expect(
        !node.election_timeout_expired(),
        "Timeout does not expire before deadline"
    );

    node.advance_time(1);

    expect(
        node.current_time_ms() == 150,
        "Logical time reaches election deadline"
    );

    expect(
        node.election_timeout_expired(),
        "Timeout expires exactly at deadline"
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

    node.advance_time(150);
    node.start_election();

    expect(
        node.election_deadline_ms() == 300,
        "Starting election creates fresh deadline"
    );

    expect(
        !node.election_timeout_expired(),
        "New election timeout is not immediately expired"
    );

    node.advance_time(150);

    expect(
        node.election_timeout_expired(),
        "Candidate can time out without majority"
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
        "Node stores minimum timeout"
    );

    expect(
        node.max_election_timeout_ms() == 300,
        "Node stores maximum timeout"
    );

    expect(
        node.election_timeout_ms() >= 150,
        "Selected timeout is not below minimum"
    );

    expect(
        node.election_timeout_ms() <= 300,
        "Selected timeout is not above maximum"
    );

    expect(
        node.election_deadline_ms() ==
            node.election_timeout_ms(),
        "Initial deadline equals selected timeout"
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
        "Same seed produces repeatable timeout"
    );
}

void test_invalid_timeout_configuration_is_rejected() {
    bool zero_timeout_rejected = false;
    bool invalid_range_rejected = false;

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
        zero_timeout_rejected = true;
    }

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
        invalid_range_rejected = true;
    }

    expect(
        zero_timeout_rejected,
        "Zero election timeout is rejected"
    );

    expect(
        invalid_range_rejected,
        "Maximum timeout below minimum is rejected"
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

    const bool election_started =
        node.tick(149);

    expect(
        !election_started,
        "Tick before deadline does not start election"
    );

    expect(
        node.role() == NodeRole::follower,
        "Node remains follower before deadline"
    );

    expect(
        node.current_term() == 0,
        "Term does not change before deadline"
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

    const bool election_started =
        node.tick(150);

    expect(
        election_started,
        "Tick at deadline starts election"
    );

    expect(
        node.role() == NodeRole::candidate,
        "Timed-out follower becomes candidate"
    );

    expect(
        node.current_term() == 1,
        "Automatic election increments term"
    );

    expect(
        node.voted_for().value_or("") == "node-1",
        "Automatic candidate votes for itself"
    );

    expect(
        node.election_deadline_ms() == 300,
        "Automatic election creates fresh deadline"
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

    const bool first_election =
        node.tick(100);

    const bool early_second_election =
        node.tick(99);

    const bool second_election =
        node.tick(1);

    expect(
        first_election,
        "First timeout starts first election"
    );

    expect(
        !early_second_election,
        "Candidate does not restart before deadline"
    );

    expect(
        second_election,
        "Candidate timeout starts another election"
    );

    expect(
        node.current_term() == 2,
        "Second election uses newer term"
    );

    expect(
        node.votes_received() == 1,
        "New election keeps only self-vote"
    );
}

void test_leader_does_not_start_another_election() {
    const vector<string> members{
        "node-1"
    };

    RaftCore node{
        "node-1",
        members,
        0,
        {},
        100,
        100,
        1
    };

    const bool first_election =
        node.tick(100);

    const bool another_election =
        node.tick(1000);

    expect(
        first_election,
        "One-node timeout starts election"
    );

    expect(
        node.role() == NodeRole::leader,
        "One-node candidate becomes leader"
    );

    expect(
        !another_election,
        "Leader does not start another election"
    );

    expect(
        node.current_term() == 1,
        "Leader term does not change because of time"
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
        "Follower grants valid vote"
    );

    expect(
        follower.current_time_ms() == 90,
        "Granting vote does not change logical time"
    );

    expect(
        follower.election_deadline_ms() == 190,
        "Granted vote creates fresh deadline"
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

    const bool early_election =
        follower.tick(99);

    expect(
        !early_election,
        "Follower does not use old election deadline"
    );

    expect(
        follower.role() == NodeRole::follower,
        "Follower waits for candidate to become leader"
    );

    const bool election_started =
        follower.tick(1);

    expect(
        election_started,
        "Follower starts election at new deadline"
    );

    expect(
        follower.current_term() == 2,
        "New election advances to term two"
    );
}

void test_rejected_requests_do_not_reset_deadline() {
    RaftCore stale_request_follower{
        "node-2",
        three_node_cluster(),
        2,
        {},
        100,
        100,
        1
    };

    stale_request_follower.advance_time(90);

    const RequestVoteRequest stale_request{
        1,
        "node-1",
        0,
        0
    };

    static_cast<void>(
        stale_request_follower.handle_request_vote(
            stale_request
        )
    );

    expect(
        stale_request_follower.election_deadline_ms() == 100,
        "Stale request does not reset deadline"
    );

    const vector<LogEntry> follower_log{
        {1, "PUT x 10"},
        {2, "PUT y 20"}
    };

    RaftCore outdated_log_follower{
        "node-2",
        three_node_cluster(),
        2,
        follower_log,
        100,
        100,
        1
    };

    outdated_log_follower.advance_time(90);

    const RequestVoteRequest outdated_request{
        3,
        "node-1",
        10,
        1
    };

    static_cast<void>(
        outdated_log_follower.handle_request_vote(
            outdated_request
        )
    );

    expect(
        outdated_log_follower.election_deadline_ms() == 100,
        "Outdated candidate log does not reset deadline"
    );
}

void test_follower_has_no_vote_request_actions() {
    RaftCore follower{
        "node-1",
        three_node_cluster(),
        0,
        {},
        150,
        150,
        1
    };

    expect(
        follower.pending_request_vote_count() == 0,
        "Follower has no outbound vote requests"
    );
}

void test_election_creates_request_for_each_peer() {
    const vector<LogEntry> initial_log{
        {1, "PUT x 10"},
        {2, "PUT y 20"}
    };

    RaftCore candidate{
        "node-1",
        three_node_cluster(),
        2,
        initial_log,
        150,
        150,
        1
    };

    candidate.start_election();

    expect(
        candidate.pending_request_vote_count() == 2,
        "Three-node candidate creates two requests"
    );

    const vector<RequestVoteAction> actions =
        candidate.take_request_vote_actions();

    bool found_node_2 = false;
    bool found_node_3 = false;
    bool found_self = false;
    bool requests_are_correct = true;

    for (const RequestVoteAction& action : actions) {
        if (action.target_node_id == "node-2") {
            found_node_2 = true;
        }

        if (action.target_node_id == "node-3") {
            found_node_3 = true;
        }

        if (action.target_node_id == "node-1") {
            found_self = true;
        }

        if (
            action.request.term != 3 ||
            action.request.candidate_id != "node-1" ||
            action.request.last_log_index != 2 ||
            action.request.last_log_term != 2
        ) {
            requests_are_correct = false;
        }
    }

    expect(
        actions.size() == 2,
        "Exactly two vote-request actions are returned"
    );

    expect(
        found_node_2,
        "Candidate creates request for node-2"
    );

    expect(
        found_node_3,
        "Candidate creates request for node-3"
    );

    expect(
        !found_self,
        "Candidate does not request vote from itself"
    );

    expect(
        requests_are_correct,
        "Outbound requests contain current election data"
    );
}

void test_taking_actions_clears_queue() {
    RaftCore candidate{
        "node-1",
        three_node_cluster(),
        0,
        {},
        150,
        150,
        1
    };

    candidate.start_election();

    const vector<RequestVoteAction> first_take =
        candidate.take_request_vote_actions();

    const vector<RequestVoteAction> second_take =
        candidate.take_request_vote_actions();

    expect(
        first_take.size() == 2,
        "First take returns queued vote requests"
    );

    expect(
        candidate.pending_request_vote_count() == 0,
        "Taking requests clears internal queue"
    );

    expect(
        second_take.empty(),
        "Taking from empty queue returns no actions"
    );
}

void test_new_election_replaces_old_actions() {
    RaftCore candidate{
        "node-1",
        three_node_cluster(),
        0,
        {},
        100,
        100,
        1
    };

    candidate.start_election();
    candidate.start_election();

    expect(
        candidate.current_term() == 2,
        "Second election advances to term two"
    );

    expect(
        candidate.pending_request_vote_count() == 2,
        "New election replaces old requests"
    );

    const vector<RequestVoteAction> actions =
        candidate.take_request_vote_actions();

    bool all_actions_use_term_two = true;

    for (const RequestVoteAction& action : actions) {
        if (action.request.term != 2) {
            all_actions_use_term_two = false;
        }
    }

    expect(
        all_actions_use_term_two,
        "Remaining requests use new election term"
    );
}

void test_one_node_cluster_creates_no_vote_requests() {
    const vector<string> members{
        "node-1"
    };

    RaftCore node{
        "node-1",
        members,
        0,
        {},
        100,
        100,
        1
    };

    node.start_election();

    expect(
        node.role() == NodeRole::leader,
        "One-node cluster elects itself"
    );

    expect(
        node.pending_request_vote_count() == 0,
        "One-node leader creates no vote requests"
    );
}

void test_stepping_down_clears_vote_requests() {
    RaftCore candidate{
        "node-1",
        three_node_cluster(),
        0,
        {},
        100,
        100,
        1
    };

    candidate.start_election();

    expect(
        candidate.pending_request_vote_count() == 2,
        "Candidate initially has two pending requests"
    );

    const RequestVoteResponse higher_term_response{
        2,
        false
    };

    candidate.receive_vote(
        "node-2",
        higher_term_response
    );

    expect(
        candidate.role() == NodeRole::follower,
        "Higher term makes candidate step down"
    );

    expect(
        candidate.pending_request_vote_count() == 0,
        "Stepping down removes stale requests"
    );
}

void test_only_leader_can_create_heartbeat() {
    RaftCore follower{
        "node-1",
        three_node_cluster()
    };

    bool exception_was_thrown = false;

    try {
        static_cast<void>(
            follower.make_heartbeat_request()
        );
    } catch (const logic_error&) {
        exception_was_thrown = true;
    }

    expect(
        exception_was_thrown,
        "Follower cannot create a heartbeat"
    );
}

void test_leader_creates_heartbeat() {
    const vector<string> members{
        "node-1"
    };

    RaftCore leader{
        "node-1",
        members,
        2,
        {
            LogEntry{1, "PUT x 10"},
            LogEntry{2, "PUT y 20"}
        },
        100,
        100,
        1
    };

    // A one-node candidate immediately becomes leader.
    leader.start_election();

    const AppendEntriesRequest heartbeat =
        leader.make_heartbeat_request();

    expect(
        leader.role() == NodeRole::leader,
        "One-node cluster creates a leader"
    );

    expect(
        heartbeat.term == 3,
        "Heartbeat contains the leader's current term"
    );

    expect(
        heartbeat.leader_id == "node-1",
        "Heartbeat contains the leader ID"
    );

    expect(
        heartbeat.prev_log_index == 2,
        "Heartbeat contains the leader's last log index"
    );

    expect(
        heartbeat.prev_log_term == 2,
        "Heartbeat contains the leader's last log term"
    );

    expect(
        leader.leader_id().value_or("") == "node-1",
        "Leader recognizes itself as the current leader"
    );
}

void test_follower_accepts_valid_heartbeat() {
    RaftCore follower{
        "node-2",
        three_node_cluster(),
        0,
        {},
        100,
        100,
        1
    };

    const AppendEntriesRequest heartbeat{
        1,
        "node-1",
        0,
        0
    };

    const auto response =
        follower.handle_append_entries(heartbeat);

    expect(
        response.success,
        "Follower accepts a valid heartbeat"
    );

    expect(
        response.term == 1,
        "Heartbeat response contains the follower's term"
    );

    expect(
        follower.current_term() == 1,
        "Follower adopts the leader's newer term"
    );

    expect(
        follower.role() == NodeRole::follower,
        "Heartbeat receiver remains a follower"
    );

    expect(
        follower.leader_id().value_or("") == "node-1",
        "Follower records the recognized leader"
    );
}

void test_valid_heartbeat_resets_election_timer() {
    RaftCore follower{
        "node-2",
        three_node_cluster(),
        0,
        {},
        100,
        100,
        1
    };

    // Move closer to the original deadline of 100 milliseconds.
    follower.advance_time(60);

    const AppendEntriesRequest heartbeat{
        1,
        "node-1",
        0,
        0
    };

    const auto response =
        follower.handle_append_entries(heartbeat);

    expect(
        response.success,
        "Heartbeat used for timer test is accepted"
    );

    expect(
        follower.current_time_ms() == 60,
        "Processing heartbeat does not advance logical time"
    );

    expect(
        follower.election_deadline_ms() == 160,
        "Valid heartbeat starts a fresh election timeout"
    );

    expect(
        !follower.election_timeout_expired(),
        "Follower does not start an election after heartbeat"
    );
}

void test_stale_heartbeat_is_rejected() {
    RaftCore follower{
        "node-2",
        three_node_cluster(),
        2,
        {},
        100,
        100,
        1
    };

    follower.advance_time(60);

    const uint64_t deadline_before_request =
        follower.election_deadline_ms();

    const AppendEntriesRequest stale_heartbeat{
        1,
        "node-1",
        0,
        0
    };

    const auto response =
        follower.handle_append_entries(stale_heartbeat);

    expect(
        !response.success,
        "Follower rejects a stale heartbeat"
    );

    expect(
        response.term == 2,
        "Stale heartbeat response returns the newer local term"
    );

    expect(
        follower.current_term() == 2,
        "Stale heartbeat does not change the follower term"
    );

    expect(
        follower.election_deadline_ms() ==
            deadline_before_request,
        "Stale heartbeat does not reset the election timer"
    );

    expect(
        !follower.leader_id().has_value(),
        "Stale heartbeat does not establish a leader"
    );
}

void test_candidate_steps_down_for_heartbeat() {
    RaftCore candidate{
        "node-1",
        three_node_cluster(),
        0,
        {},
        100,
        100,
        1
    };

    candidate.start_election();

    expect(
        candidate.role() == NodeRole::candidate,
        "Node starts as candidate before heartbeat"
    );

    const AppendEntriesRequest heartbeat{
        1,
        "node-2",
        0,
        0
    };

    const auto response =
        candidate.handle_append_entries(heartbeat);

    expect(
        response.success,
        "Candidate accepts valid same-term heartbeat"
    );

    expect(
        candidate.role() == NodeRole::follower,
        "Candidate steps down after discovering a leader"
    );

    expect(
        candidate.current_term() == 1,
        "Same-term heartbeat does not increment the term"
    );

    expect(
        candidate.voted_for().value_or("") == "node-1",
        "Same-term step-down preserves the existing vote"
    );

    expect(
        candidate.leader_id().value_or("") == "node-2",
        "Former candidate records the current leader"
    );

    expect(
        candidate.pending_request_vote_count() == 0,
        "Stepping down clears pending vote requests"
    );
}

void test_heartbeat_rejects_mismatched_log() {
    RaftCore follower{
        "node-2",
        three_node_cluster(),
        2,
        {
            LogEntry{1, "PUT x 10"},
            LogEntry{2, "PUT y 20"}
        },
        100,
        100,
        1
    };

    const AppendEntriesRequest heartbeat{
        2,
        "node-1",
        2,
        1
    };

    const auto response =
        follower.handle_append_entries(heartbeat);

    expect(
        !response.success,
        "Heartbeat fails when previous log term does not match"
    );

    expect(
        follower.leader_id().value_or("") == "node-1",
        "Follower still recognizes the current leader"
    );

    expect(
        follower.log_entries().size() == 2,
        "Failed heartbeat does not change the follower log"
    );
}

void test_unknown_heartbeat_sender_is_rejected() {
    RaftCore follower{
        "node-2",
        three_node_cluster()
    };

    const AppendEntriesRequest heartbeat{
        1,
        "node-99",
        0,
        0
    };

    const auto response =
        follower.handle_append_entries(heartbeat);

    expect(
        !response.success,
        "Heartbeat from unknown node is rejected"
    );

    expect(
        follower.current_term() == 0,
        "Unknown sender cannot change the follower term"
    );

    expect(
        !follower.leader_id().has_value(),
        "Unknown sender is not recorded as leader"
    );
}

}  // namespace

int main() {
    // Basic Raft state and elections.
    test_node_starts_as_follower();
    test_starting_election_creates_candidate();
    test_candidate_creates_vote_request();

    // RequestVote follower behavior.
    test_follower_grants_first_vote();
    test_repeated_vote_request_is_granted_again();
    test_follower_rejects_second_candidate();
    test_stale_vote_request_is_rejected();
    test_higher_term_request_resets_previous_vote();
    test_unknown_candidate_is_rejected();

    // RequestVote candidate behavior.
    test_candidate_becomes_leader();
    test_duplicate_vote_response_is_not_counted_twice();
    test_higher_response_term_stops_candidate();

    // Log freshness and validation.
    test_vote_request_contains_log_information();
    test_candidate_with_older_log_term_is_rejected();
    test_candidate_with_newer_log_term_is_accepted();
    test_shorter_log_with_equal_term_is_rejected();
    test_equal_log_is_accepted();
    test_invalid_logs_are_rejected();

    // Election timer behavior.
    test_initial_election_deadline();
    test_election_timeout_expires_at_deadline();
    test_starting_election_resets_deadline();
    test_timeout_is_selected_inside_range();
    test_same_seed_produces_same_timeout();
    test_invalid_timeout_configuration_is_rejected();

    // Automatic timeout processing.
    test_tick_before_deadline_does_not_start_election();
    test_tick_at_deadline_starts_election();
    test_candidate_timeout_starts_new_election();
    test_leader_does_not_start_another_election();

    // Timer reset after voting.
    test_granted_vote_resets_election_deadline();
    test_follower_waits_until_new_deadline();
    test_rejected_requests_do_not_reset_deadline();

    // Outbound RequestVote actions.
    test_follower_has_no_vote_request_actions();
    test_election_creates_request_for_each_peer();
    test_taking_actions_clears_queue();
    test_new_election_replaces_old_actions();
    test_one_node_cluster_creates_no_vote_requests();
    test_stepping_down_clears_vote_requests();

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