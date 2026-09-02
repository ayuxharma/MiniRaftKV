#include "miniraft/raft_core.hpp"

#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

// MiniRaft types used by the tests.
using miniraft::AppendEntriesAction;
using miniraft::AppendEntriesResponse;
using miniraft::AppendEntriesRequest;
using miniraft::LogEntry;
using miniraft::NodeRole;
using miniraft::RaftCore;
using miniraft::RequestVoteAction;
using miniraft::RequestVoteRequest;
using miniraft::RequestVoteResponse;

using miniraft::FileMetadata;


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

void test_leader_queues_heartbeat_for_each_follower() {
    RaftCore leader{
        "node-1",
        three_node_cluster(),
        0,
        {},
        100,
        100,
        1
    };

    leader.start_election();

    // The self-vote plus node 2's vote creates a majority.
    leader.receive_vote(
        "node-2",
        RequestVoteResponse{
            1,
            true
        }
    );

    expect(
        leader.role() == NodeRole::leader,
        "Candidate becomes leader before queuing heartbeats"
    );

    expect(
        leader.pending_append_entries_count() == 2,
        "New leader queues one heartbeat per follower"
    );

    const vector<AppendEntriesAction> actions =
        leader.take_append_entries_actions();

    bool contains_node_2 = false;
    bool contains_node_3 = false;
    bool contains_leader = false;
    bool all_actions_are_current = true;

    for (const AppendEntriesAction& action : actions) {
        if (action.target_node_id == "node-2") {
            contains_node_2 = true;
        }

        if (action.target_node_id == "node-3") {
            contains_node_3 = true;
        }

        if (action.target_node_id == "node-1") {
            contains_leader = true;
        }

        if (
            action.request.term != 1 ||
            action.request.leader_id != "node-1"
        ) {
            all_actions_are_current = false;
        }
    }

    expect(
        actions.size() == 2,
        "Three-node leader creates exactly two heartbeat actions"
    );

    expect(
        contains_node_2,
        "Heartbeat queue contains node 2"
    );

    expect(
        contains_node_3,
        "Heartbeat queue contains node 3"
    );

    expect(
        !contains_leader,
        "Leader does not send a heartbeat to itself"
    );

    expect(
        all_actions_are_current,
        "Every heartbeat contains the current leader and term"
    );
}

void test_taking_heartbeat_actions_clears_queue() {
    RaftCore leader{
        "node-1",
        three_node_cluster()
    };

    leader.start_election();

    leader.receive_vote(
        "node-2",
        RequestVoteResponse{
            1,
            true
        }
    );

    const vector<AppendEntriesAction> first_actions =
        leader.take_append_entries_actions();

    const vector<AppendEntriesAction> second_actions =
        leader.take_append_entries_actions();

    expect(
        first_actions.size() == 2,
        "First take returns queued heartbeat actions"
    );

    expect(
        leader.pending_append_entries_count() == 0,
        "Taking heartbeat actions clears the internal queue"
    );

    expect(
        second_actions.empty(),
        "Taking from an empty heartbeat queue returns no actions"
    );
}

void test_follower_cannot_queue_heartbeats() {
    RaftCore follower{
        "node-1",
        three_node_cluster()
    };

    bool exception_was_thrown = false;

    try {
        follower.queue_heartbeat_actions();
    } catch (const logic_error&) {
        exception_was_thrown = true;
    }

    expect(
        exception_was_thrown,
        "Follower cannot queue heartbeat actions"
    );
}

void test_higher_term_heartbeat_response_stops_leader() {
    RaftCore leader{
        "node-1",
        three_node_cluster()
    };

    leader.start_election();

    leader.receive_vote(
        "node-2",
        RequestVoteResponse{
            1,
            true
        }
    );

    expect(
        leader.pending_append_entries_count() == 2,
        "Leader has pending heartbeats before stepping down"
    );

    const AppendEntriesResponse newer_response{
        2,
        false
    };

    leader.receive_append_entries_response(
        "node-3",
        newer_response
    );

    expect(
        leader.role() == NodeRole::follower,
        "Higher-term heartbeat response makes leader step down"
    );

    expect(
        leader.current_term() == 2,
        "Former leader adopts the newer term"
    );

    expect(
        !leader.leader_id().has_value(),
        "Former leader no longer recognizes itself as leader"
    );

    expect(
        leader.pending_append_entries_count() == 0,
        "Stepping down clears stale heartbeat actions"
    );
}

void test_unknown_heartbeat_response_is_rejected() {
    RaftCore leader{
        "node-1",
        three_node_cluster()
    };

    leader.start_election();

    leader.receive_vote(
        "node-2",
        RequestVoteResponse{
            1,
            true
        }
    );

    bool exception_was_thrown = false;

    try {
        leader.receive_append_entries_response(
            "node-99",
            AppendEntriesResponse{
                1,
                true
            }
        );
    } catch (const invalid_argument&) {
        exception_was_thrown = true;
    }

    expect(
        exception_was_thrown,
        "Heartbeat response from unknown node is rejected"
    );
}

void test_follower_appends_entries_to_empty_log() {
    RaftCore follower{
        "node-2",
        three_node_cluster(),
        0,
        {},
        100,
        100,
        1
    };

    const AppendEntriesRequest request{
        1,
        "node-1",
        0,
        0,
        {
            LogEntry{1, "UPDATE notes.txt VERSION 1"},
            LogEntry{1, "UPDATE photo.jpg VERSION 1"}
        },
        0
    };

    const auto response =
        follower.handle_append_entries(request);

    expect(
        response.success,
        "Follower accepts entries after matching empty history"
    );

    expect(
        response.matched_index == 2,
        "Response reports both appended entries as matched"
    );

    expect(
        follower.log_entries().size() == 2,
        "Follower appends both entries"
    );

    expect(
        follower.log_entries()[0].command ==
            "UPDATE notes.txt VERSION 1",
        "Follower stores the first command"
    );

    expect(
        follower.log_entries()[1].command ==
            "UPDATE photo.jpg VERSION 1",
        "Follower stores the second command"
    );
}

void test_matching_entries_are_not_duplicated() {
    const vector<LogEntry> existing_log{
        LogEntry{1, "UPDATE notes.txt VERSION 1"},
        LogEntry{2, "UPDATE notes.txt VERSION 2"}
    };

    RaftCore follower{
        "node-2",
        three_node_cluster(),
        2,
        existing_log,
        100,
        100,
        1
    };

    const AppendEntriesRequest request{
        2,
        "node-1",
        0,
        0,
        {
            LogEntry{1, "UPDATE notes.txt VERSION 1"},
            LogEntry{2, "UPDATE notes.txt VERSION 2"}
        },
        0
    };

    const auto response =
        follower.handle_append_entries(request);

    expect(
        response.success,
        "Follower accepts entries that already match"
    );

    expect(
        follower.log_entries().size() == 2,
        "Matching entries are not appended twice"
    );

    expect(
        response.matched_index == 2,
        "Already stored entries are reported as matched"
    );
}

void test_follower_appends_only_missing_suffix() {
    const vector<LogEntry> existing_log{
        LogEntry{1, "UPDATE notes.txt VERSION 1"},
        LogEntry{2, "UPDATE notes.txt VERSION 2"}
    };

    RaftCore follower{
        "node-2",
        three_node_cluster(),
        3,
        existing_log,
        100,
        100,
        1
    };

    const AppendEntriesRequest request{
        3,
        "node-1",
        1,
        1,
        {
            LogEntry{2, "UPDATE notes.txt VERSION 2"},
            LogEntry{3, "UPDATE notes.txt VERSION 3"}
        },
        0
    };

    const auto response =
        follower.handle_append_entries(request);

    expect(
        response.success,
        "Follower accepts matching prefix and missing suffix"
    );

    expect(
        follower.log_entries().size() == 3,
        "Follower appends only the missing entry"
    );

    expect(
        follower.log_entries()[2].term == 3,
        "Missing entry keeps the leader's term"
    );

    expect(
        follower.log_entries()[2].command ==
            "UPDATE notes.txt VERSION 3",
        "Missing command is appended at the correct position"
    );

    expect(
        response.matched_index == 3,
        "Response reports the complete replicated suffix"
    );
}

void test_follower_removes_conflicting_suffix() {
    const vector<LogEntry> conflicting_log{
        LogEntry{1, "UPDATE notes.txt VERSION 1"},
        LogEntry{2, "OLD UNCOMMITTED COMMAND"},
        LogEntry{2, "ANOTHER OLD COMMAND"}
    };

    RaftCore follower{
        "node-2",
        three_node_cluster(),
        3,
        conflicting_log,
        100,
        100,
        1
    };

    const AppendEntriesRequest request{
        3,
        "node-1",
        1,
        1,
        {
            LogEntry{3, "UPDATE notes.txt VERSION 2"},
            LogEntry{3, "UPDATE notes.txt VERSION 3"}
        },
        0
    };

    const auto response =
        follower.handle_append_entries(request);

    expect(
        response.success,
        "Follower accepts leader entries after conflict repair"
    );

    expect(
        follower.log_entries().size() == 3,
        "Conflicting suffix is replaced by leader suffix"
    );

    expect(
        follower.log_entries()[0].term == 1,
        "Matching prefix remains unchanged"
    );

    expect(
        follower.log_entries()[1].term == 3,
        "First conflicting entry is replaced"
    );

    expect(
        follower.log_entries()[1].command ==
            "UPDATE notes.txt VERSION 2",
        "Leader command replaces old conflicting command"
    );

    expect(
        follower.log_entries()[2].command ==
            "UPDATE notes.txt VERSION 3",
        "Entries after the conflict are replaced"
    );

    expect(
        response.matched_index == 3,
        "Conflict repair reports the new matched index"
    );
}

void test_previous_log_mismatch_preserves_log() {
    const vector<LogEntry> existing_log{
        LogEntry{1, "UPDATE notes.txt VERSION 1"},
        LogEntry{2, "UPDATE notes.txt VERSION 2"}
    };

    RaftCore follower{
        "node-2",
        three_node_cluster(),
        3,
        existing_log,
        100,
        100,
        1
    };

    const AppendEntriesRequest request{
        3,
        "node-1",
        2,
        1,
        {
            LogEntry{3, "UPDATE notes.txt VERSION 3"}
        },
        0
    };

    const auto response =
        follower.handle_append_entries(request);

    expect(
        !response.success,
        "Follower rejects mismatched previous log term"
    );

    expect(
        response.matched_index == 0,
        "Rejected request does not report a matched index"
    );

    expect(
        follower.log_entries().size() == 2,
        "Rejected request does not change log size"
    );

    expect(
        follower.log_entries()[1].term == 2,
        "Rejected request preserves existing log terms"
    );

    expect(
        follower.log_entries()[1].command ==
            "UPDATE notes.txt VERSION 2",
        "Rejected request preserves existing commands"
    );
}

void test_invalid_entry_batch_is_atomic() {
    const vector<LogEntry> existing_log{
        LogEntry{1, "UPDATE notes.txt VERSION 1"}
    };

    RaftCore follower{
        "node-2",
        three_node_cluster(),
        2,
        existing_log,
        100,
        100,
        1
    };

    const AppendEntriesRequest request{
        2,
        "node-1",
        1,
        1,
        {
            LogEntry{2, "UPDATE notes.txt VERSION 2"},

            // Term zero makes the complete batch invalid.
            LogEntry{0, "INVALID ENTRY"}
        },
        0
    };

    const auto response =
        follower.handle_append_entries(request);

    expect(
        !response.success,
        "Follower rejects a batch containing an invalid entry"
    );

    expect(
        follower.log_entries().size() == 1,
        "Invalid batch does not append its valid prefix"
    );

    expect(
        follower.log_entries()[0].command ==
            "UPDATE notes.txt VERSION 1",
        "Invalid batch leaves the original log unchanged"
    );
}

void test_matched_index_covers_only_request_history() {
    const vector<LogEntry> existing_log{
        LogEntry{1, "UPDATE notes.txt VERSION 1"},
        LogEntry{2, "UNVERIFIED ENTRY 2"},
        LogEntry{3, "UNVERIFIED ENTRY 3"}
    };

    RaftCore follower{
        "node-2",
        three_node_cluster(),
        3,
        existing_log,
        100,
        100,
        1
    };

    // This heartbeat verifies only index 1.
    const AppendEntriesRequest request{
        3,
        "node-1",
        1,
        1,
        {},
        0
    };

    const auto response =
        follower.handle_append_entries(request);

    expect(
        response.success,
        "Follower accepts heartbeat with matching history"
    );

    expect(
        follower.last_log_index() == 3,
        "Heartbeat does not remove unrelated later entries"
    );

    expect(
        response.matched_index == 1,
        "Response reports only the index verified by request"
    );
}

void test_leader_initializes_replication_state() {
    const vector<LogEntry> initial_log{
        LogEntry{1, "UPDATE notes.txt VERSION 1"},
        LogEntry{2, "UPDATE notes.txt VERSION 2"}
    };

    RaftCore leader{
        "node-1",
        three_node_cluster(),
        2,
        initial_log,
        100,
        100,
        1
    };

    leader.start_election();

    leader.receive_vote(
        "node-2",
        RequestVoteResponse{
            3,
            true
        }
    );

    expect(
        leader.role() == NodeRole::leader,
        "Candidate becomes leader before replication initialization"
    );

    expect(
        leader.next_index_for("node-2") == 3,
        "Node 2 next index starts after the leader log"
    );

    expect(
        leader.next_index_for("node-3") == 3,
        "Node 3 next index starts after the leader log"
    );

    expect(
        leader.match_index_for("node-2") == 0,
        "Node 2 starts with no confirmed matched entry"
    );

    expect(
        leader.match_index_for("node-3") == 0,
        "Node 3 starts with no confirmed matched entry"
    );
}

void test_only_leader_can_append_command() {
    RaftCore follower{
        "node-1",
        three_node_cluster()
    };

    bool exception_was_thrown = false;

    try {
        static_cast<void>(
            follower.append_command(
                "UPDATE notes.txt VERSION 1"
            )
        );
    } catch (const logic_error&) {
        exception_was_thrown = true;
    }

    expect(
        exception_was_thrown,
        "Follower cannot append a client command"
    );

    expect(
        follower.log_entries().empty(),
        "Rejected follower command does not modify the log"
    );
}

void test_leader_appends_current_term_command() {
    RaftCore leader{
        "node-1",
        three_node_cluster()
    };

    leader.start_election();

    leader.receive_vote(
        "node-2",
        RequestVoteResponse{
            1,
            true
        }
    );

    const auto new_log_index =
        leader.append_command(
            "UPDATE notes.txt VERSION 1"
        );

    expect(
        new_log_index == 1,
        "First leader command receives logical index one"
    );

    expect(
        leader.log_entries().size() == 1,
        "Leader stores the new command locally"
    );

    expect(
        leader.log_entries()[0].term == 1,
        "New entry uses the leader's current term"
    );

    expect(
        leader.log_entries()[0].command ==
            "UPDATE notes.txt VERSION 1",
        "New log entry stores the client command"
    );

    expect(
        leader.pending_append_entries_count() == 2,
        "Appending a command queues replication for both followers"
    );
}

void test_leader_rejects_empty_command() {
    const vector<string> members{
        "node-1"
    };

    RaftCore leader{
        "node-1",
        members
    };

    leader.start_election();

    bool exception_was_thrown = false;

    try {
        static_cast<void>(
            leader.append_command("")
        );
    } catch (const invalid_argument&) {
        exception_was_thrown = true;
    }

    expect(
        exception_was_thrown,
        "Leader rejects an empty client command"
    );

    expect(
        leader.log_entries().empty(),
        "Rejected empty command does not modify the log"
    );
}

void test_appended_command_is_sent_to_followers() {
    RaftCore leader{
        "node-1",
        three_node_cluster()
    };

    leader.start_election();

    leader.receive_vote(
        "node-2",
        RequestVoteResponse{
            1,
            true
        }
    );

    const auto new_log_index =
        leader.append_command(
            "UPDATE notes.txt VERSION 1"
        );

    static_cast<void>(new_log_index);

    const vector<AppendEntriesAction> actions =
        leader.take_append_entries_actions();

    bool node_2_received_entry = false;
    bool node_3_received_entry = false;
    bool every_request_starts_at_zero = true;

    for (const AppendEntriesAction& action : actions) {
        if (
            action.request.prev_log_index != 0 ||
            action.request.prev_log_term != 0
        ) {
            every_request_starts_at_zero = false;
        }

        if (
            action.request.entries.size() != 1 ||
            action.request.entries[0].command !=
                "UPDATE notes.txt VERSION 1"
        ) {
            continue;
        }

        if (action.target_node_id == "node-2") {
            node_2_received_entry = true;
        }

        if (action.target_node_id == "node-3") {
            node_3_received_entry = true;
        }
    }

    expect(
        actions.size() == 2,
        "Leader creates one replication action per follower"
    );

    expect(
        every_request_starts_at_zero,
        "First replication request follows the empty log position"
    );

    expect(
        node_2_received_entry,
        "Node 2 receives the appended command"
    );

    expect(
        node_3_received_entry,
        "Node 3 receives the appended command"
    );
}

void test_failed_response_moves_next_index_backward() {
    const vector<LogEntry> initial_log{
        LogEntry{1, "COMMAND 1"},
        LogEntry{2, "COMMAND 2"},
        LogEntry{3, "COMMAND 3"}
    };

    RaftCore leader{
        "node-1",
        three_node_cluster(),
        3,
        initial_log
    };

    leader.start_election();

    leader.receive_vote(
        "node-2",
        RequestVoteResponse{
            4,
            true
        }
    );

    expect(
        leader.next_index_for("node-3") == 4,
        "Follower initially starts after leader's third entry"
    );

    const AppendEntriesResponse failed_response{
        4,
        false,
        0
    };

    leader.receive_append_entries_response(
        "node-3",
        failed_response
    );

    expect(
        leader.next_index_for("node-3") == 3,
        "First rejection moves next index backward"
    );

    leader.receive_append_entries_response(
        "node-3",
        failed_response
    );

    expect(
        leader.next_index_for("node-3") == 2,
        "Second rejection moves next index backward again"
    );

    leader.receive_append_entries_response(
        "node-3",
        failed_response
    );

    leader.receive_append_entries_response(
        "node-3",
        failed_response
    );

    expect(
        leader.next_index_for("node-3") == 1,
        "Next index never moves below one"
    );

    expect(
        leader.match_index_for("node-3") == 0,
        "Rejected requests do not change match index"
    );
}

void test_successful_response_updates_replication_progress() {
    const vector<LogEntry> initial_log{
        LogEntry{1, "COMMAND 1"},
        LogEntry{2, "COMMAND 2"},
        LogEntry{3, "COMMAND 3"}
    };

    RaftCore leader{
        "node-1",
        three_node_cluster(),
        3,
        initial_log
    };

    leader.start_election();

    leader.receive_vote(
        "node-2",
        RequestVoteResponse{
            4,
            true
        }
    );

    // Simulate backtracking until the leader sends the full log.
    const AppendEntriesResponse failed_response{
        4,
        false,
        0
    };

    leader.receive_append_entries_response(
        "node-3",
        failed_response
    );

    leader.receive_append_entries_response(
        "node-3",
        failed_response
    );

    leader.receive_append_entries_response(
        "node-3",
        failed_response
    );

    const AppendEntriesResponse successful_response{
        4,
        true,
        3
    };

    leader.receive_append_entries_response(
        "node-3",
        successful_response
    );

    expect(
        leader.match_index_for("node-3") == 3,
        "Successful response updates follower match index"
    );

    expect(
        leader.next_index_for("node-3") == 4,
        "Successful response moves next index after matched entry"
    );
}

void test_delayed_success_does_not_reverse_progress() {
    const vector<LogEntry> initial_log{
        LogEntry{1, "COMMAND 1"},
        LogEntry{2, "COMMAND 2"},
        LogEntry{3, "COMMAND 3"}
    };

    RaftCore leader{
        "node-1",
        three_node_cluster(),
        3,
        initial_log
    };

    leader.start_election();

    leader.receive_vote(
        "node-2",
        RequestVoteResponse{
            4,
            true
        }
    );

    leader.receive_append_entries_response(
        "node-3",
        AppendEntriesResponse{
            4,
            true,
            3
        }
    );

    // This response represents an older request arriving late.
    leader.receive_append_entries_response(
        "node-3",
        AppendEntriesResponse{
            4,
            true,
            1
        }
    );

    expect(
        leader.match_index_for("node-3") == 3,
        "Delayed response does not reduce match index"
    );

    expect(
        leader.next_index_for("node-3") == 4,
        "Delayed response does not reduce next index"
    );
}

void test_followers_receive_individualized_requests() {
    const vector<LogEntry> initial_log{
        LogEntry{1, "COMMAND 1"},
        LogEntry{2, "COMMAND 2"},
        LogEntry{3, "COMMAND 3"}
    };

    RaftCore leader{
        "node-1",
        three_node_cluster(),
        3,
        initial_log
    };

    leader.start_election();

    leader.receive_vote(
        "node-2",
        RequestVoteResponse{
            4,
            true
        }
    );

    // Node 3 rejects once, but node 2 remains fully caught up.
    leader.receive_append_entries_response(
        "node-3",
        AppendEntriesResponse{
            4,
            false,
            0
        }
    );

    leader.queue_heartbeat_actions();

    const vector<AppendEntriesAction> actions =
        leader.take_append_entries_actions();

    bool node_2_received_empty_heartbeat = false;
    bool node_3_received_entry_3 = false;

    for (const AppendEntriesAction& action : actions) {
        if (action.target_node_id == "node-2") {
            node_2_received_empty_heartbeat =
                action.request.prev_log_index == 3 &&
                action.request.prev_log_term == 3 &&
                action.request.entries.empty();
        }

        if (action.target_node_id == "node-3") {
            node_3_received_entry_3 =
                action.request.prev_log_index == 2 &&
                action.request.prev_log_term == 2 &&
                action.request.entries.size() == 1 &&
                action.request.entries[0].term == 3 &&
                action.request.entries[0].command ==
                    "COMMAND 3";
        }
    }

    expect(
        node_2_received_empty_heartbeat,
        "Caught-up follower receives an empty heartbeat"
    );

    expect(
        node_3_received_entry_3,
        "Behind follower receives its missing log suffix"
    );
}

void test_replication_state_is_leader_only() {
    RaftCore node{
        "node-1",
        three_node_cluster()
    };

    bool next_index_was_rejected = false;
    bool match_index_was_rejected = false;

    try {
        static_cast<void>(
            node.next_index_for("node-2")
        );
    } catch (const logic_error&) {
        next_index_was_rejected = true;
    }

    try {
        static_cast<void>(
            node.match_index_for("node-2")
        );
    } catch (const logic_error&) {
        match_index_was_rejected = true;
    }

    expect(
        next_index_was_rejected,
        "Follower cannot read leader next index"
    );

    expect(
        match_index_was_rejected,
        "Follower cannot read leader match index"
    );
}

void test_new_node_has_no_committed_or_applied_entries() {
    RaftCore node{
        "node-1",
        three_node_cluster()
    };

    expect(
        node.commit_index() == 0,
        "A new node starts with commit index zero"
    );

    expect(
        node.last_applied() == 0,
        "A new node starts with last applied index zero"
    );

    expect(
        node.applied_commands().empty(),
        "A new node has not applied any commands"
    );
}

void test_majority_commit_applies_command_once() {
    RaftCore leader{
        "node-1",
        three_node_cluster()
    };

    leader.start_election();
    leader.receive_vote(
        "node-2",
        RequestVoteResponse{1, true}
    );

    static_cast<void>(
        leader.append_command("UPDATE notes.txt VERSION 1")
    );

    expect(
        leader.commit_index() == 0,
        "Leader alone cannot commit in a three-node cluster"
    );

    leader.receive_append_entries_response(
        "node-2",
        AppendEntriesResponse{1, true, 1}
    );

    expect(
        leader.commit_index() == 1,
        "Leader commits after one follower confirms the entry"
    );

    expect(
        leader.last_applied() == 1,
        "Leader applies the newly committed entry"
    );

    expect(
        leader.applied_commands().size() == 1 &&
            leader.applied_commands()[0] ==
                "UPDATE notes.txt VERSION 1",
        "Leader applies the committed command in log order"
    );

    // A repeated response must not apply the same command again.
    leader.receive_append_entries_response(
        "node-2",
        AppendEntriesResponse{1, true, 1}
    );

    expect(
        leader.applied_commands().size() == 1,
        "Repeated responses do not apply a command twice"
    );
}

void test_one_node_leader_commits_immediately() {
    RaftCore leader{
        "node-1",
        vector<string>{"node-1"}
    };

    leader.start_election();

    static_cast<void>(
        leader.append_command("ONLY NODE COMMAND")
    );

    expect(
        leader.commit_index() == 1,
        "One-node leader commits its own entry immediately"
    );

    expect(
        leader.last_applied() == 1 &&
            leader.applied_commands().size() == 1,
        "One-node leader immediately applies its committed entry"
    );
}

void test_current_term_rule_commits_earlier_entries() {
    RaftCore leader{
        "node-1",
        three_node_cluster(),
        1,
        vector<LogEntry>{
            LogEntry{1, "OLD TERM COMMAND"}
        }
    };

    // The election moves this node into term two.
    leader.start_election();
    leader.receive_vote(
        "node-2",
        RequestVoteResponse{2, true}
    );

    leader.receive_append_entries_response(
        "node-2",
        AppendEntriesResponse{2, true, 1}
    );

    expect(
        leader.commit_index() == 0,
        "Leader does not directly commit an old-term entry"
    );

    static_cast<void>(
        leader.append_command("CURRENT TERM COMMAND")
    );

    leader.receive_append_entries_response(
        "node-2",
        AppendEntriesResponse{2, true, 2}
    );

    expect(
        leader.commit_index() == 2,
        "Current-term commitment also commits earlier entries"
    );

    expect(
        leader.applied_commands().size() == 2 &&
            leader.applied_commands()[0] == "OLD TERM COMMAND" &&
            leader.applied_commands()[1] == "CURRENT TERM COMMAND",
        "Earlier and current-term commands are applied in order"
    );
}

void test_follower_clamps_and_applies_leader_commit() {
    RaftCore follower{
        "node-2",
        three_node_cluster()
    };

    const AppendEntriesResponse response =
        follower.handle_append_entries(
            AppendEntriesRequest{
                1,
                "node-1",
                0,
                0,
                {
                    LogEntry{1, "COMMAND 1"},
                    LogEntry{1, "COMMAND 2"}
                },
                99
            }
        );

    expect(
        response.success,
        "Follower accepts the leader's entries"
    );

    expect(
        follower.commit_index() == 2,
        "Follower never commits beyond verified local history"
    );

    expect(
        follower.last_applied() == 2 &&
            follower.applied_commands().size() == 2,
        "Follower applies every newly committed entry"
    );
}

void test_follower_learns_commit_from_later_heartbeat() {
    RaftCore follower{
        "node-2",
        three_node_cluster()
    };

    static_cast<void>(
        follower.handle_append_entries(
            AppendEntriesRequest{
                1,
                "node-1",
                0,
                0,
                {LogEntry{1, "COMMAND 1"}},
                0
            }
        )
    );

    expect(
        follower.commit_index() == 0,
        "Replication alone does not commit a follower entry"
    );

    static_cast<void>(
        follower.handle_append_entries(
            AppendEntriesRequest{
                1,
                "node-1",
                1,
                1,
                {},
                1
            }
        )
    );

    expect(
        follower.commit_index() == 1 &&
            follower.last_applied() == 1,
        "Follower learns and applies commit from a heartbeat"
    );
}

void test_committed_entry_cannot_be_replaced() {
    RaftCore follower{
        "node-2",
        three_node_cluster()
    };

    static_cast<void>(
        follower.handle_append_entries(
            AppendEntriesRequest{
                1,
                "node-1",
                0,
                0,
                {LogEntry{1, "COMMITTED COMMAND"}},
                1
            }
        )
    );

    const AppendEntriesResponse response =
        follower.handle_append_entries(
            AppendEntriesRequest{
                2,
                "node-3",
                0,
                0,
                {LogEntry{2, "CONFLICTING COMMAND"}},
                0
            }
        );

    expect(
        !response.success,
        "Follower rejects replacement of a committed entry"
    );

    expect(
        follower.log_entries()[0].command == "COMMITTED COMMAND" &&
            follower.applied_commands().size() == 1,
        "Committed and applied history remains unchanged"
    );
}

void test_committed_metadata_updates_state_machine() {
    RaftCore leader{
        "node-1",
        vector<string>{
            "node-1"
        }
    };

    leader.start_election();

    static_cast<void>(
        leader.append_metadata(
            FileMetadata{
                "notes.txt",
                1,
                {
                    "hash-a",
                    "hash-b"
                },
                false
            }
        )
    );

    // A one-node cluster commits immediately.
    const FileMetadata* metadata =
        leader.metadata_store().find(
            "notes.txt"
        );

    expect(
        leader.commit_index() == 1 &&
            leader.last_applied() == 1,
        "One-node leader commits and applies metadata"
    );

    expect(
        metadata != nullptr &&
            metadata->version == 1 &&
            metadata->block_hashes.size() == 2 &&
            !metadata->deleted,
        "Committed command updates the metadata state machine"
    );
}

void test_uncommitted_metadata_is_not_visible() {
    RaftCore leader{
        "node-1",
        three_node_cluster()
    };

    leader.start_election();

    leader.receive_vote(
        "node-2",
        RequestVoteResponse{
            1,
            true
        }
    );

    static_cast<void>(
        leader.append_metadata(
            FileMetadata{
                "notes.txt",
                1,
                {
                    "hash-a"
                },
                false
            }
        )
    );

    expect(
        leader.commit_index() == 0,
        "Metadata entry remains uncommitted without replication"
    );

    expect(
        leader.metadata_store().find("notes.txt") == nullptr,
        "Uncommitted metadata is not visible in state machine"
    );

    leader.receive_append_entries_response(
        "node-2",
        AppendEntriesResponse{
            1,
            true,
            1
        }
    );

    expect(
        leader.metadata_store().find("notes.txt") != nullptr,
        "Metadata becomes visible after majority commitment"
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

// AppendEntries heartbeat handling.
test_only_leader_can_create_heartbeat();
test_leader_creates_heartbeat();
test_follower_accepts_valid_heartbeat();
test_valid_heartbeat_resets_election_timer();
test_stale_heartbeat_is_rejected();
test_candidate_steps_down_for_heartbeat();
test_heartbeat_rejects_mismatched_log();
test_unknown_heartbeat_sender_is_rejected();

// Outbound AppendEntries heartbeat actions.
test_leader_queues_heartbeat_for_each_follower();
test_taking_heartbeat_actions_clears_queue();
test_follower_cannot_queue_heartbeats();
test_higher_term_heartbeat_response_stops_leader();
test_unknown_heartbeat_response_is_rejected();

// Follower-side log replication.
test_follower_appends_entries_to_empty_log();
test_matching_entries_are_not_duplicated();
test_follower_appends_only_missing_suffix();
test_follower_removes_conflicting_suffix();
test_previous_log_mismatch_preserves_log();
test_invalid_entry_batch_is_atomic();
test_matched_index_covers_only_request_history();

// Leader-side replication progress.
test_leader_initializes_replication_state();
test_only_leader_can_append_command();
test_leader_appends_current_term_command();
test_leader_rejects_empty_command();
test_appended_command_is_sent_to_followers();
test_failed_response_moves_next_index_backward();
test_successful_response_updates_replication_progress();
test_delayed_success_does_not_reverse_progress();
test_followers_receive_individualized_requests();
test_replication_state_is_leader_only();

// Commit propagation and exactly-once application.
test_new_node_has_no_committed_or_applied_entries();
test_majority_commit_applies_command_once();
test_one_node_leader_commits_immediately();
test_current_term_rule_commits_earlier_entries();
test_follower_clamps_and_applies_leader_commit();
test_follower_learns_commit_from_later_heartbeat();
test_committed_entry_cannot_be_replaced();

// Replicated metadata state machine.
test_committed_metadata_updates_state_machine();
test_uncommitted_metadata_is_not_visible();

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
