#include "miniraft/raft_core.hpp"

#include <iostream>

#include <string>

#include <vector>

// Import the names required by this test file.
using miniraft::NodeRole;
using miniraft::RaftCore;
using std::cerr;
using std::cout;
using std::string;
using std::vector;

namespace {

// Count how many expectations failed during this test run.
int failure_count = 0;

// Check one expected condition.
//
// Unlike the standard assert macro, this function continues running,
// which allows us to see several failures in one execution.
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

// Create the standard membership list used by most tests.
vector<string> three_node_cluster() {
    return {
        "node-1",
        "node-2",
        "node-3"
    };
}

void test_node_starts_as_follower() {
    // Arrange: create a new Raft node.
    RaftCore node{
        "node-1",
        three_node_cluster()
    };

    // Assert: verify the required initial Raft state.
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
}

void test_starting_election_creates_candidate() {
    // Arrange: create a follower.
    RaftCore node{
        "node-1",
        three_node_cluster()
    };

    // Act: simulate the election timeout.
    node.start_election();

    // Assert: the follower should now be a candidate.
    expect(
        node.role() == NodeRole::candidate,
        "Starting an election makes the node a candidate"
    );

    expect(
        node.current_term() == 1,
        "Starting an election increments the term"
    );

    expect(
        node.voted_for().has_value(),
        "A candidate records its own vote"
    );

    expect(
        node.voted_for().value() == "node-1",
        "A candidate votes for itself"
    );

    expect(
        node.votes_received() == 1,
        "A candidate begins with one self-vote"
    );
}

void test_candidate_becomes_leader_after_majority() {
    // Arrange: create a candidate in a three-node cluster.
    RaftCore node{
        "node-1",
        three_node_cluster()
    };

    node.start_election();

    // Act: receive one additional vote.
    //
    // In a three-node cluster:
    // node-1's vote + node-2's vote = majority of two.
    node.receive_vote(
        "node-2",
        1,
        true
    );

    // Assert: the candidate should now be leader.
    expect(
        node.role() == NodeRole::leader,
        "A candidate becomes leader after receiving a majority"
    );

    expect(
        node.votes_received() == 2,
        "The leader election counted two unique votes"
    );
}

void test_duplicate_votes_are_not_counted_twice() {
    // A five-node cluster needs three votes for a majority.
    const vector<string> members{
        "node-1",
        "node-2",
        "node-3",
        "node-4",
        "node-5"
    };

    RaftCore node{
        "node-1",
        members
    };

    node.start_election();

    // Receive node-2's vote twice.
    node.receive_vote("node-2", 1, true);
    node.receive_vote("node-2", 1, true);

    expect(
        node.votes_received() == 2,
        "A duplicate vote is counted only once"
    );

    expect(
        node.role() == NodeRole::candidate,
        "Two unique votes are not a majority in a five-node cluster"
    );

    // A third unique vote now creates a majority.
    node.receive_vote("node-3", 1, true);

    expect(
        node.role() == NodeRole::leader,
        "Three unique votes form a five-node majority"
    );
}

void test_higher_term_forces_leader_to_step_down() {
    // Arrange: elect node-1 as leader in term one.
    RaftCore node{
        "node-1",
        three_node_cluster()
    };

    node.start_election();
    node.receive_vote("node-2", 1, true);

    expect(
        node.role() == NodeRole::leader,
        "The test node first becomes leader"
    );

    // Act: discover that another node is already in term two.
    node.receive_vote(
        "node-3",
        2,
        false
    );

    // Assert: Raft requires the old leader to step down.
    expect(
        node.role() == NodeRole::follower,
        "A higher term forces the node to become follower"
    );

    expect(
        node.current_term() == 2,
        "The node adopts the higher term"
    );

    expect(
        !node.voted_for().has_value(),
        "The node has not voted in the newly discovered term"
    );

    expect(
        node.votes_received() == 0,
        "Votes from the old election are cleared"
    );
}

}  // namespace

int main() {
    // Run each unit test sequentially.
    test_node_starts_as_follower();
    test_starting_election_creates_candidate();
    test_candidate_becomes_leader_after_majority();
    test_duplicate_votes_are_not_counted_twice();
    test_higher_term_forces_leader_to_step_down();

    // A zero exit code tells CTest that everything passed.
    if (failure_count == 0) {
        cout << "\nAll Raft core tests passed.\n";
        return 0;
    }

    // A non-zero exit code tells CTest that the test failed.
    cerr
        << "\n"
        << failure_count
        << " Raft core expectation(s) failed.\n";

    return 1;
}