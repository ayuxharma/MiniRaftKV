#include "miniraft/rpc_conversion.hpp"

#include <iostream>
#include <string>

using miniraft::AppendEntriesRequest;
using miniraft::AppendEntriesResponse;
using miniraft::LogEntry;
using miniraft::RequestVoteRequest;
using miniraft::RequestVoteResponse;
using miniraft::from_rpc;
using miniraft::to_rpc;

using std::cerr;
using std::cout;
using std::string;

namespace {

int failure_count = 0;

void expect(
    const bool condition,
    const string& message
) {
    if (condition) {
        cout << "[PASS] "
             << message
             << '\n';

        return;
    }

    cerr << "[FAIL] "
         << message
         << '\n';

    ++failure_count;
}

void test_vote_request_round_trip() {
    const RequestVoteRequest original{
        4,
        "node-2",
        7,
        3
    };

    // Internal → RPC → Internal
    const RequestVoteRequest converted =
        from_rpc(
            to_rpc(original)
        );

    expect(
        converted.term ==
            original.term &&
        converted.candidate_id ==
            original.candidate_id &&
        converted.last_log_index ==
            original.last_log_index &&
        converted.last_log_term ==
            original.last_log_term,
        "RequestVote request survives an RPC round trip"
    );
}

void test_vote_response_round_trip() {
    const RequestVoteResponse original{
        4,
        true
    };

    const RequestVoteResponse converted =
        from_rpc(
            to_rpc(original)
        );

    expect(
        converted.term ==
            original.term &&
        converted.vote_granted ==
            original.vote_granted,
        "RequestVote response survives an RPC round trip"
    );
}

void test_append_entries_request_round_trip() {
    const AppendEntriesRequest original{
        5,
        "node-1",
        2,
        4,
        {
            LogEntry{
                5,
                "first-command"
            },
            LogEntry{
                5,
                "second-command"
            }
        },
        3
    };

    const AppendEntriesRequest converted =
        from_rpc(
            to_rpc(original)
        );

    const bool entries_match =
        converted.entries.size() == 2 &&
        converted.entries[0].term == 5 &&
        converted.entries[0].command ==
            "first-command" &&
        converted.entries[1].term == 5 &&
        converted.entries[1].command ==
            "second-command";

    expect(
        converted.term ==
            original.term &&
        converted.leader_id ==
            original.leader_id &&
        converted.prev_log_index ==
            original.prev_log_index &&
        converted.prev_log_term ==
            original.prev_log_term &&
        converted.leader_commit ==
            original.leader_commit &&
        entries_match,
        "AppendEntries request survives an RPC round trip"
    );
}

void test_append_entries_response_round_trip() {
    const AppendEntriesResponse original{
        5,
        true,
        4
    };

    const AppendEntriesResponse converted =
        from_rpc(
            to_rpc(original)
        );

    expect(
        converted.term ==
            original.term &&
        converted.success ==
            original.success &&
        converted.matched_index ==
            original.matched_index,
        "AppendEntries response survives an RPC round trip"
    );
}

}  // namespace

int main() {
    test_vote_request_round_trip();
    test_vote_response_round_trip();
    test_append_entries_request_round_trip();
    test_append_entries_response_round_trip();

    if (failure_count == 0) {
        cout
            << "All RPC conversion tests passed.\n";

        return 0;
    }

    cerr
        << failure_count
        << " RPC conversion test(s) failed.\n";

    return 1;
}