#include "miniraft/rpc_conversion.hpp"

namespace miniraft {

rpc::RequestVoteRequest to_rpc(
    const RequestVoteRequest& request
) {
    rpc::RequestVoteRequest result;

    // Protocol Buffer fields are changed using set_ functions.
    result.set_term(
        request.term
    );

    result.set_candidate_id(
        request.candidate_id
    );

    result.set_last_log_index(
        request.last_log_index
    );

    result.set_last_log_term(
        request.last_log_term
    );

    return result;
}

RequestVoteRequest from_rpc(
    const rpc::RequestVoteRequest& request
) {
    // Protocol Buffer fields are read using functions
    // such as term() and candidate_id().
    return RequestVoteRequest{
        request.term(),
        request.candidate_id(),
        request.last_log_index(),
        request.last_log_term()
    };
}

rpc::RequestVoteResponse to_rpc(
    const RequestVoteResponse& response
) {
    rpc::RequestVoteResponse result;

    result.set_term(
        response.term
    );

    result.set_vote_granted(
        response.vote_granted
    );

    return result;
}

RequestVoteResponse from_rpc(
    const rpc::RequestVoteResponse& response
) {
    return RequestVoteResponse{
        response.term(),
        response.vote_granted()
    };
}

rpc::AppendEntriesRequest to_rpc(
    const AppendEntriesRequest& request
) {
    rpc::AppendEntriesRequest result;

    result.set_term(
        request.term
    );

    result.set_leader_id(
        request.leader_id
    );

    result.set_prev_log_index(
        request.prev_log_index
    );

    result.set_prev_log_term(
        request.prev_log_term
    );

    result.set_leader_commit(
        request.leader_commit
    );

    // AppendEntries may carry zero or more log entries.
    for (const LogEntry& entry : request.entries) {
        // add_entries() creates the next generated
        // Protocol Buffer LogEntry.
        rpc::LogEntry* rpc_entry =
            result.add_entries();

        rpc_entry->set_term(
            entry.term
        );

        rpc_entry->set_command(
            entry.command
        );
    }

    return result;
}

AppendEntriesRequest from_rpc(
    const rpc::AppendEntriesRequest& request
) {
    AppendEntriesRequest result;

    result.term =
        request.term();

    result.leader_id =
        request.leader_id();

    result.prev_log_index =
        request.prev_log_index();

    result.prev_log_term =
        request.prev_log_term();

    result.leader_commit =
        request.leader_commit();

    // Reserve enough memory before copying log entries.
    result.entries.reserve(
        static_cast<size_t>(
            request.entries_size()
        )
    );

    for (
        const rpc::LogEntry& entry :
        request.entries()
    ) {
        result.entries.push_back(
            LogEntry{
                entry.term(),
                entry.command()
            }
        );
    }

    return result;
}

rpc::AppendEntriesResponse to_rpc(
    const AppendEntriesResponse& response
) {
    rpc::AppendEntriesResponse result;

    result.set_term(
        response.term
    );

    result.set_success(
        response.success
    );

    result.set_matched_index(
        response.matched_index
    );

    return result;
}

AppendEntriesResponse from_rpc(
    const rpc::AppendEntriesResponse& response
) {
    return AppendEntriesResponse{
        response.term(),
        response.success(),
        response.matched_index()
    };
}

}  // namespace miniraft