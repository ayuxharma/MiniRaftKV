#include "miniraft/raft_storage.hpp"

#include <fstream>
#include <iomanip>
#include <stdexcept>
#include <utility>

namespace miniraft {

using std::ifstream;
using std::invalid_argument;
using std::move;
using std::ofstream;
using std::quoted;
using std::runtime_error;

namespace {

// Versioning lets us detect unsupported file formats later.
const string storage_format = "MINIRAFT_STATE_V1";

void validate_state(
    const PersistentRaftState& state
) {
    if (
        state.commit_index >
        state.log_entries.size()
    ) {
        throw invalid_argument{
            "Commit index cannot exceed log size"
        };
    }

    if (
        state.voted_for.has_value() &&
        state.voted_for->empty()
    ) {
        throw invalid_argument{
            "Persisted vote cannot contain an empty node ID"
        };
    }

    uint64_t previous_term = 0;

    for (const LogEntry& entry : state.log_entries) {
        if (entry.term == 0) {
            throw invalid_argument{
                "Persisted log entry term cannot be zero"
            };
        }

        if (entry.command.empty()) {
            throw invalid_argument{
                "Persisted log command cannot be empty"
            };
        }

        if (entry.term < previous_term) {
            throw invalid_argument{
                "Persisted log terms cannot decrease"
            };
        }

        if (entry.term > state.current_term) {
            throw invalid_argument{
                "Persisted log term cannot exceed current term"
            };
        }

        previous_term = entry.term;
    }
}

void require_label(
    const string& actual,
    const string& expected
) {
    if (actual != expected) {
        throw runtime_error{
            "Raft state file contains an unexpected field"
        };
    }
}

}  // namespace

FileRaftStorage::FileRaftStorage(
    string file_path
)
    : file_path_{move(file_path)} {
    if (file_path_.empty()) {
        throw invalid_argument{
            "Raft storage path cannot be empty"
        };
    }
}

bool FileRaftStorage::exists() const {
    const ifstream input{file_path_};

    return input.good();
}

void FileRaftStorage::save(
    const PersistentRaftState& state
) const {
    validate_state(state);

    // trunc replaces the previous file contents.
    ofstream output{
        file_path_,
        ofstream::out | ofstream::trunc
    };

    if (!output) {
        throw runtime_error{
            "Could not open Raft state file for writing"
        };
    }

    output
        << storage_format
        << '\n';

    output
        << "current_term "
        << state.current_term
        << '\n';

    output
        << "voted_for "
        << (state.voted_for.has_value() ? 1 : 0);

    if (state.voted_for.has_value()) {
        // quoted preserves spaces and special characters.
        output
            << ' '
            << quoted(state.voted_for.value());
    }

    output << '\n';

    output
        << "commit_index "
        << state.commit_index
        << '\n';

    output
        << "log_count "
        << state.log_entries.size()
        << '\n';

    for (const LogEntry& entry : state.log_entries) {
        output
            << "entry "
            << entry.term
            << ' '
            << quoted(entry.command)
            << '\n';
    }

    // Ask the stream to send buffered bytes to the operating system.
    output.flush();

    if (!output) {
        throw runtime_error{
            "Failed while writing Raft state file"
        };
    }
}

PersistentRaftState FileRaftStorage::load() const {
    ifstream input{file_path_};

    if (!input) {
        throw runtime_error{
            "Could not open Raft state file for reading"
        };
    }

    string format;

    if (!getline(input, format)) {
        throw runtime_error{
            "Raft state file is empty"
        };
    }

    if (format != storage_format) {
        throw runtime_error{
            "Unsupported Raft state file format"
        };
    }

    PersistentRaftState state;
    string label;

    if (
        !(input >> label >> state.current_term)
    ) {
        throw runtime_error{
            "Could not read persisted current term"
        };
    }

    require_label(
        label,
        "current_term"
    );

    int has_vote = 0;

    if (!(input >> label >> has_vote)) {
        throw runtime_error{
            "Could not read persisted vote"
        };
    }

    require_label(
        label,
        "voted_for"
    );

    if (has_vote == 1) {
        string voted_for;

        if (!(input >> quoted(voted_for))) {
            throw runtime_error{
                "Could not read persisted candidate ID"
            };
        }

        state.voted_for = voted_for;
    } else if (has_vote != 0) {
        throw runtime_error{
            "Persisted vote flag must be zero or one"
        };
    }

    if (
        !(input >> label >> state.commit_index)
    ) {
        throw runtime_error{
            "Could not read persisted commit index"
        };
    }

    require_label(
        label,
        "commit_index"
    );

    size_t log_count = 0;

    if (!(input >> label >> log_count)) {
        throw runtime_error{
            "Could not read persisted log count"
        };
    }

    require_label(
        label,
        "log_count"
    );

    state.log_entries.reserve(log_count);

    for (size_t index = 0; index < log_count; ++index) {
        LogEntry entry;

        if (
            !(
                input >>
                label >>
                entry.term >>
                quoted(entry.command)
            )
        ) {
            throw runtime_error{
                "Could not read persisted log entry"
            };
        }

        require_label(
            label,
            "entry"
        );

        state.log_entries.push_back(
            move(entry)
        );
    }

    validate_state(state);

    return state;
}

}  // namespace miniraft