#include "miniraft/raft_storage.hpp"

#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

using miniraft::FileRaftStorage;
using miniraft::LogEntry;
using miniraft::PersistentRaftState;

using std::cerr;
using std::cout;
using std::filesystem::remove;
using std::filesystem::temp_directory_path;
using std::invalid_argument;
using std::ofstream;
using std::runtime_error;
using std::string;
using std::vector;

namespace {

int failure_count = 0;

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

// Use one known file in the operating system's temporary directory.
string test_state_path() {
    return (
        temp_directory_path() /
        "miniraft_storage_tests.state"
    ).string();
}

void remove_test_file() {
    // remove returns false when the file does not exist.
    static_cast<void>(
        remove(test_state_path())
    );
}

void test_missing_state_file_does_not_exist() {
    remove_test_file();

    const FileRaftStorage storage{
        test_state_path()
    };

    expect(
        !storage.exists(),
        "Missing Raft state file is reported correctly"
    );
}

void test_state_survives_save_and_load() {
    remove_test_file();

    const FileRaftStorage storage{
        test_state_path()
    };

    const PersistentRaftState original{
        3,
        string{"node-2"},
        vector<LogEntry>{
            LogEntry{
                1,
                "UPDATE notes.txt VERSION 1"
            },
            LogEntry{
                3,
                "META|UPSERT|photo.jpg|1|hash-a,hash-b"
            }
        },
        1
    };

    storage.save(original);

    expect(
        storage.exists(),
        "Saving Raft state creates the state file"
    );

    const PersistentRaftState loaded =
        storage.load();

    expect(
        loaded.current_term == 3,
        "Current term survives save and load"
    );

    expect(
        loaded.voted_for.has_value() &&
            loaded.voted_for.value() == "node-2",
        "Recorded vote survives save and load"
    );

    expect(
        loaded.commit_index == 1,
        "Commit index survives save and load"
    );

    expect(
        loaded.log_entries.size() == 2,
        "Complete log survives save and load"
    );

    expect(
        loaded.log_entries[0].term == 1 &&
            loaded.log_entries[0].command ==
                "UPDATE notes.txt VERSION 1",
        "First log entry survives save and load"
    );

    expect(
        loaded.log_entries[1].term == 3 &&
            loaded.log_entries[1].command ==
                "META|UPSERT|photo.jpg|1|hash-a,hash-b",
        "Metadata command survives save and load"
    );

    remove_test_file();
}

void test_absent_vote_survives_save_and_load() {
    remove_test_file();

    const FileRaftStorage storage{
        test_state_path()
    };

    storage.save(
        PersistentRaftState{
            0,
            {},
            {},
            0
        }
    );

    const PersistentRaftState loaded =
        storage.load();

    expect(
        !loaded.voted_for.has_value(),
        "Missing vote survives save and load"
    );

    remove_test_file();
}

void test_save_replaces_previous_state() {
    remove_test_file();

    const FileRaftStorage storage{
        test_state_path()
    };

    storage.save(
        PersistentRaftState{
            1,
            string{"node-1"},
            vector<LogEntry>{
                LogEntry{
                    1,
                    "FIRST COMMAND"
                }
            },
            0
        }
    );

    storage.save(
        PersistentRaftState{
            2,
            string{"node-2"},
            vector<LogEntry>{
                LogEntry{
                    1,
                    "FIRST COMMAND"
                },
                LogEntry{
                    2,
                    "SECOND COMMAND"
                }
            },
            2
        }
    );

    const PersistentRaftState loaded =
        storage.load();

    expect(
        loaded.current_term == 2,
        "New save replaces the old current term"
    );

    expect(
        loaded.voted_for.value_or("") == "node-2",
        "New save replaces the old vote"
    );

    expect(
        loaded.log_entries.size() == 2 &&
            loaded.commit_index == 2,
        "New save replaces old log and commit progress"
    );

    remove_test_file();
}

void test_invalid_state_is_not_saved() {
    remove_test_file();

    const FileRaftStorage storage{
        test_state_path()
    };

    bool exception_was_thrown = false;

    try {
        storage.save(
            PersistentRaftState{
                1,
                {},
                vector<LogEntry>{
                    LogEntry{
                        1,
                        "COMMAND 1"
                    }
                },
                2
            }
        );
    } catch (const invalid_argument&) {
        exception_was_thrown = true;
    }

    expect(
        exception_was_thrown,
        "Commit index beyond log size is rejected"
    );

    expect(
        !storage.exists(),
        "Invalid state does not create a file"
    );
}

void test_corrupted_file_is_rejected() {
    remove_test_file();

    {
        ofstream output{
            test_state_path()
        };

        output
            << "THIS_IS_NOT_A_VALID_RAFT_FILE\n";
    }

    const FileRaftStorage storage{
        test_state_path()
    };

    bool exception_was_thrown = false;

    try {
        static_cast<void>(
            storage.load()
        );
    } catch (const runtime_error&) {
        exception_was_thrown = true;
    }

    expect(
        exception_was_thrown,
        "Corrupted Raft state file is rejected"
    );

    remove_test_file();
}

void test_empty_storage_path_is_rejected() {
    bool exception_was_thrown = false;

    try {
        const FileRaftStorage storage{""};

        static_cast<void>(storage);
    } catch (const invalid_argument&) {
        exception_was_thrown = true;
    }

    expect(
        exception_was_thrown,
        "Empty storage path is rejected"
    );
}

}  // namespace

int main() {
    test_missing_state_file_does_not_exist();
    test_state_survives_save_and_load();
    test_absent_vote_survives_save_and_load();
    test_save_replaces_previous_state();
    test_invalid_state_is_not_saved();
    test_corrupted_file_is_rejected();
    test_empty_storage_path_is_rejected();

    // Clean up even if one expectation failed.
    remove_test_file();

    if (failure_count == 0) {
        cout << "\nAll Raft storage tests passed.\n";
        return 0;
    }

    cerr
        << "\n"
        << failure_count
        << " Raft storage expectation(s) failed.\n";

    return 1;
}