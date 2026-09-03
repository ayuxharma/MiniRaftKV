#pragma once

#include "miniraft/raft_core.hpp"

#include <string>
#include <vector>

namespace miniraft {

using std::string;
using std::uint64_t;
using std::vector;

// safety-critical raft state that must survive a restart
struct PersistentRaftState{
    uint64_t current_term{0};
    Optional<string> voted_for;
    vector<LogEntry> log_entries;
    uint64_t commit_index{0};
};

// Saves and loads one node's persistent Raft state.
// Each Raft node will eventually use a separate state file.
class FileRaftStorage {
public:
    explicit FileRaftStorage(
        string file_path
    );

    [[nodiscard]] bool exists() const;
    
    void save(const PersistentRaftState& state) const;
    [[nodiscard]] PersistentRaftState load() const;

private:
    string file_path_;
};

} // namespace miniraft