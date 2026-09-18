#pragma once

#include <cstddef>
#include <string>
#include <unordered_map>

namespace miniraft {

using std::size_t;
using std::string;
using std::unordered_map;

// Build readable commands that can later be placed
// inside the Raft log.
[[nodiscard]]
string make_set_command(
    const string& key,
    const string& value
);

[[nodiscard]]
string make_delete_command(
    const string& key
);

// Identify commands belonging to the key-value state machine.
[[nodiscard]]
bool is_key_value_command(
    const string& command
);

// The small state machine that will be replicated by Raft.
class KeyValueStore {
public:
    // Apply one committed SET or DELETE command.
    void apply(
        const string& command
    );

    // Return nullptr when the key does not exist.
    [[nodiscard]]
    const string* get(
        const string& key
    ) const;

    [[nodiscard]]
    bool contains(
        const string& key
    ) const;

    [[nodiscard]]
    size_t size() const;

private:
    unordered_map<string, string> values_;
};

}  // namespace miniraft