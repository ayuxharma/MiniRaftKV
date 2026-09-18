#include "miniraft/key_value_store.hpp"

#include <stdexcept>

namespace miniraft {

using std::invalid_argument;

namespace {

void validate_key(
    const string& key
) {
    if (key.empty()) {
        throw invalid_argument{
            "Key cannot be empty"
        };
    }

    // Spaces separate fields in our simple command format.
    if (key.find(' ') != string::npos) {
        throw invalid_argument{
            "Key cannot contain spaces"
        };
    }
}

}  // namespace

string make_set_command(
    const string& key,
    const string& value
) {
    validate_key(key);

    return
        "SET " + key + " " + value;
}

string make_delete_command(
    const string& key
) {
    validate_key(key);

    return
        "DELETE " + key;
}

bool is_key_value_command(
    const string& command
) {
    // Position zero means the command must start
    // with SET or DELETE.
    return
        command.rfind("SET ", 0) == 0 ||
        command.rfind("DELETE ", 0) == 0;
}

void KeyValueStore::apply(
    const string& command
) {
    if (command.rfind("SET ", 0) == 0) {
        // A SET command looks like:
        //
        // SET name Ayush Sharma
        //
        // The first separator after SET marks the
        // end of the key. Everything after it is the value.
        const size_t key_end =
            command.find(' ', 4);

        if (key_end == string::npos) {
            throw invalid_argument{
                "SET command must contain a key and value"
            };
        }

        const string key =
            command.substr(
                4,
                key_end - 4
            );

        const string value =
            command.substr(
                key_end + 1
            );

        validate_key(key);

        // Insert a new key or replace its existing value.
        values_.insert_or_assign(
            key,
            value
        );

        return;
    }

    if (command.rfind("DELETE ", 0) == 0) {
        const string key =
            command.substr(7);

        validate_key(key);

        // Deleting a missing key is a harmless no-op.
        values_.erase(key);

        return;
    }

    throw invalid_argument{
        "Unknown key-value command"
    };
}

const string* KeyValueStore::get(
    const string& key
) const {
    const auto iterator =
        values_.find(key);

    if (iterator == values_.end()) {
        return nullptr;
    }

    return &iterator->second;
}

bool KeyValueStore::contains(
    const string& key
) const {
    return
        values_.find(key) !=
        values_.end();
}

size_t KeyValueStore::size() const {
    return values_.size();
}

}  // namespace miniraft