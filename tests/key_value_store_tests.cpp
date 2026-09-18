#include "miniraft/key_value_store.hpp"

#include <iostream>
#include <stdexcept>
#include <string>

using miniraft::KeyValueStore;
using miniraft::is_key_value_command;
using miniraft::make_delete_command;
using miniraft::make_set_command;

using std::cerr;
using std::cout;
using std::invalid_argument;
using std::string;

namespace {

int failure_count = 0;

void expect(
    const bool condition,
    const string& message
) {
    if (condition) {
        cout
            << "[PASS] "
            << message
            << '\n';

        return;
    }

    cerr
        << "[FAIL] "
        << message
        << '\n';

    ++failure_count;
}

void test_new_store_is_empty() {
    const KeyValueStore store;

    expect(
        store.size() == 0,
        "New key-value store is empty"
    );

    expect(
        store.get("name") == nullptr,
        "Unknown key returns nullptr"
    );
}

void test_set_and_update_value() {
    KeyValueStore store;

    store.apply(
        make_set_command(
            "name",
            "Ayush Sharma"
        )
    );

    const string* first_value =
        store.get("name");

    expect(
        first_value != nullptr &&
            *first_value == "Ayush Sharma",
        "SET stores a value"
    );

    store.apply(
        make_set_command(
            "name",
            "MiniRaft"
        )
    );

    const string* updated_value =
        store.get("name");

    expect(
        updated_value != nullptr &&
            *updated_value == "MiniRaft",
        "SET updates an existing value"
    );

    expect(
        store.size() == 1,
        "Updating does not create another key"
    );
}

void test_delete_value() {
    KeyValueStore store;

    store.apply(
        make_set_command(
            "language",
            "C++"
        )
    );

    store.apply(
        make_delete_command(
            "language"
        )
    );

    expect(
        !store.contains("language"),
        "DELETE removes a key"
    );
}

void test_command_helpers() {
    expect(
        make_set_command(
            "project",
            "Mini Raft"
        ) == "SET project Mini Raft",
        "SET helper creates a readable command"
    );

    expect(
        make_delete_command("project") ==
            "DELETE project",
        "DELETE helper creates a readable command"
    );

    expect(
        is_key_value_command(
            "SET project MiniRaft"
        ) &&
        is_key_value_command(
            "DELETE project"
        ) &&
        !is_key_value_command(
            "UNKNOWN project"
        ),
        "Key-value commands are recognized"
    );
}

void test_invalid_command_is_rejected() {
    KeyValueStore store;
    bool exception_was_thrown = false;

    try {
        store.apply(
            "UNKNOWN name"
        );
    } catch (const invalid_argument&) {
        exception_was_thrown = true;
    }

    expect(
        exception_was_thrown,
        "Unknown command is rejected"
    );
}

}  // namespace

int main() {
    test_new_store_is_empty();
    test_set_and_update_value();
    test_delete_value();
    test_command_helpers();
    test_invalid_command_is_rejected();

    if (failure_count == 0) {
        cout
            << "All key-value store tests passed.\n";

        return 0;
    }

    cerr
        << failure_count
        << " key-value store test(s) failed.\n";

    return 1;
}