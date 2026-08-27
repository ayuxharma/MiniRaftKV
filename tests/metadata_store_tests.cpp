#include "miniraft/metadata_store.hpp"

#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

using miniraft::FileMetadata;
using miniraft::MetadataStore;

using std::cerr;
using std::cout;
using std::invalid_argument;
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

void test_new_store_is_empty() {
    const MetadataStore store;

    expect(
        store.size() == 0,
        "New metadata store is empty"
    );

    expect(
        store.find("notes.txt") == nullptr,
        "Unknown filename is not found"
    );
}

void test_new_file_starts_at_version_one() {
    MetadataStore store;

    const bool accepted =
        store.apply(
            FileMetadata{
                "notes.txt",
                1,
                {
                    "hash-a",
                    "hash-b"
                },
                false
            }
        );

    expect(
        accepted,
        "New file accepts version one"
    );

    expect(
        store.size() == 1,
        "Accepted file is added to the store"
    );

    const FileMetadata* metadata =
        store.find("notes.txt");

    expect(
        metadata != nullptr,
        "Accepted file can be found"
    );

    if (metadata != nullptr) {
        expect(
            metadata->version == 1,
            "Stored file has version one"
        );

        expect(
            metadata->block_hashes.size() == 2,
            "Stored file keeps its ordered block hashes"
        );

        expect(
            !metadata->deleted,
            "New file is not a tombstone"
        );
    }
}

void test_new_file_rejects_wrong_version() {
    MetadataStore store;

    const bool accepted =
        store.apply(
            FileMetadata{
                "notes.txt",
                2,
                {
                    "hash-a"
                },
                false
            }
        );

    expect(
        !accepted,
        "New file rejects a version other than one"
    );

    expect(
        store.find("notes.txt") == nullptr,
        "Rejected new file does not change the store"
    );
}

void test_update_requires_next_version() {
    MetadataStore store;

    static_cast<void>(
        store.apply(
            FileMetadata{
                "notes.txt",
                1,
                {
                    "old-hash"
                },
                false
            }
        )
    );

    const bool skipped_version_accepted =
        store.apply(
            FileMetadata{
                "notes.txt",
                3,
                {
                    "new-hash"
                },
                false
            }
        );

    expect(
        !skipped_version_accepted,
        "Existing file rejects a skipped version"
    );

    const bool next_version_accepted =
        store.apply(
            FileMetadata{
                "notes.txt",
                2,
                {
                    "new-hash"
                },
                false
            }
        );

    expect(
        next_version_accepted,
        "Existing file accepts exactly the next version"
    );

    const FileMetadata* metadata =
        store.find("notes.txt");

    expect(
        metadata != nullptr &&
            metadata->version == 2,
        "Accepted update replaces the stored version"
    );

    expect(
        metadata != nullptr &&
            metadata->block_hashes.size() == 1 &&
            metadata->block_hashes[0] == "new-hash",
        "Accepted update replaces the old block hashes"
    );
}

void test_stale_version_does_not_replace_metadata() {
    MetadataStore store;

    static_cast<void>(
        store.apply(
            FileMetadata{
                "notes.txt",
                1,
                {
                    "original-hash"
                },
                false
            }
        )
    );

    const bool accepted =
        store.apply(
            FileMetadata{
                "notes.txt",
                1,
                {
                    "stale-hash"
                },
                false
            }
        );

    expect(
        !accepted,
        "Existing file rejects a repeated stale version"
    );

    const FileMetadata* metadata =
        store.find("notes.txt");

    expect(
        metadata != nullptr &&
            metadata->block_hashes[0] == "original-hash",
        "Rejected stale update preserves existing metadata"
    );
}

void test_delete_creates_tombstone() {
    MetadataStore store;

    static_cast<void>(
        store.apply(
            FileMetadata{
                "photo.jpg",
                1,
                {
                    "photo-hash"
                },
                false
            }
        )
    );

    const bool deleted =
        store.delete_file(
            "photo.jpg",
            2
        );

    expect(
        deleted,
        "Next file version may be a deletion"
    );

    const FileMetadata* metadata =
        store.find("photo.jpg");

    expect(
        metadata != nullptr &&
            metadata->version == 2,
        "Tombstone keeps the deletion version"
    );

    expect(
        metadata != nullptr &&
            metadata->deleted,
        "Deleted file is marked as a tombstone"
    );

    expect(
        metadata != nullptr &&
            metadata->block_hashes.empty(),
        "Tombstone contains no block hashes"
    );

    expect(
        store.size() == 1,
        "Deleted filename remains in metadata history"
    );
}

void test_deleted_file_can_be_recreated() {
    MetadataStore store;

    static_cast<void>(
        store.apply(
            FileMetadata{
                "notes.txt",
                1,
                {
                    "first-hash"
                },
                false
            }
        )
    );

    static_cast<void>(
        store.delete_file(
            "notes.txt",
            2
        )
    );

    const bool recreated =
        store.apply(
            FileMetadata{
                "notes.txt",
                3,
                {
                    "recreated-hash"
                },
                false
            }
        );

    expect(
        recreated,
        "Deleted file can be recreated with next version"
    );

    const FileMetadata* metadata =
        store.find("notes.txt");

    expect(
        metadata != nullptr &&
            metadata->version == 3 &&
            !metadata->deleted,
        "Recreated file replaces the tombstone"
    );
}

void test_invalid_metadata_is_rejected() {
    MetadataStore store;

    bool empty_filename_rejected = false;

    try {
        static_cast<void>(
            store.apply(
                FileMetadata{
                    "",
                    1,
                    {},
                    false
                }
            )
        );
    } catch (const invalid_argument&) {
        empty_filename_rejected = true;
    }

    expect(
        empty_filename_rejected,
        "Empty filename throws an exception"
    );

    bool zero_version_rejected = false;

    try {
        static_cast<void>(
            store.apply(
                FileMetadata{
                    "notes.txt",
                    0,
                    {},
                    false
                }
            )
        );
    } catch (const invalid_argument&) {
        zero_version_rejected = true;
    }

    expect(
        zero_version_rejected,
        "Version zero throws an exception"
    );

    bool empty_hash_rejected = false;

    try {
        static_cast<void>(
            store.apply(
                FileMetadata{
                    "notes.txt",
                    1,
                    {
                        ""
                    },
                    false
                }
            )
        );
    } catch (const invalid_argument&) {
        empty_hash_rejected = true;
    }

    expect(
        empty_hash_rejected,
        "Empty block hash throws an exception"
    );

    bool invalid_tombstone_rejected = false;

    try {
        static_cast<void>(
            store.apply(
                FileMetadata{
                    "notes.txt",
                    1,
                    {
                        "hash-a"
                    },
                    true
                }
            )
        );
    } catch (const invalid_argument&) {
        invalid_tombstone_rejected = true;
    }

    expect(
        invalid_tombstone_rejected,
        "Tombstone containing block hashes throws an exception"
    );
}

void test_same_updates_produce_same_state() {
    MetadataStore first_store;
    MetadataStore second_store;

    const vector<FileMetadata> updates{
        FileMetadata{
            "notes.txt",
            1,
            {
                "hash-a"
            },
            false
        },
        FileMetadata{
            "notes.txt",
            2,
            {
                "hash-b",
                "hash-c"
            },
            false
        }
    };

    for (const FileMetadata& update : updates) {
        static_cast<void>(
            first_store.apply(update)
        );

        static_cast<void>(
            second_store.apply(update)
        );
    }

    const FileMetadata* first =
        first_store.find("notes.txt");

    const FileMetadata* second =
        second_store.find("notes.txt");

    expect(
        first != nullptr &&
            second != nullptr &&
            first->version == second->version &&
            first->block_hashes == second->block_hashes &&
            first->deleted == second->deleted,
        "Same ordered updates produce the same metadata state"
    );
}

}  // namespace

int main() {
    test_new_store_is_empty();
    test_new_file_starts_at_version_one();
    test_new_file_rejects_wrong_version();
    test_update_requires_next_version();
    test_stale_version_does_not_replace_metadata();
    test_delete_creates_tombstone();
    test_deleted_file_can_be_recreated();
    test_invalid_metadata_is_rejected();
    test_same_updates_produce_same_state();

    if (failure_count == 0) {
        cout << "\nAll metadata store tests passed.\n";
        return 0;
    }

    cerr
        << "\n"
        << failure_count
        << " metadata store expectation(s) failed.\n";

    return 1;
}