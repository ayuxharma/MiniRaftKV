#include "miniraft/block_store.hpp"

#include <iostream>
#include <string>

using miniraft::Block;
using miniraft::BlockStore;

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

// Convert ordinary text into raw block bytes.
Block block_from_text(
    const string& text
) {
    return Block{
        text.begin(),
        text.end()
    };
}

void test_sha256_matches_known_value() {
    const Block block =
        block_from_text("abc");

    // This is the official known SHA-256 result for "abc".
    expect(
        BlockStore::hash_block(block) ==
            "ba7816bf8f01cfea414140de5dae2223"
            "b00361a396177a9cb410ff61f20015ad",
        "SHA-256 matches the known digest for abc"
    );
}

void test_put_and_get_block() {
    BlockStore store;

    const Block original =
        block_from_text("hello MiniRaft");

    const string hash =
        store.put(original);

    const Block* stored =
        store.get(hash);

    expect(
        store.contains(hash),
        "Stored block is found by its hash"
    );

    expect(
        stored != nullptr,
        "get returns the stored block"
    );

    expect(
        stored != nullptr &&
            *stored == original,
        "Retrieved bytes equal the original bytes"
    );
}

void test_identical_blocks_are_deduplicated() {
    BlockStore store;

    const Block block =
        block_from_text("same content");

    const string first_hash =
        store.put(block);

    const string second_hash =
        store.put(block);

    expect(
        first_hash == second_hash,
        "Equal blocks have equal hashes"
    );

    expect(
        store.size() == 1,
        "Equal blocks are stored only once"
    );
}

void test_different_blocks_have_different_hashes() {
    BlockStore store;

    const string first_hash =
        store.put(
            block_from_text("first")
        );

    const string second_hash =
        store.put(
            block_from_text("second")
        );

    expect(
        first_hash != second_hash,
        "Different blocks have different hashes"
    );

    expect(
        store.size() == 2,
        "Different blocks are both stored"
    );
}

void test_missing_hash_returns_nullptr() {
    const BlockStore store;

    expect(
        store.get("missing-hash") == nullptr,
        "Unknown hash does not return a block"
    );

    expect(
        !store.contains("missing-hash"),
        "Unknown hash is not reported as present"
    );
}

}  // namespace

int main() {
    test_sha256_matches_known_value();
    test_put_and_get_block();
    test_identical_blocks_are_deduplicated();
    test_different_blocks_have_different_hashes();
    test_missing_hash_returns_nullptr();

    if (failure_count == 0) {
        cout << "All block store tests passed.\n";
        return 0;
    }

    cerr << failure_count
         << " block store test(s) failed.\n";

    return 1;
}