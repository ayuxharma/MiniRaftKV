#include "miniraft/file_chunker.hpp"

#include <cstdio>
#include <fstream>
#include <iostream>
#include <iterator>
#include <stdexcept>
#include <string>
#include <vector>

using miniraft::BlockStore;
using miniraft::FileChunker;

using std::cerr;
using std::cout;
using std::ifstream;
using std::ios;
using std::istreambuf_iterator;
using std::ofstream;
using std::remove;
using std::runtime_error;
using std::streamsize;
using std::string;
using std::vector;

namespace {

int failure_count = 0;

const string input_path =
    "miniraft_chunker_input.test";

const string output_path =
    "miniraft_chunker_output.test";

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

void remove_test_files() {
    // remove returns an error code if the file does not exist.
    // That is acceptable during test cleanup.
    static_cast<void>(
        remove(input_path.c_str())
    );

    static_cast<void>(
        remove(output_path.c_str())
    );
}

void write_test_file(
    const string& contents
) {
    ofstream output{
        input_path,
        ios::binary | ios::trunc
    };

    if (!output) {
        throw runtime_error{
            "Could not create test input file"
        };
    }

    output.write(
        contents.data(),
        static_cast<streamsize>(
            contents.size()
        )
    );
}

string read_test_file(
    const string& path
) {
    ifstream input{
        path,
        ios::binary
    };

    if (!input) {
        throw runtime_error{
            "Could not read test output file"
        };
    }

    return string{
        istreambuf_iterator<char>{input},
        istreambuf_iterator<char>{}
    };
}

void test_file_is_split_and_restored() {
    remove_test_files();

    write_test_file(
        "abcdefghij"
    );

    BlockStore store;

    // Ten bytes with a block size of four produces:
    // "abcd", "efgh", and "ij".
    const vector<string> hashes =
        FileChunker::store_file(
            input_path,
            store,
            4
        );

    expect(
        hashes.size() == 3,
        "Ten bytes become three four-byte blocks"
    );

    expect(
        store.size() == 3,
        "Three unique blocks are stored"
    );

    FileChunker::restore_file(
        hashes,
        store,
        output_path
    );

    expect(
        read_test_file(output_path) ==
            "abcdefghij",
        "Restored file exactly matches the original"
    );

    remove_test_files();
}

void test_repeated_blocks_are_deduplicated() {
    remove_test_files();

    write_test_file(
        "abcdabcd"
    );

    BlockStore store;

    const vector<string> hashes =
        FileChunker::store_file(
            input_path,
            store,
            4
        );

    expect(
        hashes.size() == 2,
        "Both block positions are recorded"
    );

    expect(
        hashes[0] == hashes[1],
        "Equal blocks use the same hash"
    );

    expect(
        store.size() == 1,
        "Equal blocks occupy one store entry"
    );

    remove_test_files();
}

void test_empty_file_can_be_restored() {
    remove_test_files();

    write_test_file("");

    BlockStore store;

    const vector<string> hashes =
        FileChunker::store_file(
            input_path,
            store
        );

    expect(
        hashes.empty(),
        "Empty file has no blocks"
    );

    FileChunker::restore_file(
        hashes,
        store,
        output_path
    );

    expect(
        read_test_file(output_path).empty(),
        "Empty file is restored"
    );

    remove_test_files();
}

void test_missing_block_prevents_restore() {
    remove_test_files();

    const BlockStore store;
    bool exception_was_thrown = false;

    try {
        FileChunker::restore_file(
            vector<string>{
                "missing-hash"
            },
            store,
            output_path
        );
    } catch (const runtime_error&) {
        exception_was_thrown = true;
    }

    expect(
        exception_was_thrown,
        "Missing block prevents file restoration"
    );

    remove_test_files();
}

}  // namespace

int main() {
    test_file_is_split_and_restored();
    test_repeated_blocks_are_deduplicated();
    test_empty_file_can_be_restored();
    test_missing_block_prevents_restore();

    remove_test_files();

    if (failure_count == 0) {
        cout << "All file chunker tests passed.\n";
        return 0;
    }

    cerr << failure_count
         << " file chunker test(s) failed.\n";

    return 1;
}