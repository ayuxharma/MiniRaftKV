#include "miniraft/file_service.hpp"

#include <cstdio>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <iterator>
#include <stdexcept>
#include <string>

using miniraft::FileMetadata;
using miniraft::FileService;
using miniraft::InMemoryCluster;

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
using std::uint64_t;

namespace {

int failure_count = 0;

const string input_path =
    "miniraft_service_input.test";

const string output_path =
    "miniraft_service_output.test";

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
    static_cast<void>(
        remove(input_path.c_str())
    );

    static_cast<void>(
        remove(output_path.c_str())
    );
}

void write_file(
    const string& contents
) {
    ofstream output{
        input_path,
        ios::binary | ios::trunc
    };

    if (!output) {
        throw runtime_error{
            "Could not create test file"
        };
    }

    output.write(
        contents.data(),
        static_cast<streamsize>(
            contents.size()
        )
    );
}

string read_file(
    const string& path
) {
    ifstream input{
        path,
        ios::binary
    };

    if (!input) {
        throw runtime_error{
            "Could not read test file"
        };
    }

    return string{
        istreambuf_iterator<char>{input},
        istreambuf_iterator<char>{}
    };
}

void test_upload_download_and_delete() {
    remove_test_files();

    InMemoryCluster cluster{
        {
            "node-1",
            "node-2",
            "node-3"
        }
    };

    // Elect node-1 before accepting file operations.
    static_cast<void>(
        cluster.start_election("node-1")
    );

    FileService service{
        cluster,
        "node-1"
    };

    write_file(
        "MiniRaft stores this file"
    );

    const uint64_t uploaded_version =
        service.upload_file(
            input_path,
            "notes.txt"
        );

    expect(
        uploaded_version == 1,
        "First upload creates version one"
    );

    expect(
        service.block_store().size() == 1,
        "File content is stored as a block"
    );

    // Verify that the committed metadata reached every node.
    for (
        const string& node_id :
        {"node-1", "node-2", "node-3"}
    ) {
        const FileMetadata* metadata =
            cluster
                .node(node_id)
                .metadata_store()
                .find("notes.txt");

        expect(
            metadata != nullptr &&
                metadata->version == 1,
            "Committed metadata reaches " +
                node_id
        );
    }

    service.download_file(
        "notes.txt",
        output_path
    );

    expect(
        read_file(output_path) ==
            "MiniRaft stores this file",
        "Downloaded file matches uploaded file"
    );

    const uint64_t deleted_version =
        service.delete_file(
            "notes.txt"
        );

    expect(
        deleted_version == 2,
        "Deletion creates the next version"
    );

    const FileMetadata* deleted_metadata =
        cluster
            .node("node-3")
            .metadata_store()
            .find("notes.txt");

    expect(
        deleted_metadata != nullptr &&
            deleted_metadata->deleted,
        "Deletion tombstone reaches followers"
    );

    remove_test_files();
}

}  // namespace

int main() {
    test_upload_download_and_delete();

    remove_test_files();

    if (failure_count == 0) {
        cout << "All file service tests passed.\n";
        return 0;
    }

    cerr << failure_count
         << " file service test(s) failed.\n";

    return 1;
}