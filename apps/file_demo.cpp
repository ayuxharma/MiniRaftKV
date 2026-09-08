#include "miniraft/file_service.hpp"

#include <cstdint>
#include <exception>
#include <iostream>
#include <string>
#include <vector>

using miniraft::FileMetadata;
using miniraft::FileService;
using miniraft::InMemoryCluster;
using miniraft::RaftCore;
using miniraft::to_string;

using std::cerr;
using std::cout;
using std::exception;
using std::string;
using std::uint64_t;
using std::vector;

int main(
    int argc,
    char* argv[]
) {
    // Expected command:
    //
    // ./miniraft_demo input.txt notes.txt restored.txt
    if (argc != 4) {
        cerr
            << "Usage: miniraft_demo "
            << "<input-path> "
            << "<stored-filename> "
            << "<output-path>\n";

        return 1;
    }

    const string input_path{
        argv[1]
    };

    const string stored_filename{
        argv[2]
    };

    const string output_path{
        argv[3]
    };

    try {
        const vector<string> node_ids{
            "node-1",
            "node-2",
            "node-3"
        };

        // Create three Raft nodes in one process.
        InMemoryCluster cluster{
            node_ids
        };

        // Trigger a deterministic election for this demo.
        static_cast<void>(
            cluster.start_election(
                "node-1"
            )
        );

        // File operations will be submitted through node-1.
        FileService service{
            cluster,
            "node-1"
        };

        const uint64_t version =
            service.upload_file(
                input_path,
                stored_filename
            );

        cout
            << "Uploaded "
            << stored_filename
            << " as version "
            << version
            << ".\n\n";

        cout
            << "Raft cluster after upload:\n";

        // Show that every node has applied the same metadata.
        for (const string& node_id : node_ids) {
            const RaftCore& node =
                cluster.node(node_id);

            const FileMetadata* metadata =
                node
                    .metadata_store()
                    .find(stored_filename);

            cout
                << "  "
                << node.node_id()
                << " role="
                << to_string(node.role())
                << " term="
                << node.current_term()
                << " commit_index="
                << node.commit_index()
                << " metadata_version="
                << (
                    metadata == nullptr
                        ? 0
                        : metadata->version
                )
                << '\n';
        }

        // Reconstruct the uploaded file from its blocks.
        service.download_file(
            stored_filename,
            output_path
        );

        cout
            << "\nRestored the file to "
            << output_path
            << ".\n";

        cout
            << "Unique blocks stored: "
            << service.block_store().size()
            << '\n';

        return 0;
    } catch (const exception& error) {
        cerr
            << "Demo failed: "
            << error.what()
            << '\n';

        return 1;
    }
}