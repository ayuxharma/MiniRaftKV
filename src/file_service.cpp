#include "miniraft/file_service.hpp"

#include "miniraft/file_chunker.hpp"

#include <stdexcept>

namespace miniraft {

using std::invalid_argument;
using std::logic_error;
using std::runtime_error;

FileService::FileService(
    InMemoryCluster& cluster,
    const string& leader_node_id
)
    : cluster_{cluster},
      leader_node_id_{leader_node_id} {
    if (leader_node_id_.empty()) {
        throw invalid_argument{
            "Leader node ID cannot be empty"
        };
    }
}

RaftCore& FileService::leader() {
    RaftCore& selected_node =
        cluster_.node(leader_node_id_);

    if (selected_node.role() != NodeRole::leader) {
        throw logic_error{
            "Configured file-service node is not the Raft leader"
        };
    }

    return selected_node;
}

const RaftCore& FileService::leader() const {
    const RaftCore& selected_node =
        cluster_.node(leader_node_id_);

    if (selected_node.role() != NodeRole::leader) {
        throw logic_error{
            "Configured file-service node is not the Raft leader"
        };
    }

    return selected_node;
}

void FileService::replicate_and_propagate_commit() {
    RaftCore& selected_leader =
        leader();

    // Moving next_index backward may require multiple rounds.
    const size_t maximum_rounds =
        static_cast<size_t>(
            selected_leader.last_log_index() + 1
        );

    const bool replicated =
        cluster_.replicate_until_caught_up(
            leader_node_id_,
            maximum_rounds
        );

    if (!replicated) {
        throw runtime_error{
            "File metadata could not be replicated"
        };
    }

    // The leader may commit during replication.
    // This fresh heartbeat sends the new commit index
    // to every follower.
    static_cast<void>(
        cluster_.send_heartbeats(
            leader_node_id_
        )
    );
}

uint64_t FileService::upload_file(
    const string& local_path,
    const string& filename
) {
    if (filename.empty()) {
        throw invalid_argument{
            "Filename cannot be empty"
        };
    }

    RaftCore& selected_leader =
        leader();

    const FileMetadata* current =
        selected_leader
            .metadata_store()
            .find(filename);

    // New files begin at version one.
    // Existing files move to the next version.
    const uint64_t next_version =
        current == nullptr
            ? 1
            : current->version + 1;

    // Store the actual file contents as blocks.
    const vector<string> block_hashes =
        FileChunker::store_file(
            local_path,
            block_store_
        );

    // Only the hashes and version are placed in the Raft log.
    static_cast<void>(
        selected_leader.append_metadata(
            FileMetadata{
                filename,
                next_version,
                block_hashes,
                false
            }
        )
    );

    replicate_and_propagate_commit();

    return next_version;
}

void FileService::download_file(
    const string& filename,
    const string& output_path
) const {
    const FileMetadata* metadata =
        leader()
            .metadata_store()
            .find(filename);

    if (metadata == nullptr) {
        throw runtime_error{
            "File metadata does not exist: " +
            filename
        };
    }

    if (metadata->deleted) {
        throw runtime_error{
            "File has been deleted: " +
            filename
        };
    }

    // Use the ordered block hashes to reconstruct the file.
    FileChunker::restore_file(
        metadata->block_hashes,
        block_store_,
        output_path
    );
}

uint64_t FileService::delete_file(
    const string& filename
) {
    RaftCore& selected_leader =
        leader();

    const FileMetadata* current =
        selected_leader
            .metadata_store()
            .find(filename);

    if (
        current == nullptr ||
        current->deleted
    ) {
        throw runtime_error{
            "Cannot delete a file that does not exist: " +
            filename
        };
    }

    const uint64_t next_version =
        current->version + 1;

    // A tombstone has no block hashes and deleted is true.
    static_cast<void>(
        selected_leader.append_metadata(
            FileMetadata{
                filename,
                next_version,
                {},
                true
            }
        )
    );

    replicate_and_propagate_commit();

    return next_version;
}

const BlockStore& FileService::block_store() const {
    return block_store_;
}

}  // namespace miniraft