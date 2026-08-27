#include "miniraft/metadata_store.hpp"

#include <stdexcept>

namespace miniraft {

using std::invalid_argument;

void MetadataStore::validate(
    const FileMetadata& metadata
) {
    if (metadata.filename.empty()) {
        throw invalid_argument{
            "Metadata filename cannot be empty"
        };
    }

    if (metadata.version == 0) {
        throw invalid_argument{
            "Metadata version must be greater than zero"
        };
    }

    // A tombstone describes deletion, so it must not refer to file-content blocks.
    if (
        metadata.deleted &&
        !metadata.block_hashes.empty()
    ) {
        throw invalid_argument{
            "Deleted metadata cannot contain block hashes"
        };
    }

    for (const string& block_hash : metadata.block_hashes) {
        if (block_hash.empty()) {
            throw invalid_argument{
                "A block hash cannot be empty"
            };
        }
    }
}

bool MetadataStore::apply(
    const FileMetadata& metadata
) {
    validate(metadata);

    auto iterator =
        files_.find(metadata.filename);

    if (iterator == files_.end()) {
        // The first version of a new file must always be one.
        if (metadata.version != 1) {
            return false;
        }

        files_.emplace(
            metadata.filename,
            metadata
        );

        return true;
    }

    const uint64_t expected_version =
        iterator->second.version + 1;

    // Reject stale versions and skipped versions.
    if (metadata.version != expected_version) {
        return false;
    }

    // Replace the old metadata with the new accepted version.
    iterator->second = metadata;

    return true;
}

bool MetadataStore::delete_file(
    const string& filename,
    const uint64_t version
) {
    // A deletion is stored as normal metadata with the tombstone flag set and no block hashes.
    return apply(
        FileMetadata{
            filename,
            version,
            {},
            true
        }
    );
}

const FileMetadata* MetadataStore::find(
    const string& filename
) const {
    const auto iterator =
        files_.find(filename);

    if (iterator == files_.end()) {
        return nullptr;
    }

    // Return a read-only pointer to the stored metadata.
    return &iterator->second;
}

size_t MetadataStore::size() const {
    return files_.size();
}

}  // namespace miniraft