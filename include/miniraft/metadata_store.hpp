#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

namespace miniraft
{
using std::size_t;
using std::string;
using std::uint64_t;
using std::unordered_map;
using std::vector; 

// metadata describing one synchronized file. the file contents are not stored here. block hashes identify the pieces that will later be stored in blockstore
struct FileMetadata {

    string filename ;
    uint64_t version{0} ;
    vector<string> block_hashes ;
    bool deleted{false} ;
} ;

// deterministic state machine containing the latest metadata for every known file
class MetadataStore {
public:
    // apply a new file version, returns true when version is accepted and false when version is stale or skips a version
    [[nodiscard]] bool apply (const FileMetadata& metadata) ;

    // apply a deletion tombstone for an existing file
    [[nodiscard]] bool delete_file (
        const string& filename ,
        uint64_t version 
    ) ;

    // find latest metdata for one file, returns nullptr when file has never been recorded
    [[nodiscard]] const FileMetadata* find (
        const string& filename 
    ) const ;

    // Return the number of filenames known to this store.
    // Tombstones are included because they remain part of the state.
    [[nodiscard]] size_t size() const;

private:
    // Reject structurally invalid metadata before checking versions.
    static void validate(
        const FileMetadata& metadata
    );

    // Each filename maps to its latest accepted metadata.
    unordered_map<string, FileMetadata> files_;
};

}  // namespace miniraft