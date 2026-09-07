#pragma once

#include "miniraft/block_store.hpp"

#include <cstddef>
#include <string>
#include <vector>

namespace miniraft {

using std::size_t;
using std::string;
using std::vector;

class FileChunker {
public:
    static constexpr size_t default_block_size = 4096;

    [[nodiscard]] static vector<string> store_file(
        const string& file_path,
        BlockStore& block_store,
        size_t block_size = default_block_size
    );

    static void restore_file(
        const vector<string>& block_hashes,
        const BlockStore& block_store,
        const string& output_file_path
    );
};

}