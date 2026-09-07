#include "miniraft/file_chunker.hpp"

#include <fstream>
#include <stdexcept>

namespace miniraft {

using std::ifstream;
using std::invalid_argument;
using std::ios;
using std::ofstream;
using std::runtime_error;
using std::streamsize;

vector<string> FileChunker::store_file(
    const string& file_path,
    BlockStore& block_store,
    const size_t block_size
) {
    if (block_size == 0) {
        throw invalid_argument{
            "Block size must be greater than zero"
        };
    }

    // Binary mode is required because files may contain bytes
    // that are not ordinary text characters.
    ifstream input{
        file_path,
        ios::binary
    };

    if (!input.is_open()) {
        throw runtime_error{
            "Could not open input file: " + file_path
        };
    }

    vector<string> block_hashes;

    // Reuse one buffer while reading the file.
    Block buffer(block_size);

    while (true) {
        input.read(
            reinterpret_cast<char*>(buffer.data()),
            static_cast<streamsize>(block_size)
        );

        const streamsize bytes_read =
            input.gcount();

        if (bytes_read > 0) {
            // The final block may be smaller than block_size.
            buffer.resize(
                static_cast<size_t>(bytes_read)
            );

            block_hashes.push_back(
                block_store.put(buffer)
            );

            // Restore the buffer before reading another block.
            buffer.resize(block_size);
        }

        if (input.eof()) {
            break;
        }

        if (!input) {
            throw runtime_error{
                "Could not read input file: " + file_path
            };
        }
    }

    return block_hashes;
}

void FileChunker::restore_file(
    const vector<string>& block_hashes,
    const BlockStore& block_store,
    const string& output_path
) {
    // trunc removes any previous contents of the output file.
    ofstream output{
        output_path,
        ios::binary | ios::trunc
    };

    if (!output.is_open()) {
        throw runtime_error{
            "Could not open output file: " + output_path
        };
    }

    // The hash order is the original file-block order.
    for (const string& hash : block_hashes) {
        const Block* block =
            block_store.get(hash);

        if (block == nullptr) {
            throw runtime_error{
                "Cannot restore file because block is missing: " +
                hash
            };
        }

        output.write(
            reinterpret_cast<const char*>(
                block->data()
            ),
            static_cast<streamsize>(
                block->size()
            )
        );

        if (!output) {
            throw runtime_error{
                "Could not write output file: " +
                output_path
            };
        }
    }
}

}  // namespace miniraft