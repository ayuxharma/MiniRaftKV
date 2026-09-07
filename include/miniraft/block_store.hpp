#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

namespace miniraft {

using std::size_t;
using std::string;
using std::uint8_t;
using std::unordered_map;
using std::vector;

using Block = vector<uint8_t>; // A block is a small piece of a file represented as raw bytes.

class BlockStore {

public:
    [[nodiscard]] string put(const Block& block);
    [[nodiscard]] const Block* get(const string& hash) const;
    [[nodiscard]] bool contains(const string& hash) const;
    [[nodiscard]] size_t size() const;
    [[nodiscard]] static string hash_block(const Block& block);

private:
    unordered_map<string, Block> blocks_;
};

} // namespace miniraft