#pragma once
#include "miniraft/metadata_store.hpp"
#include <string>

namespace miniraft {

using std::string ;

[[nodiscard]] bool is_metadata_command(const string& command);
[[nodiscard]] string encode_metadata_command(const FileMetadata& command);
[[nodiscard]] FileMetadata decode_metadata_command(const string& command);
} // namespace miniraft