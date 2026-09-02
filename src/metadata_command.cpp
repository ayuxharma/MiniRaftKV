#include "miniraft/metadata_command.hpp"

#include <cstddef>
#include <stdexcept>
#include <string>
#include <vector>

namespace miniraft {

using std::invalid_argument;
using std::out_of_range;
using std::size_t;
using std::stoull;
using std::to_string;
using std::vector;

namespace {

// Split a string while preserving an empty final field.
//
// For example:
//
// "META|DELETE|notes.txt|2|"
//
// becomes:
//
// ["META", "DELETE", "notes.txt", "2", ""]
vector<string> split(
    const string& text,
    const char separator
) {
    vector<string> parts;

    size_t start = 0;

    while (true) {
        const size_t separator_position =
            text.find(separator, start);

        if (separator_position == string::npos) {
            parts.push_back(
                text.substr(start)
            );

            break;
        }

        parts.push_back(
            text.substr(
                start,
                separator_position - start
            )
        );

        start = separator_position + 1;
    }

    return parts;
}

uint64_t parse_version(
    const string& text
) {
    if (text.empty()) {
        throw invalid_argument{
            "Metadata command version cannot be empty"
        };
    }

    size_t consumed_characters = 0;
    uint64_t version = 0;

    try {
        version = stoull(
            text,
            &consumed_characters
        );
    } catch (const invalid_argument&) {
        throw invalid_argument{
            "Metadata command contains an invalid version"
        };
    } catch (const out_of_range&) {
        throw invalid_argument{
            "Metadata command version is too large"
        };
    }

    if (
        consumed_characters != text.size() ||
        version == 0
    ) {
        throw invalid_argument{
            "Metadata command contains an invalid version"
        };
    }

    return version;
}

vector<string> parse_block_hashes(
    const string& encoded_hashes
) {
    if (encoded_hashes.empty()) {
        return {};
    }

    vector<string> block_hashes =
        split(encoded_hashes, ',');

    for (const string& block_hash : block_hashes) {
        if (block_hash.empty()) {
            throw invalid_argument{
                "Metadata command contains an empty block hash"
            };
        }
    }

    return block_hashes;
}

}  // namespace

bool is_metadata_command(
    const string& command
) {
    // Position zero means that the command begins with "META|".
    return command.rfind("META|", 0) == 0;
}

string encode_metadata_command(
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

    // The simple learning format uses | between fields.
    if (
        metadata.filename.find('|') !=
        string::npos
    ) {
        throw invalid_argument{
            "Metadata filename cannot contain |"
        };
    }

    if (
        metadata.deleted &&
        !metadata.block_hashes.empty()
    ) {
        throw invalid_argument{
            "Deleted metadata cannot contain block hashes"
        };
    }

    string command = "META|";

    command +=
        metadata.deleted
            ? "DELETE|"
            : "UPSERT|";

    command += metadata.filename;
    command += '|';
    command += to_string(metadata.version);
    command += '|';

    for (
        size_t index = 0;
        index < metadata.block_hashes.size();
        ++index
    ) {
        const string& block_hash =
            metadata.block_hashes[index];

        if (block_hash.empty()) {
            throw invalid_argument{
                "A block hash cannot be empty"
            };
        }

        if (
            block_hash.find(',') != string::npos ||
            block_hash.find('|') != string::npos
        ) {
            throw invalid_argument{
                "Block hash contains a reserved separator"
            };
        }

        if (index > 0) {
            command += ',';
        }

        command += block_hash;
    }

    return command;
}

FileMetadata decode_metadata_command(
    const string& command
) {
    if (!is_metadata_command(command)) {
        throw invalid_argument{
            "Command is not a metadata command"
        };
    }

    const vector<string> fields =
        split(command, '|');

    // Expected fields:
    //
    // 0: META
    // 1: UPSERT or DELETE
    // 2: filename
    // 3: version
    // 4: comma-separated hashes
    if (fields.size() != 5) {
        throw invalid_argument{
            "Metadata command must contain exactly five fields"
        };
    }

    if (fields[2].empty()) {
        throw invalid_argument{
            "Metadata command filename cannot be empty"
        };
    }

    const uint64_t version =
        parse_version(fields[3]);

    if (fields[1] == "DELETE") {
        if (!fields[4].empty()) {
            throw invalid_argument{
                "Delete command cannot contain block hashes"
            };
        }

        return FileMetadata{
            fields[2],
            version,
            {},
            true
        };
    }

    if (fields[1] == "UPSERT") {
        return FileMetadata{
            fields[2],
            version,
            parse_block_hashes(fields[4]),
            false
        };
    }

    throw invalid_argument{
        "Metadata command has an unknown operation"
    };
}

}  // namespace miniraft