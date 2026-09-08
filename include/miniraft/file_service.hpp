#pragma once

#include "miniraft/block_store.hpp"
#include "miniraft/in_memory_cluster.hpp"

#include <cstdint>
#include <string>

namespace miniraft {

using std::string;
using std::uint64_t;

// connect file blocks to metadata replicated by raft
class FileService{
public:
    FileService(InMemoryCluster& cluster, const string& leader_node_id);

    [[nodiscard]] uint64_t upload_file(const string& local_path, const string& filename);
    
    void download_file(const string& filename, const string& output_path) const;

    [[nodiscard]] uint64_t delete_file(const string& filename);
    
    [[nodiscard]] const BlockStore& block_store() const;


private:
    [[nodiscard]] RaftCore& leader();

    [[nodiscard]] const RaftCore& leader() const;

    void replicate_and_propagate_commit();

    InMemoryCluster& cluster_;
    string leader_node_id_;
    BlockStore block_store_;
};

}