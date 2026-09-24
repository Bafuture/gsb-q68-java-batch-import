package com.example.gsb.batchimport;

/**
 * Persists one progress position per (job, shard). The value is the last line number that
 * was fully handled (written if valid, rejected if invalid).
 */
public interface CheckpointStore {

    long load(String jobId, String shardId);

    void save(String jobId, String shardId, long lineNumber);
}
