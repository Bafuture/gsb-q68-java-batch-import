package com.example.gsb.batchimport;

/**
 * A source of rows that can be split into disjoint, contiguous shards.
 *
 * <p>Each shard is an independent streaming cursor with its own checkpoint, which allows
 * one worker thread per shard while keeping shard-internal ordering.
 */
public interface DataSource {

    /** Total number of rows (needed to balance contiguous shard ranges). */
    long size();

    /**
     * Opens the shard {@code shardIndex} of {@code shardCount}, positioned so that the
     * first row returned is strictly after {@code checkpointLine} (0 means start at the
     * shard's first row).
     */
    ShardReader openShard(int shardIndex, int shardCount, long checkpointLine);
}
