package com.example.gsb.batchimport;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Checkpoint store for tests and short-lived runs; state dies with the JVM. */
public final class InMemoryCheckpointStore implements CheckpointStore {

    private final Map<String, Long> positions = new ConcurrentHashMap<>();

    @Override
    public long load(String jobId, String shardId) {
        return positions.getOrDefault(key(jobId, shardId), 0L);
    }

    @Override
    public void save(String jobId, String shardId, long lineNumber) {
        positions.put(key(jobId, shardId), lineNumber);
    }

    private static String key(String jobId, String shardId) {
        return jobId + "#" + shardId;
    }
}
