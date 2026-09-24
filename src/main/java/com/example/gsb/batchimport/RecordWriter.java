package com.example.gsb.batchimport;

import java.util.List;

/**
 * Sink for validated rows. A real implementation would execute a batched INSERT/UPSERT
 * against a database; the test implementation keeps rows in memory.
 *
 * <p>{@code writeBatch} is expected to be idempotent per row key: a batch may be
 * delivered again after a crash that happened after the write but before the checkpoint
 * was persisted. Implementations must therefore use an upsert / dedup strategy.
 *
 * <p>Implementations must be thread-safe: batches from different shards may be written
 * concurrently.
 */
public interface RecordWriter {

    /**
     * @return key identifying the row for idempotent writes; default is the line number.
     */
    default String keyOf(Row row) {
        return Long.toString(row.lineNumber());
    }

    void writeBatch(String shardId, List<Row> rows);
}
