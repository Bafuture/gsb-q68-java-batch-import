package com.example.gsb.batchimport;

import java.util.List;

/**
 * Streaming cursor over one shard of a {@link DataSource}. Rows are pulled in batches so
 * the whole shard never has to reside in memory.
 */
public interface ShardReader extends AutoCloseable {

    /**
     * Reads up to {@code maxRows} rows after the reader's current position.
     *
     * @return the next rows, or an empty list when the shard is exhausted.
     */
    List<Row> readBatch(int maxRows);

    @Override
    void close();
}
