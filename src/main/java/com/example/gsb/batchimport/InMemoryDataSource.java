package com.example.gsb.batchimport;

import java.util.List;
import java.util.Objects;

/**
 * Fake data source backed by an in-memory list. Shards are contiguous line-number ranges
 * and readers hold a cursor, so only one batch per shard is live at a time even though
 * the fixture data itself lives in memory.
 */
public final class InMemoryDataSource implements DataSource {

    private final List<String> values;

    public InMemoryDataSource(List<String> values) {
        this.values = List.copyOf(Objects.requireNonNull(values, "values"));
    }

    @Override
    public long size() {
        return values.size();
    }

    @Override
    public ShardReader openShard(int shardIndex, int shardCount, long checkpointLine) {
        if (shardCount < 1 || shardIndex < 0 || shardIndex >= shardCount) {
            throw new IllegalArgumentException("bad shard: index=" + shardIndex + ", count=" + shardCount);
        }
        long total = values.size();
        long base = total / shardCount;
        long remainder = total % shardCount;
        long startLine = 1 + shardIndex * base + Math.min(shardIndex, remainder);
        long endLine = startLine + base + (shardIndex < remainder ? 1 : 0); // exclusive
        long fromLine = Math.max(startLine, checkpointLine + 1);
        return new Cursor(values, fromLine, endLine);
    }

    private static final class Cursor implements ShardReader {

        private final List<String> values;
        private long nextLine;
        private final long endLineExclusive;

        Cursor(List<String> values, long nextLine, long endLineExclusive) {
            this.values = values;
            this.nextLine = nextLine;
            this.endLineExclusive = endLineExclusive;
        }

        @Override
        public List<Row> readBatch(int maxRows) {
            if (maxRows < 1) {
                throw new IllegalArgumentException("maxRows must be >= 1");
            }
            long remaining = endLineExclusive - nextLine;
            if (remaining <= 0) {
                return List.of();
            }
            int count = (int) Math.min(maxRows, remaining);
            java.util.List<Row> batch = new java.util.ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                long line = nextLine++;
                batch.add(new Row(line, values.get((int) line - 1)));
            }
            return batch;
        }

        @Override
        public void close() {
            // no resources to release
        }
    }
}
