package com.example.gsb.batchimport;

import java.util.ArrayList;
import java.util.List;
import java.util.function.LongFunction;

/**
 * Streaming fake data source that does not hold any row payload in memory: the value for
 * a line is produced on demand by a generator function. This models a file/cursor reader
 * and is used to prove that the engine's live memory does not grow with total rows.
 */
public final class GeneratedDataSource implements DataSource {

    private final long totalRows;
    private final LongFunction<String> generator;

    public GeneratedDataSource(long totalRows, LongFunction<String> generator) {
        if (totalRows < 0) {
            throw new IllegalArgumentException("totalRows must be >= 0");
        }
        this.totalRows = totalRows;
        this.generator = java.util.Objects.requireNonNull(generator, "generator");
    }

    @Override
    public long size() {
        return totalRows;
    }

    @Override
    public ShardReader openShard(int shardIndex, int shardCount, long checkpointLine) {
        if (shardCount < 1 || shardIndex < 0 || shardIndex >= shardCount) {
            throw new IllegalArgumentException("bad shard: index=" + shardIndex + ", count=" + shardCount);
        }
        long base = totalRows / shardCount;
        long remainder = totalRows % shardCount;
        long startLine = 1 + shardIndex * base + Math.min(shardIndex, remainder);
        long endLine = startLine + base + (shardIndex < remainder ? 1 : 0);
        long fromLine = Math.max(startLine, checkpointLine + 1);
        return new Cursor(fromLine, endLine);
    }

    private final class Cursor implements ShardReader {

        private long nextLine;
        private final long endLineExclusive;

        Cursor(long nextLine, long endLineExclusive) {
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
            List<Row> batch = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                long line = nextLine++;
                batch.add(new Row(line, generator.apply(line)));
            }
            return batch;
        }

        @Override
        public void close() {
            // no resources to release
        }
    }
}
