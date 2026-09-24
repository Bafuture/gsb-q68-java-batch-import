package com.example.gsb.batchimport;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Fake writer simulating a database table backed by an idempotent upsert.
 *
 * <p>Rows are deduped by {@link RecordWriter#keyOf(Row)} (line number by default), so a
 * redelivered batch after a crash never produces a duplicate row. It also tracks the
 * number of duplicate write attempts for assertions and diagnostics.
 */
public final class InMemoryWriter implements RecordWriter {

    private final Map<String, Row> stored = new ConcurrentHashMap<>();
    private final java.util.concurrent.atomic.AtomicLong duplicateAttempts =
            new java.util.concurrent.atomic.AtomicLong();

    @Override
    public void writeBatch(String shardId, List<Row> rows) {
        for (Row row : rows) {
            Row previous = stored.putIfAbsent(keyOf(row), row);
            if (previous != null) {
                duplicateAttempts.incrementAndGet();
            }
        }
    }

    /** All written rows, ordered by line number. */
    public List<Row> writtenRows() {
        List<Row> rows = new ArrayList<>(stored.values());
        rows.sort(java.util.Comparator.comparingLong(Row::lineNumber));
        return rows;
    }

    public Set<String> writtenKeys() {
        return Set.copyOf(stored.keySet());
    }

    public long writtenCount() {
        return stored.size();
    }

    public long duplicateAttempts() {
        return duplicateAttempts.get();
    }
}
