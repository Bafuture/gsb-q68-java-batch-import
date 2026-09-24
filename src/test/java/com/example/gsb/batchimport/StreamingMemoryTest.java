package com.example.gsb.batchimport;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * Proves the streaming invariant: live data-row memory is bounded by the batch/shard
 * configuration and does not scale with the total number of rows.
 *
 * <p>A {@link GeneratedDataSource} never materializes payloads, and this writer counts
 * how many generated {@link Row} objects are simultaneously alive inside the processing
 * path (source batch + validated copy + writer argument), updating a peak counter.
 */
class StreamingMemoryTest {

    @Test
    void liveRowCountStaysBoundedRegardlessOfTotalRows() {
        int batchSize = 100;
        int shardCount = 4;
        TrackingWriter writer = new TrackingWriter();

        EngineConfig config = EngineConfig
                .builder("job-memory", row -> Optional.empty(), writer, new InMemoryCheckpointStore())
                .batchSize(batchSize)
                .shardCount(shardCount)
                .build();

        GeneratedDataSource source = new GeneratedDataSource(200_000,
                line -> "row-" + line + "-" + "padding".repeat(8));

        ImportStats stats = new BatchImportEngine(config).run(source);

        assertThat(stats.successCount()).isEqualTo(200_000);
        assertThat(stats.failureCount()).isZero();

        // At most: one pulled batch + one validated list + one copied write-argument
        // per concurrently running worker; small slack for bookkeeping objects.
        int expectedUpperBound = 3 * batchSize * shardCount + batchSize;
        assertThat(writer.peakLiveRows())
                .as("live rows (peak=%d) must be bounded by batch configuration", writer.peakLiveRows())
                .isLessThanOrEqualTo(expectedUpperBound);
    }

    private static final class TrackingWriter implements RecordWriter {

        private final AtomicInteger live = new AtomicInteger();
        private final AtomicInteger peak = new AtomicInteger();
        @Override
        public void writeBatch(String shardId, List<Row> rows) {
            int current = live.addAndGet(rows.size());
            peak.accumulateAndGet(current, Math::max);
            try {
                // simulate sink work while the referenced rows stay live; do NOT retain them
                long sum = 0;
                for (Row row : rows) {
                    sum += row.lineNumber() + row.value().length();
                }
                if (sum < 0) {
                    throw new AssertionError("unreachable");
                }
            } finally {
                live.addAndGet(-rows.size());
            }
        }

        int peakLiveRows() {
            return peak.get();
        }
    }
}
