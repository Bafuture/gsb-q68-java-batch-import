package com.example.gsb.batchimport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.LongStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CheckpointResumeTest {

    private static final RowValidator REJECT_EVERY_TENTH = row ->
            row.lineNumber() % 10 == 0 ? Optional.of("rejected every tenth line") : Optional.empty();

    private static InMemoryDataSource source(long n) {
        return new InMemoryDataSource(new java.util.ArrayList<>(
                LongStream.rangeClosed(1, n).mapToObj("v%d"::formatted).toList()));
    }

    @Test
    void resumesFromPreSeededCheckpointWithoutRewritingEarlierLines() {
        InMemoryCheckpointStore checkpoints = new InMemoryCheckpointStore();
        checkpoints.save("job-preseed", "shard-0", 50);
        InMemoryWriter writer = new InMemoryWriter();

        EngineConfig config = EngineConfig
                .builder("job-preseed", REJECT_EVERY_TENTH, writer, checkpoints)
                .batchSize(10)
                .shardCount(1)
                .build();

        ImportStats stats = new BatchImportEngine(config).run(source(100));

        assertThat(stats.totalRows()).isEqualTo(50);
        assertThat(writer.writtenRows())
                .extracting(Row::lineNumber)
                .allMatch(line -> line > 50 && line <= 100)
                .hasSize(45); // 51..100 minus 60,70,80,90,100
        assertThat(checkpoints.load("job-preseed", "shard-0")).isEqualTo(100);
    }

    @Test
    void cooperativeStopThenResumeWritesEveryRowExactlyOnce() {
        long total = 10_000;
        InMemoryCheckpointStore checkpoints = new InMemoryCheckpointStore();
        InMemoryWriter writer = new InMemoryWriter();
        List<FailureDetail> allFailures = new ArrayList<>();

        BatchImportEngine engine1 = engine(
                "job-stop", writer, checkpoints, 250, 4, 1_234L);
        ImportStats stats1 = engine1.run(source(total));
        allFailures.addAll(stats1.failures());
        assertThat(stats1.totalRows()).isLessThan(total);
        assertThat(writer.duplicateAttempts()).isZero();

        BatchImportEngine engine2 = engine("job-stop", writer, checkpoints, 250, 4, null);
        ImportStats stats2 = engine2.run(source(total));
        allFailures.addAll(stats2.failures());

        // strict exactly-once for rows and for failure details after a clean stop
        assertThat(stats1.totalRows() + stats2.totalRows()).isEqualTo(total);
        assertThat(stats1.successCount() + stats2.successCount()).isEqualTo(total - total / 10);
        assertThat(stats1.failureCount() + stats2.failureCount()).isEqualTo(total / 10);
        assertThat(writer.writtenCount()).isEqualTo(total - total / 10);
        assertThat(writer.writtenRows())
                .extracting(Row::lineNumber)
                .doesNotHaveDuplicates()
                .noneMatch(line -> line % 10 == 0);
        assertThat(allFailures)
                .extracting(FailureDetail::lineNumber)
                .doesNotHaveDuplicates()
                .hasSize((int) (total / 10));
        for (int shard = 0; shard < 4; shard++) {
            assertThat(checkpoints.load("job-stop", "shard-" + shard)).isPositive();
        }
    }

    @Test
    void hardCrashMidBatchReplaysBatchButIdempotentWriterDeduplicates() {
        long total = 1_000;
        InMemoryCheckpointStore checkpoints = new InMemoryCheckpointStore();
        PartialCrashWriter writer = new PartialCrashWriter();

        EngineConfig config1 = EngineConfig
                .builder("job-crash", REJECT_EVERY_TENTH, writer, checkpoints)
                .batchSize(100)
                .shardCount(1)
                .build();

        assertThatThrownBy(() -> new BatchImportEngine(config1).run(source(total)))
                .isInstanceOf(BatchImportException.class);

        // crash batch (201..300) was not committed, so its checkpoint is absent
        assertThat(checkpoints.load("job-crash", "shard-0")).isEqualTo(200);
        assertThat(writer.crashed).isTrue();

        EngineConfig config2 = EngineConfig
                .builder("job-crash", REJECT_EVERY_TENTH, writer, checkpoints)
                .batchSize(100)
                .shardCount(1)
                .build();
        ImportStats stats2 = new BatchImportEngine(config2).run(source(total));

        // pre-crash subset of the batch is redelivered and deduplicated by row key
        assertThat(writer.duplicateAttempts()).isGreaterThan(0);
        assertThat(writer.writtenCount()).isEqualTo(900);
        assertThat(writer.writtenRows())
                .extracting(Row::lineNumber)
                .doesNotHaveDuplicates()
                .hasSize(900)
                .noneMatch(line -> line % 10 == 0);
        // failure details of the replayed batch appear exactly once
        assertThat(stats2.failures())
                .extracting(FailureDetail::lineNumber)
                .doesNotHaveDuplicates()
                .hasSize(80); // failures in lines 201..1000
        assertThat(checkpoints.load("job-crash", "shard-0")).isEqualTo(1000);
    }

    @Test
    void fileCheckpointSurvivesNewEngineAndStoreInstances(@TempDir java.nio.file.Path tempDir) {
        long total = 2_000;
        InMemoryWriter writer = new InMemoryWriter();
        BatchImportEngine engine1 = engine(
                "job-file", writer, new FileCheckpointStore(tempDir), 128, 3, 777L);
        engine1.run(source(total));

        assertThat(new FileCheckpointStore(tempDir).files()).isNotEmpty();

        // brand new store instance reads the same persisted positions
        BatchImportEngine engine2 = engine(
                "job-file", writer, new FileCheckpointStore(tempDir), 128, 3, null);
        engine2.run(source(total));

        assertThat(writer.writtenCount()).isEqualTo(total - total / 10);
        assertThat(writer.writtenRows()).extracting(Row::lineNumber).doesNotHaveDuplicates();
        assertThat(writer.duplicateAttempts()).isZero();
    }

    private static BatchImportEngine engine(String jobId,
                                            RecordWriter writer,
                                            CheckpointStore checkpoints,
                                            int batchSize,
                                            int shardCount,
                                            Long stopAfterRows) {
        BatchImportEngine[] ref = new BatchImportEngine[1];
        ProgressCallback callback = stopAfterRows == null
                ? null
                : progress -> {
                    if (progress.processedRows() >= stopAfterRows) {
                        ref[0].requestStop();
                    }
                };
        EngineConfig config = EngineConfig
                .builder(jobId, REJECT_EVERY_TENTH, writer, checkpoints)
                .batchSize(batchSize)
                .shardCount(shardCount)
                .progressCallback(callback)
                .build();
        ref[0] = new BatchImportEngine(config);
        return ref[0];
    }

    /**
     * Writes part of one batch and then dies to model a non-transactional database
     * batch failing halfway through.
     */
    private static final class PartialCrashWriter implements RecordWriter {

        private final InMemoryWriter delegate = new InMemoryWriter();
        volatile boolean crashed;

        @Override
        public void writeBatch(String shardId, List<Row> rows) {
            if (!crashed) {
                for (int i = 0; i < rows.size(); i++) {
                    if (rows.get(i).lineNumber() == 251) {
                        crashed = true;
                        delegate.writeBatch(shardId, rows.subList(0, i));
                        throw new IllegalStateException("simulated hard crash mid-batch");
                    }
                }
            }
            delegate.writeBatch(shardId, rows);
        }

        long writtenCount() {
            return delegate.writtenCount();
        }

        long duplicateAttempts() {
            return delegate.duplicateAttempts();
        }

        List<Row> writtenRows() {
            return delegate.writtenRows();
        }
    }
}
