package com.example.gsb.batchimport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.LongStream;
import org.junit.jupiter.api.Test;

class BatchImportEngineTest {

    private static final RowValidator REJECT_EVERY_TENTH = row ->
            row.lineNumber() % 10 == 0 ? Optional.of("rejected every tenth line") : Optional.empty();

    private static InMemoryDataSource source(long n) {
        return new InMemoryDataSource(LongStream.rangeClosed(1, n).mapToObj("v%d"::formatted).toList());
    }

    @Test
    void validRowsWrittenInvalidRowsCollectedWithLineAndReason() {
        long total = 5_000;
        InMemoryWriter writer = new InMemoryWriter();
        List<Progress> snapshots = new CopyOnWriteArrayList<>();

        EngineConfig config = EngineConfig
                .builder("job-success", REJECT_EVERY_TENTH, writer, new InMemoryCheckpointStore())
                .batchSize(237)
                .shardCount(4)
                .progressCallback(snapshots::add)
                .build();

        ImportStats stats = new BatchImportEngine(config).run(source(total));

        assertThat(stats.totalRows()).isEqualTo(total);
        assertThat(stats.successCount()).isEqualTo(4_500);
        assertThat(stats.failureCount()).isEqualTo(500);

        List<Long> writtenLines = writer.writtenRows().stream().map(Row::lineNumber).toList();
        assertThat(writtenLines)
                .hasSize(4_500)
                .doesNotHaveDuplicates()
                .doesNotContain(10L, 20L, 5_000L)
                .contains(1L, 9L, 11L, 4_999L);

        assertThat(stats.failures()).hasSize(500);
        assertThat(stats.failures())
                .extracting(FailureDetail::lineNumber)
                .isSorted()
                .containsExactly(LongStream.rangeClosed(1, 500).map(i -> i * 10).boxed().toArray(Long[]::new));
        assertThat(stats.failures())
                .allSatisfy(detail -> {
                    assertThat(detail.reason()).isEqualTo("rejected every tenth line");
                    assertThat(detail.value()).isEqualTo("v" + detail.lineNumber());
                });

        assertThat(snapshots).isNotEmpty();
        Progress last = snapshots.get(snapshots.size() - 1);
        assertThat(last.processedRows()).isEqualTo(total);
        assertThat(last.successCount()).isEqualTo(4_500);
        assertThat(last.failureCount()).isEqualTo(500);
        long previous = 0;
        for (Progress snapshot : snapshots) {
            assertThat(snapshot.processedRows()).isGreaterThanOrEqualTo(previous);
            previous = snapshot.processedRows();
        }
    }

    @Test
    void oneBadRowDoesNotAbortItsBatch() {
        InMemoryDataSource smallSource = new InMemoryDataSource(List.of("a", "bad", "c", "d"));
        InMemoryWriter writer = new InMemoryWriter();
        RowValidator onlyBadRejected = row ->
                "bad".equals(row.value()) ? Optional.of("value must not be 'bad'") : Optional.empty();

        EngineConfig config = EngineConfig
                .builder("job-one-bad", onlyBadRejected, writer, new InMemoryCheckpointStore())
                .batchSize(10)
                .shardCount(1)
                .build();

        ImportStats stats = new BatchImportEngine(config).run(smallSource);

        assertThat(stats.successCount()).isEqualTo(3);
        assertThat(stats.failureCount()).isEqualTo(1);
        assertThat(writer.writtenRows()).extracting(Row::value).containsExactly("a", "c", "d");
        assertThat(stats.failures()).singleElement().satisfies(detail -> {
            assertThat(detail.lineNumber()).isEqualTo(2);
            assertThat(detail.reason()).isEqualTo("value must not be 'bad'");
        });
    }

    @Test
    void writerFailureAbortsShardAndExposesCheckpointForResume() {
        InMemoryWriter writer = new InMemoryWriter();
        RecordWriter failingWriter = (shardId, rows) -> {
            for (Row row : rows) {
                if (row.lineNumber() == 251) {
                    throw new IllegalStateException("simulated writer outage");
                }
            }
            writer.writeBatch(shardId, rows);
        };
        InMemoryCheckpointStore checkpoints = new InMemoryCheckpointStore();

        EngineConfig config = EngineConfig
                .builder("job-writer-failure", REJECT_EVERY_TENTH, failingWriter, checkpoints)
                .batchSize(100)
                .shardCount(1)
                .build();

        assertThatThrownBy(() -> new BatchImportEngine(config).run(source(1_000)))
                .isInstanceOf(BatchImportException.class)
                .hasMessageContaining("writer failed");

        // failed batch is 201..300, so the committed checkpoint stays at 200
        assertThat(checkpoints.load("job-writer-failure", "shard-0")).isEqualTo(200);
        assertThat(writer.writtenRows()).extracting(Row::lineNumber).allMatch(line -> line <= 200);
    }

    @Test
    void emptySourceProducesEmptyReport() {
        EngineConfig config = EngineConfig
                .builder("job-empty", row -> Optional.empty(), new InMemoryWriter(),
                        new InMemoryCheckpointStore())
                .build();
        ImportStats stats = new BatchImportEngine(config).run(new InMemoryDataSource(List.of()));
        assertThat(stats.totalRows()).isZero();
        assertThat(stats.successCount()).isZero();
        assertThat(stats.failureCount()).isZero();
        assertThat(stats.failures()).isEmpty();
    }
}
