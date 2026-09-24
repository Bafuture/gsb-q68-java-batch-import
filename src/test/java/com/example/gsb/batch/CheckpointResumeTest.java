package com.example.gsb.batch;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

/** 检查点：中断续跑不重复写入。 */
class CheckpointResumeTest {

    private static List<String> rows(int count) {
        return IntStream.range(0, count).mapToObj(i -> "row-" + i).toList();
    }

    @Test
    void resumesFromCheckpointWithoutDuplicates() {
        int total = 1_000;
        int batchSize = 50;
        InMemoryDataSource source = new InMemoryDataSource(rows(total));
        InMemoryRecordWriter writer = new InMemoryRecordWriter();
        InMemoryCheckpointStore checkpoints = new InMemoryCheckpointStore();

        // 第一次运行：提交满 300 行后请求停止。
        AtomicReference<ImportEngine> ref = new AtomicReference<>();
        ImportEngine first = new ImportEngine(source, RecordValidator.acceptAll(), writer,
                checkpoints, ImportConfig.of(batchSize, 2),
                snapshot -> {
                    if (snapshot.committedRow() >= 300) {
                        ref.get().requestStop();
                    }
                });
        ref.set(first);
        ImportReport firstReport = first.run();

        assertThat(firstReport.interrupted()).isTrue();
        long committedAfterFirstRun = checkpoints.load().orElseThrow().nextRow();
        assertThat(committedAfterFirstRun).isGreaterThanOrEqualTo(300).isLessThan(total);
        assertThat(writer.writtenRecords()).hasSize((int) committedAfterFirstRun);

        // 第二次运行：同一存储与写入器，从检查点继续。
        ImportEngine second = new ImportEngine(source, RecordValidator.acceptAll(), writer,
                checkpoints, ImportConfig.of(batchSize, 2), ProgressListener.NOOP);
        ImportReport secondReport = second.run();

        assertThat(secondReport.interrupted()).isFalse();
        assertThat(firstReport.successCount() + secondReport.successCount()).isEqualTo(total);
        assertThat(writer.writtenRecords())
                .hasSize(total)
                .doesNotHaveDuplicates()
                .containsExactlyInAnyOrderElementsOf(rows(total));
        assertThat(checkpoints.load()).contains(new Checkpoint(total));
    }

    @Test
    void completedRunIsNotReprocessed() {
        int total = 500;
        InMemoryDataSource source = new InMemoryDataSource(rows(total));
        InMemoryRecordWriter writer = new InMemoryRecordWriter();
        InMemoryCheckpointStore checkpoints = new InMemoryCheckpointStore();

        new ImportEngine(source, RecordValidator.acceptAll(), writer,
                checkpoints, ImportConfig.of(50, 2), ProgressListener.NOOP).run();
        assertThat(writer.writtenRecords()).hasSize(total);

        // 再次运行：检查点已在末尾，不应再写入任何数据。
        ImportReport report = new ImportEngine(source, RecordValidator.acceptAll(), writer,
                checkpoints, ImportConfig.of(50, 2), ProgressListener.NOOP).run();

        assertThat(report.successCount()).isZero();
        assertThat(writer.writtenRecords()).hasSize(total).doesNotHaveDuplicates();
    }

    @Test
    void resumesAcrossMultipleInterruptions() {
        int total = 2_000;
        InMemoryDataSource source = new InMemoryDataSource(rows(total));
        InMemoryRecordWriter writer = new InMemoryRecordWriter();
        InMemoryCheckpointStore checkpoints = new InMemoryCheckpointStore();

        // 连续三次「跑一段就停」，最后跑到完成。
        for (int round = 0; round < 3; round++) {
            long stopAfter = 500L * (round + 1);
            AtomicReference<ImportEngine> ref = new AtomicReference<>();
            ImportEngine engine = new ImportEngine(source, RecordValidator.acceptAll(), writer,
                    checkpoints, ImportConfig.of(40, 3),
                    snapshot -> {
                        if (snapshot.committedRow() >= stopAfter) {
                            ref.get().requestStop();
                        }
                    });
            ref.set(engine);
            engine.run();
        }
        ImportReport finalReport = new ImportEngine(source, RecordValidator.acceptAll(), writer,
                checkpoints, ImportConfig.of(40, 3), ProgressListener.NOOP).run();

        assertThat(finalReport.interrupted()).isFalse();
        assertThat(writer.writtenRecords())
                .hasSize(total)
                .doesNotHaveDuplicates()
                .containsExactlyInAnyOrderElementsOf(rows(total));
        assertThat(checkpoints.load()).contains(new Checkpoint(total));
    }
}
