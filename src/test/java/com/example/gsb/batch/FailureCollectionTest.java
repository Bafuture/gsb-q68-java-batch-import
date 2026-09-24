package com.example.gsb.batch;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

/** 单条校验失败：记录行号与原因，继续处理不中断。 */
class FailureCollectionTest {

    @Test
    void collectsRowNumberAndReasonAndContinues() {
        int total = 100;
        List<String> rows = IntStream.range(0, total)
                .mapToObj(i -> (i == 10 || i == 25 || i == 99) ? "bad-" + i : "ok-" + i)
                .toList();
        InMemoryDataSource source = new InMemoryDataSource(rows);
        InMemoryRecordWriter writer = new InMemoryRecordWriter();
        InMemoryCheckpointStore checkpoints = new InMemoryCheckpointStore();

        RecordValidator validator = payload -> payload.startsWith("bad")
                ? Optional.of("rejected: " + payload)
                : Optional.empty();

        ImportEngine engine = new ImportEngine(source, validator, writer,
                checkpoints, ImportConfig.of(10, 2), ProgressListener.NOOP);
        ImportReport report = engine.run();

        assertThat(report.successCount()).isEqualTo(total - 3);
        assertThat(report.failureCount()).isEqualTo(3);
        assertThat(report.failures())
                .containsExactlyInAnyOrder(
                        new FailureDetail(10, "rejected: bad-10"),
                        new FailureDetail(25, "rejected: bad-25"),
                        new FailureDetail(99, "rejected: bad-99"));

        // 失败行之后的记录仍然被处理（不中断整批与后续批次）。
        assertThat(writer.writtenRecords())
                .hasSize(total - 3)
                .contains("ok-11", "ok-26")
                .doesNotContain("bad-10", "bad-25", "bad-99");

        // 失败不阻塞检查点：整段数据都已提交。
        assertThat(checkpoints.load()).contains(new Checkpoint(total));
    }

    @Test
    void degradesWholeBatchToFailuresWhenWriterThrows() {
        List<String> rows = IntStream.range(0, 30).mapToObj(i -> "row-" + i).toList();
        InMemoryDataSource source = new InMemoryDataSource(rows);
        InMemoryCheckpointStore checkpoints = new InMemoryCheckpointStore();

        // 第二批（行 10..19）写入时抛异常。
        RecordWriter flakyWriter = records -> {
            if (records.contains("row-10")) {
                throw new RuntimeException("disk full");
            }
        };

        ImportEngine engine = new ImportEngine(source, RecordValidator.acceptAll(), flakyWriter,
                checkpoints, new ImportConfig(10, 1, 2), ProgressListener.NOOP);
        ImportReport report = engine.run();

        assertThat(report.successCount()).isEqualTo(20);
        assertThat(report.failureCount()).isEqualTo(10);
        assertThat(report.failures())
                .allSatisfy(f -> assertThat(f.reason()).contains("disk full"))
                .extracting(FailureDetail::rowNumber)
                .containsExactlyInAnyOrder(10L, 11L, 12L, 13L, 14L, 15L, 16L, 17L, 18L, 19L);
        assertThat(checkpoints.load()).contains(new Checkpoint(30));
    }
}
