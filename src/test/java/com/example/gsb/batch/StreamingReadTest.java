package com.example.gsb.batch;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

/** 分片读取与多线程批处理。 */
class StreamingReadTest {

    private static List<String> rows(int count) {
        return IntStream.range(0, count).mapToObj(i -> "row-" + i).toList();
    }

    @Test
    void readsInBoundedBatchesAndWritesEverything() {
        int total = 10_000;
        int batchSize = 128;
        InMemoryDataSource source = new InMemoryDataSource(rows(total));
        InMemoryRecordWriter writer = new InMemoryRecordWriter();
        InMemoryCheckpointStore checkpoints = new InMemoryCheckpointStore();

        ImportEngine engine = new ImportEngine(source, RecordValidator.acceptAll(), writer,
                checkpoints, ImportConfig.of(batchSize, 4), ProgressListener.NOOP);
        ImportReport report = engine.run();

        assertThat(report.successCount()).isEqualTo(total);
        assertThat(report.failureCount()).isZero();
        assertThat(report.interrupted()).isFalse();

        // 分片：每次写入都不超过批大小，批次数与理论值一致，最后一批为余数。
        List<Integer> batchSizes = writer.batchSizes();
        int expectedBatches = (total + batchSize - 1) / batchSize;
        assertThat(batchSizes).hasSize(expectedBatches);
        assertThat(batchSizes).allSatisfy(size -> assertThat(size).isLessThanOrEqualTo(batchSize));
        assertThat(batchSizes.stream().mapToInt(Integer::intValue).sum()).isEqualTo(total);
        // 多线程下写入顺序不定：除一个余数批外，其余都是满批。
        assertThat(batchSizes).containsOnlyOnce(total % batchSize);
        assertThat(batchSizes.stream().filter(s -> s == batchSize).count())
                .isEqualTo(expectedBatches - 1L);

        // 全部写入且无重复（多线程下顺序无关）。
        assertThat(writer.writtenRecords())
                .hasSize(total)
                .doesNotHaveDuplicates()
                .containsExactlyInAnyOrderElementsOf(rows(total));

        // 跑完后检查点推进到末尾。
        assertThat(checkpoints.load()).contains(new Checkpoint(total));
    }

    @Test
    void backpressureBoundsInFlightBatches() {
        int total = 2_000;
        int batchSize = 50;
        int maxInFlight = 3;
        InMemoryDataSource source = new InMemoryDataSource(rows(total));
        InMemoryRecordWriter writer = new InMemoryRecordWriter();

        // 用一个慢写入器放大并发窗口，观察在飞批次数峰值。
        var inFlight = new java.util.concurrent.atomic.AtomicInteger();
        var peak = new java.util.concurrent.atomic.AtomicInteger();
        RecordWriter slowWriter = records -> {
            int now = inFlight.incrementAndGet();
            peak.accumulateAndGet(now, Math::max);
            try {
                Thread.sleep(2);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                inFlight.decrementAndGet();
            }
            writer.writeBatch(records);
        };

        ImportEngine engine = new ImportEngine(source, RecordValidator.acceptAll(), slowWriter,
                new InMemoryCheckpointStore(),
                new ImportConfig(batchSize, 8, maxInFlight), ProgressListener.NOOP);
        ImportReport report = engine.run();

        assertThat(report.successCount()).isEqualTo(total);
        // 正在执行的写入批次数永不超过在飞上限（读取端也因此被限流）。
        assertThat(peak.get()).isLessThanOrEqualTo(maxInFlight);
    }

    @Test
    void invokesProgressListenerMonotonically() {
        int total = 1_000;
        InMemoryDataSource source = new InMemoryDataSource(rows(total));
        InMemoryRecordWriter writer = new InMemoryRecordWriter();
        List<ProgressSnapshot> snapshots = new CopyOnWriteArrayList<>();

        ImportEngine engine = new ImportEngine(source, RecordValidator.acceptAll(), writer,
                new InMemoryCheckpointStore(), ImportConfig.of(100, 4),
                snapshots::add);
        engine.run();

        assertThat(snapshots).isNotEmpty();
        // 检查点单调推进，最终覆盖全部行。
        for (int i = 1; i < snapshots.size(); i++) {
            assertThat(snapshots.get(i).committedRow())
                    .isGreaterThanOrEqualTo(snapshots.get(i - 1).committedRow());
        }
        assertThat(snapshots.get(snapshots.size() - 1).committedRow()).isEqualTo(total);
    }

    @Test
    void handlesEmptySource() {
        ImportEngine engine = new ImportEngine(new InMemoryDataSource(List.of()),
                RecordValidator.acceptAll(), new InMemoryRecordWriter(),
                new InMemoryCheckpointStore(), ImportConfig.of(10, 2), ProgressListener.NOOP);
        ImportReport report = engine.run();

        assertThat(report.successCount()).isZero();
        assertThat(report.failureCount()).isZero();
        assertThat(report.failures()).isEmpty();
        assertThat(report.interrupted()).isFalse();
    }
}
