package com.example.gsb.batch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class BatchImportEngineTest {

    private static final ProgressListener NOOP_LISTENER = (p, s, f) -> {
    };

    private static InMemoryDataSource sourceOf(long totalRows) {
        return new InMemoryDataSource(totalRows, i -> List.of("value-" + (i + 1)));
    }

    private static RowValidator rejectLineNumbersDivisibleBy(int divisor) {
        return row -> row.lineNumber() % divisor == 0
                ? Optional.of("line " + row.lineNumber() + " is invalid")
                : Optional.empty();
    }

    @Test
    void readsInShardsOfConfiguredBatchSize() {
        InMemoryDataSource source = sourceOf(250);
        InMemoryRowWriter writer = new InMemoryRowWriter();
        BatchImportEngine engine = new BatchImportEngine(
                source, writer, row -> Optional.empty(),
                new InMemoryCheckpointStore(), NOOP_LISTENER, 100, 2);

        ImportReport report = engine.run();

        assertThat(report.successCount()).isEqualTo(250);
        assertThat(writer.size()).isEqualTo(250);
        // 分片读取：offset 依次推进，limit 恒为批大小，最后一次读到空批次为止
        assertThat(source.readCalls()).containsExactly(
                new InMemoryDataSource.ReadCall(0, 100),
                new InMemoryDataSource.ReadCall(100, 100),
                new InMemoryDataSource.ReadCall(200, 100),
                new InMemoryDataSource.ReadCall(250, 100));
    }

    @Test
    void boundsInFlightRowsRegardlessOfTotalRowCount() throws Exception {
        int batchSize = 10;
        int threads = 2;
        InMemoryDataSource source = sourceOf(100_000);
        CountDownLatch releaseWrites = new CountDownLatch(1);
        InMemoryRowWriter blockedWriter = new InMemoryRowWriter() {
            @Override
            public void write(List<Row> rows) {
                try {
                    releaseWrites.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                super.write(rows);
            }
        };
        BatchImportEngine engine = new BatchImportEngine(
                source, blockedWriter, row -> Optional.empty(),
                new InMemoryCheckpointStore(), NOOP_LISTENER, batchSize, threads);

        Thread runner = new Thread(engine::run);
        runner.start();
        // 写入器被阻塞时，读取端最多领先 (threads * 2 + 1) 个批次就会被信号量卡住
        int maxReadableBatches = threads * 2 + 1;
        long deadline = System.currentTimeMillis() + 10_000;
        while (source.readCalls().size() < maxReadableBatches
                && System.currentTimeMillis() < deadline) {
            Thread.sleep(5);
        }
        Thread.sleep(200);
        assertThat(source.readCalls().size()).isLessThanOrEqualTo(maxReadableBatches);

        releaseWrites.countDown();
        runner.join(30_000);
        assertThat(runner.isAlive()).isFalse();
        assertThat(blockedWriter.size()).isEqualTo(100_000);
    }

    @Test
    void processesBatchesOnMultipleThreads() {
        Set<String> threadNames = ConcurrentHashMap.newKeySet();
        InMemoryDataSource source = sourceOf(2_000);
        InMemoryRowWriter writer = new InMemoryRowWriter() {
            @Override
            public void write(List<Row> rows) {
                threadNames.add(Thread.currentThread().getName());
                try {
                    Thread.sleep(2);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                super.write(rows);
            }
        };
        BatchImportEngine engine = new BatchImportEngine(
                source, writer, row -> Optional.empty(),
                new InMemoryCheckpointStore(), NOOP_LISTENER, 10, 4);

        ImportReport report = engine.run();

        assertThat(report.successCount()).isEqualTo(2_000);
        assertThat(threadNames.size()).isGreaterThan(1);
    }

    @Test
    void collectsFailuresWithLineNumbersAndKeepsProcessing() {
        InMemoryDataSource source = sourceOf(1_000);
        InMemoryRowWriter writer = new InMemoryRowWriter();
        BatchImportEngine engine = new BatchImportEngine(
                source, writer, rejectLineNumbersDivisibleBy(10),
                new InMemoryCheckpointStore(), NOOP_LISTENER, 100, 4);

        ImportReport report = engine.run();

        assertThat(report.failureCount()).isEqualTo(100);
        assertThat(report.successCount()).isEqualTo(900);
        assertThat(report.totalProcessed()).isEqualTo(1_000);
        // 失败明细带行号与原因，按行号排序
        assertThat(report.failures()).hasSize(100);
        assertThat(report.failures()).first()
                .isEqualTo(new FailureDetail(10, "line 10 is invalid"));
        assertThat(report.failures()).last()
                .isEqualTo(new FailureDetail(1_000, "line 1000 is invalid"));
        // 失败行未写入，合法行全部写入（整批未被中断）
        assertThat(writer.size()).isEqualTo(900);
        assertThat(writer.contains(10)).isFalse();
        assertThat(writer.contains(11)).isTrue();
    }

    @Test
    void reportsProgressViaCallback() {
        InMemoryDataSource source = sourceOf(500);
        InMemoryRowWriter writer = new InMemoryRowWriter();
        AtomicLong lastProcessed = new AtomicLong();
        AtomicLong callbacks = new AtomicLong();
        ProgressListener listener = (processed, success, failure) -> {
            lastProcessed.set(processed);
            callbacks.incrementAndGet();
        };
        BatchImportEngine engine = new BatchImportEngine(
                source, writer, rejectLineNumbersDivisibleBy(5),
                new InMemoryCheckpointStore(), listener, 50, 2);

        engine.run();

        assertThat(callbacks.get()).isEqualTo(10);
        assertThat(lastProcessed.get()).isEqualTo(500);
    }

    @Test
    void resumesFromCheckpointAfterInterruptionWithoutDuplicates() {
        int total = 1_000;
        int batchSize = 50;
        InMemoryCheckpointStore checkpointStore = new InMemoryCheckpointStore();
        InMemoryRowWriter writer = new InMemoryRowWriter();

        // 第一趟：写入满 300 行后写入器故障，任务中断
        AtomicLong written = new AtomicLong();
        RowWriter failingWriter = rows -> {
            if (written.get() >= 300) {
                throw new IllegalStateException("simulated storage failure");
            }
            writer.write(rows);
            written.addAndGet(rows.size());
        };
        BatchImportEngine firstRun = new BatchImportEngine(
                sourceOf(total), failingWriter, row -> Optional.empty(),
                checkpointStore, NOOP_LISTENER, batchSize, 1);

        assertThatThrownBy(firstRun::run)
                .isInstanceOf(ImportException.class)
                .hasMessageContaining("致命错误");
        assertThat(checkpointStore.load()).hasValue(300);
        long sizeAfterCrash = writer.size();
        assertThat(sizeAfterCrash).isGreaterThanOrEqualTo(300);

        // 第二趟：同一检查点存储与写入器，从检查点续跑
        BatchImportEngine secondRun = new BatchImportEngine(
                sourceOf(total), writer, row -> Optional.empty(),
                checkpointStore, NOOP_LISTENER, batchSize, 1);
        ImportReport report = secondRun.run();

        // 只补跑检查点之后的行，且全部行号唯一、无重复数据
        assertThat(report.successCount()).isEqualTo(total - 300);
        assertThat(writer.size()).isEqualTo(total);
        for (long line = 1; line <= total; line++) {
            assertThat(writer.contains(line)).isTrue();
        }
        assertThat(checkpointStore.load()).hasValue(total);
    }

    @Test
    void startsFromBeginningWhenNoCheckpointExists() {
        InMemoryDataSource source = sourceOf(120);
        InMemoryRowWriter writer = new InMemoryRowWriter();
        BatchImportEngine engine = new BatchImportEngine(
                source, writer, row -> Optional.empty(),
                new InMemoryCheckpointStore(), NOOP_LISTENER, 50, 2);

        engine.run();

        assertThat(writer.size()).isEqualTo(120);
        assertThat(source.readCalls().get(0).offset()).isZero();
    }
}
