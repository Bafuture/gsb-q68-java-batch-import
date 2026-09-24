package com.example.gsb.batch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 批量导入引擎。
 *
 * <p>处理模型：单生产者按 {@code batchSize} 从数据源流式切批，
 * 通过信号量限制在飞批次数（背压），提交到固定大小线程池并行处理。
 * 每个批次内逐条校验：失败记录行号与原因并继续；通过的记录整批写入。
 * 批次完成后按批序号严格有序地推进检查点——只有所有前序批次都提交后，
 * 检查点才向前移动，因此中断续跑不会重复写入已提交数据。</p>
 */
public final class ImportEngine {

    private final DataSource dataSource;
    private final RecordValidator validator;
    private final RecordWriter writer;
    private final CheckpointStore checkpointStore;
    private final ImportConfig config;
    private final ProgressListener progressListener;

    private final AtomicBoolean stopRequested = new AtomicBoolean();

    public ImportEngine(DataSource dataSource,
                        RecordValidator validator,
                        RecordWriter writer,
                        CheckpointStore checkpointStore,
                        ImportConfig config,
                        ProgressListener progressListener) {
        this.dataSource = dataSource;
        this.validator = validator;
        this.writer = writer;
        this.checkpointStore = checkpointStore;
        this.config = config;
        this.progressListener = progressListener == null ? ProgressListener.NOOP : progressListener;
    }

    /** 请求停止：当前在飞批次处理完毕后退出，检查点保持一致，可安全续跑。 */
    public void requestStop() {
        stopRequested.set(true);
    }

    public ImportReport run() {
        long startRow = checkpointStore.load().map(Checkpoint::nextRow).orElse(0L);
        Iterator<RawRecord> iterator = dataSource.readFrom(startRow);

        ExecutorService pool = Executors.newFixedThreadPool(config.threadCount());
        Semaphore inFlightPermits = new Semaphore(config.maxInFlightBatches());

        AtomicLong successCount = new AtomicLong();
        AtomicLong failureCount = new AtomicLong();
        List<FailureDetail> failures = Collections.synchronizedList(new ArrayList<>());

        // 有序提交状态：批次乱序完成，但检查点必须按批序号连续推进。
        Object commitLock = new Object();
        TreeMap<Long, Long> completedEndRows = new TreeMap<>();
        long[] nextCommitIndex = {0L};

        long batchIndex = 0;
        boolean interrupted = false;
        try {
            while (!stopRequested.get() && iterator.hasNext()) {
                List<RawRecord> batch = drainBatch(iterator, config.batchSize());
                if (batch.isEmpty()) {
                    break;
                }
                inFlightPermits.acquire();
                long currentIndex = batchIndex++;
                pool.execute(() -> {
                    try {
                        processBatch(batch, successCount, failureCount, failures);
                        long endRow = batch.get(batch.size() - 1).rowNumber() + 1;
                        synchronized (commitLock) {
                            completedEndRows.put(currentIndex, endRow);
                            Long committedRow = null;
                            while (completedEndRows.containsKey(nextCommitIndex[0])) {
                                committedRow = completedEndRows.remove(nextCommitIndex[0]);
                                nextCommitIndex[0]++;
                                checkpointStore.save(new Checkpoint(committedRow));
                            }
                            if (committedRow != null) {
                                progressListener.onProgress(new ProgressSnapshot(
                                        committedRow, successCount.get(), failureCount.get()));
                            }
                        }
                    } finally {
                        inFlightPermits.release();
                    }
                });
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            interrupted = true;
        } finally {
            pool.shutdown();
            try {
                if (!pool.awaitTermination(1, TimeUnit.MINUTES)) {
                    pool.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                pool.shutdownNow();
                interrupted = true;
            }
        }

        boolean stoppedEarly = interrupted || (stopRequested.get() && iterator.hasNext());
        return new ImportReport(successCount.get(), failureCount.get(),
                List.copyOf(failures), stoppedEarly);
    }

    /**
     * 处理单个批次：逐条校验，失败记录行号与原因并继续；
     * 通过的记录整批写入。若整批写入抛异常，则把该批全部记录
     * 降级为失败（行号 + 异常原因），保证检查点语义不被破坏。
     */
    private void processBatch(List<RawRecord> batch,
                              AtomicLong successCount,
                              AtomicLong failureCount,
                              List<FailureDetail> failures) {
        List<String> valid = new ArrayList<>(batch.size());
        List<RawRecord> validRows = new ArrayList<>(batch.size());
        for (RawRecord record : batch) {
            validator.validate(record.payload())
                    .ifPresentOrElse(
                            reason -> {
                                failures.add(new FailureDetail(record.rowNumber(), reason));
                                failureCount.incrementAndGet();
                            },
                            () -> {
                                valid.add(record.payload());
                                validRows.add(record);
                            });
        }
        try {
            writer.writeBatch(valid);
            successCount.addAndGet(valid.size());
        } catch (RuntimeException e) {
            String reason = "batch write failed: " + e.getMessage();
            for (RawRecord row : validRows) {
                failures.add(new FailureDetail(row.rowNumber(), reason));
            }
            failureCount.addAndGet(validRows.size());
        }
    }

    private static List<RawRecord> drainBatch(Iterator<RawRecord> iterator, int batchSize) {
        List<RawRecord> batch = new ArrayList<>(batchSize);
        while (batch.size() < batchSize && iterator.hasNext()) {
            batch.add(iterator.next());
        }
        return batch;
    }
}
