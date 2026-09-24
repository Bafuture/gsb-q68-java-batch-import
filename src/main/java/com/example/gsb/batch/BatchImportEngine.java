package com.example.gsb.batch;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 批量导入引擎。
 *
 * <p>处理模型：
 * <ul>
 *   <li>单线程按 {@code batchSize} 从 {@link DataSource} 流式拉取批次，
 *       用信号量限制在途批次数（{@code threads * 2}），
 *       因此内存占用上界为 {@code batchSize * threads * 2} 行，与总行数无关；</li>
 *   <li>批次提交到固定线程池并行校验 + 写入；</li>
 *   <li>单条校验失败只记录 {@link FailureDetail}，同批其余行照常写入；</li>
 *   <li>检查点按批次提交顺序（而非完成顺序）推进，保证续跑时
 *       已提交检查点之前的数据绝不重读；在途但未提交检查点的批次
 *       可能被重放，由幂等 {@link RowWriter} 去重。</li>
 * </ul>
 */
public final class BatchImportEngine {

    private final DataSource source;
    private final RowWriter writer;
    private final RowValidator validator;
    private final CheckpointStore checkpointStore;
    private final ProgressListener progressListener;
    private final int batchSize;
    private final int threads;
    private final int maxInFlightBatches;

    public BatchImportEngine(DataSource source,
                             RowWriter writer,
                             RowValidator validator,
                             CheckpointStore checkpointStore,
                             ProgressListener progressListener,
                             int batchSize,
                             int threads) {
        if (batchSize <= 0 || threads <= 0) {
            throw new IllegalArgumentException("batchSize and threads must be > 0");
        }
        this.source = source;
        this.writer = writer;
        this.validator = validator;
        this.checkpointStore = checkpointStore;
        this.progressListener = progressListener;
        this.batchSize = batchSize;
        this.threads = threads;
        this.maxInFlightBatches = threads * 2;
    }

    /**
     * 运行导入任务。若 {@link CheckpointStore} 中已有检查点，则从检查点续跑。
     *
     * @throws ImportException 发生致命错误（如写入器故障）导致任务中断
     */
    public ImportReport run() {
        AtomicLong successCount = new AtomicLong();
        AtomicLong failureCount = new AtomicLong();
        ConcurrentLinkedQueue<FailureDetail> failures = new ConcurrentLinkedQueue<>();

        long committed = checkpointStore.load().orElse(0L);
        long offset = committed;

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        Semaphore inFlight = new Semaphore(maxInFlightBatches);
        Deque<Future<Long>> pending = new ArrayDeque<>();
        try {
            while (true) {
                List<Row> batch = source.read(offset, batchSize);
                if (batch.isEmpty()) {
                    break;
                }
                offset += batch.size();
                inFlight.acquire();
                pending.addLast(pool.submit(() -> {
                    try {
                        processBatch(batch, successCount, failureCount, failures);
                        return batch.get(batch.size() - 1).lineNumber();
                    } finally {
                        inFlight.release();
                    }
                }));
                commitCompleted(pending);
            }
            while (!pending.isEmpty()) {
                commitHead(pending);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ImportException("导入任务被中断", e);
        } finally {
            pool.shutdownNow();
        }
        List<FailureDetail> sortedFailures = new ArrayList<>(failures);
        sortedFailures.sort(java.util.Comparator.comparingLong(FailureDetail::lineNumber));
        return new ImportReport(successCount.get(), failureCount.get(), sortedFailures);
    }

    private void processBatch(List<Row> batch,
                              AtomicLong successCount,
                              AtomicLong failureCount,
                              ConcurrentLinkedQueue<FailureDetail> failures) {
        List<Row> valid = new ArrayList<>(batch.size());
        for (Row row : batch) {
            Optional<String> error = validator.validate(row);
            if (error.isPresent()) {
                failures.add(new FailureDetail(row.lineNumber(), error.get()));
                failureCount.incrementAndGet();
            } else {
                valid.add(row);
            }
        }
        writer.write(valid);
        successCount.addAndGet(valid.size());
        progressListener.onProgress(
                successCount.get() + failureCount.get(), successCount.get(), failureCount.get());
    }

    /** 按提交顺序推进检查点：队首批次完成后才提交，保证检查点连续。 */
    private void commitCompleted(Deque<Future<Long>> pending) {
        while (!pending.isEmpty() && pending.peekFirst().isDone()) {
            commitHead(pending);
        }
    }

    private void commitHead(Deque<Future<Long>> pending) {
        Future<Long> head = pending.pollFirst();
        try {
            checkpointStore.save(head.get());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ImportException("导入任务被中断", e);
        } catch (ExecutionException e) {
            throw new ImportException("批次处理发生致命错误，任务中断，可从检查点续跑", e.getCause());
        }
    }
}
