package com.example.gsb.batchimport;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Streaming, sharded, multi-threaded batch import engine.
 *
 * <h2>Per-batch transaction boundary</h2>
 * For every batch each shard worker performs, in order:
 * <ol>
 *   <li>stream up to {@code batchSize} rows from the shard cursor;</li>
 *   <li>validate every row and split it into valid / rejected rows (no abort on bad rows);</li>
 *   <li>idempotently write the valid rows as one batch;</li>
 *   <li>persist the checkpoint at the last processed line;</li>
 *   <li>only then commit success/failure counters, failure details and progress.</li>
 * </ol>
 * Because the checkpoint is persisted before anything is counted and failure details are
 * appended after the checkpoint, a crash inside the batch is recovered by replaying it:
 * the idempotent writer drops duplicate rows and failure details are committed exactly
 * once.
 *
 * <p>Stop is cooperative: {@link #requestStop()} (or thread interruption) takes effect
 * between committed batches, so a resumed run never replays a committed batch.
 */
public final class BatchImportEngine {

    private final EngineConfig config;
    private volatile boolean stopRequested;

    public BatchImportEngine(EngineConfig config) {
        this.config = Objects.requireNonNull(config, "config");
    }

    /** Asks a running import to stop after the current batches are committed. */
    public void requestStop() {
        stopRequested = true;
    }

    public ImportStats run(DataSource source) {
        Objects.requireNonNull(source, "source");
        stopRequested = false;

        AtomicLong success = new AtomicLong();
        AtomicLong failure = new AtomicLong();
        List<FailureDetail> failures = java.util.Collections.synchronizedList(new ArrayList<>());

        int shardCount = Math.min(config.shardCount(), (int) Math.min(Integer.MAX_VALUE, source.size()));
        if (shardCount == 0) {
            return new ImportStats(0, 0, 0, List.of());
        }

        ExecutorService pool = Executors.newFixedThreadPool(shardCount, runnable -> {
            Thread thread = new Thread(runnable, "batch-import-worker");
            thread.setDaemon(true);
            return thread;
        });
        List<Future<?>> futures = new ArrayList<>(shardCount);
        try {
            for (int shardIndex = 0; shardIndex < shardCount; shardIndex++) {
                final int index = shardIndex;
                futures.add(pool.submit(() -> processShard(source, index, shardCount, success, failure, failures)));
            }
            awaitCompletion(futures);
        } finally {
            pool.shutdownNow();
        }

        List<FailureDetail> sortedFailures = new ArrayList<>(failures);
        sortedFailures.sort(Comparator.comparingLong(FailureDetail::lineNumber));
        long failureCount = failure.get();
        return new ImportStats(success.get() + failureCount, success.get(), failureCount, sortedFailures);
    }

    private void awaitCompletion(List<Future<?>> futures) {
        RuntimeException fatal = null;
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fatal = new BatchImportException("import interrupted while waiting for shards", e);
                break;
            } catch (java.util.concurrent.ExecutionException e) {
                Throwable cause = e.getCause();
                if (fatal == null) {
                    stopRequested = true;
                    fatal = (cause instanceof BatchImportException)
                            ? (BatchImportException) cause
                            : new BatchImportException("shard failed: " + cause.getMessage(), cause);
                }
            }
        }
        if (fatal != null) {
            for (Future<?> future : futures) {
                future.cancel(true);
            }
            throw fatal;
        }
    }

    private void processShard(DataSource source,
                              int shardIndex,
                              int shardCount,
                              AtomicLong success,
                              AtomicLong failure,
                              List<FailureDetail> failures) {
        String shardId = "shard-" + shardIndex;
        long checkpoint = config.checkpointStore().load(config.jobId(), shardId);
        try (ShardReader reader = source.openShard(shardIndex, shardCount, checkpoint)) {
            while (!stopRequested && !Thread.currentThread().isInterrupted()) {
                List<Row> batch = reader.readBatch(config.batchSize());
                if (batch.isEmpty()) {
                    return;
                }

                List<Row> valid = new ArrayList<>(batch.size());
                List<FailureDetail> batchFailures = new ArrayList<>();
                for (Row row : batch) {
                    var reason = config.validator().validate(row);
                    if (reason.isEmpty()) {
                        valid.add(row);
                    } else {
                        batchFailures.add(new FailureDetail(row.lineNumber(), row.value(), reason.get()));
                    }
                }

                try {
                    if (!valid.isEmpty()) {
                        config.writer().writeBatch(shardId, List.copyOf(valid));
                    }
                } catch (RuntimeException e) {
                    throw new BatchImportException(
                            "writer failed for shard " + shardId + " ending at line "
                                    + batch.get(batch.size() - 1).lineNumber(), e);
                }

                long lastLine = batch.get(batch.size() - 1).lineNumber();
                config.checkpointStore().save(config.jobId(), shardId, lastLine);

                long batchFailureCount = batchFailures.size();
                long batchSuccessCount = valid.size();
                failures.addAll(batchFailures);
                long totalFailure = failure.addAndGet(batchFailureCount);
                long totalSuccess = success.addAndGet(batchSuccessCount);
                config.progressCallback().onProgress(new Progress(
                        totalSuccess + totalFailure, totalSuccess, totalFailure));
            }
        } catch (BatchImportException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new BatchImportException("shard " + shardId + " failed: " + e.getMessage(), e);
        }
    }
}
