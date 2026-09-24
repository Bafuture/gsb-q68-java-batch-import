package com.example.gsb.batchimport;

/**
 * Point-in-time progress snapshot delivered to {@link ProgressCallback}s from worker
 * threads; counters are monotonically increasing during a run.
 */
public record Progress(long processedRows, long successCount, long failureCount) {

    public long totalRows() {
        return successCount + failureCount;
    }
}
