package com.example.gsb.batch;

import java.util.List;

/**
 * 任务结束后的统计报告。
 */
public record ImportReport(long successCount, long failureCount, List<FailureDetail> failures) {

    public ImportReport {
        failures = List.copyOf(failures);
    }

    public long totalProcessed() {
        return successCount + failureCount;
    }
}
