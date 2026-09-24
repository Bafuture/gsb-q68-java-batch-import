package com.example.gsb.batchimport;

import java.util.List;

/**
 * Final report of an engine run: totals plus the full list of validation failures
 * (line number, value, reason), ordered by line number.
 */
public record ImportStats(long totalRows,
                          long successCount,
                          long failureCount,
                          List<FailureDetail> failures) {

    public ImportStats {
        failures = List.copyOf(failures);
    }
}
