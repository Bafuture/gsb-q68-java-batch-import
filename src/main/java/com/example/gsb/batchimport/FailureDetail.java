package com.example.gsb.batchimport;

import java.util.Objects;

/**
 * Description of one row that failed validation and was therefore never written.
 */
public record FailureDetail(long lineNumber, String value, String reason) {

    public FailureDetail {
        if (lineNumber < 1) {
            throw new IllegalArgumentException("lineNumber must be >= 1, got " + lineNumber);
        }
        Objects.requireNonNull(reason, "reason");
    }
}
