package com.example.gsb.batchimport;

import java.util.Objects;

/**
 * A single logical row pulled from a {@link DataSource}.
 *
 * <p>{@code lineNumber} is 1-based and unique inside one job.
 */
public record Row(long lineNumber, String value) {

    public Row {
        if (lineNumber < 1) {
            throw new IllegalArgumentException("lineNumber must be >= 1, got " + lineNumber);
        }
        Objects.requireNonNull(value, "value");
    }
}
