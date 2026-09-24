package com.example.gsb.batchimport;

import java.util.Optional;

/**
 * Validates a single row. Return an {@link Optional} containing the reason when the row
 * is invalid; return {@link Optional#empty()} when the row is valid.
 *
 * <p>Implementations must be thread-safe when used with multiple shard workers.
 */
@FunctionalInterface
public interface RowValidator {

    Optional<String> validate(Row row);
}
