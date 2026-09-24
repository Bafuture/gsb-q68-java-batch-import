package com.example.gsb.batch;

import java.util.Optional;

/**
 * 单行校验器。返回空表示通过，否则返回失败原因。
 */
@FunctionalInterface
public interface RowValidator {

    Optional<String> validate(Row row);
}
