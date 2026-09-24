package com.example.gsb.batch;

import java.util.Optional;

/**
 * 单条记录校验器。返回空表示通过，否则返回失败原因。
 * 校验失败只影响当前行，不会中断所在批次。
 */
@FunctionalInterface
public interface RecordValidator {

    Optional<String> validate(String payload);

    static RecordValidator acceptAll() {
        return payload -> Optional.empty();
    }
}
