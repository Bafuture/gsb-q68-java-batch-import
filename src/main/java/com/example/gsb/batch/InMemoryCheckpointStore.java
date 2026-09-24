package com.example.gsb.batch;

import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 内存版检查点存储。真实实现可替换为数据库表或文件。
 */
public final class InMemoryCheckpointStore implements CheckpointStore {

    private static final long EMPTY = -1L;

    private final AtomicLong position = new AtomicLong(EMPTY);

    @Override
    public OptionalLong load() {
        long value = position.get();
        return value == EMPTY ? OptionalLong.empty() : OptionalLong.of(value);
    }

    @Override
    public void save(long processedRows) {
        position.set(processedRows);
    }
}
