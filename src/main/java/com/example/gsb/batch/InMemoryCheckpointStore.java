package com.example.gsb.batch;

import java.util.Optional;

/**
 * 内存检查点存储，线程安全。进程退出即丢失，仅用于演示与测试。
 */
public final class InMemoryCheckpointStore implements CheckpointStore {

    private volatile Checkpoint checkpoint;

    @Override
    public Optional<Checkpoint> load() {
        return Optional.ofNullable(checkpoint);
    }

    @Override
    public void save(Checkpoint checkpoint) {
        this.checkpoint = checkpoint;
    }
}
