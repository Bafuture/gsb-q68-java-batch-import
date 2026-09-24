package com.example.gsb.batch;

import java.util.Optional;

/**
 * 检查点存储抽象。生产实现可替换为文件或数据库；本仓库提供内存实现。
 */
public interface CheckpointStore {

    Optional<Checkpoint> load();

    void save(Checkpoint checkpoint);
}
