package com.example.gsb.batch;

import java.util.OptionalLong;

/**
 * 检查点存储。记录已连续处理完成的行数（即下一趟要读取的 offset）。
 */
public interface CheckpointStore {

    /** 上次提交的检查点；从未提交过返回空。 */
    OptionalLong load();

    /** 提交检查点，{@code processedRows} 为已连续处理完成的行数。 */
    void save(long processedRows);
}
