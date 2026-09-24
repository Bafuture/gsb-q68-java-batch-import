package com.example.gsb.batch;

import java.util.List;

/**
 * 批量写入器。实现方必须保证按 {@link Row#lineNumber()} 幂等：
 * 同一行号重复写入不得产生重复数据（检查点续跑时可能发生重放）。
 */
public interface RowWriter {

    void write(List<Row> rows);
}
