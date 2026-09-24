package com.example.gsb.batch;

import java.util.List;

/**
 * 写入器抽象：以批为单位写入已通过校验的记录。
 */
public interface RecordWriter {

    void writeBatch(List<String> records);
}
