package com.example.gsb.batch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 假写入器：把记录写入内存 List，线程安全。
 * 同时记录每次写入的批大小，便于测试验证分片行为。
 */
public final class InMemoryRecordWriter implements RecordWriter {

    private final List<String> written = Collections.synchronizedList(new ArrayList<>());
    private final List<Integer> batchSizes = Collections.synchronizedList(new ArrayList<>());

    @Override
    public void writeBatch(List<String> records) {
        if (records.isEmpty()) {
            return;
        }
        batchSizes.add(records.size());
        written.addAll(records);
    }

    public List<String> writtenRecords() {
        synchronized (written) {
            return List.copyOf(written);
        }
    }

    public List<Integer> batchSizes() {
        synchronized (batchSizes) {
            return List.copyOf(batchSizes);
        }
    }
}
