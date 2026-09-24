package com.example.gsb.batch;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 假写入器：线程安全，按行号幂等去重，模拟数据库的唯一键约束。
 */
public class InMemoryRowWriter implements RowWriter {

    private final Map<Long, Row> rows = new ConcurrentHashMap<>();
    private final AtomicLong duplicateAttempts = new AtomicLong();

    @Override
    public void write(List<Row> batch) {
        for (Row row : batch) {
            if (rows.putIfAbsent(row.lineNumber(), row) != null) {
                duplicateAttempts.incrementAndGet();
            }
        }
    }

    public long size() {
        return rows.size();
    }

    public boolean contains(long lineNumber) {
        return rows.containsKey(lineNumber);
    }

    /** 因幂等去重而被丢弃的重复写入次数。 */
    public long duplicateAttempts() {
        return duplicateAttempts.get();
    }
}
