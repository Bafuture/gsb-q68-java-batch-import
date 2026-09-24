package com.example.gsb.batch;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.LongFunction;

/**
 * 假数据源：按行号惰性生成数据，不预先生成全部行，
 * 用于模拟“几十万行文件”而本身不占线性内存。
 */
public final class InMemoryDataSource implements DataSource {

    /** 记录每次 read 调用的 [offset, limit]，供测试断言分片读取行为。 */
    public record ReadCall(long offset, int limit) {
    }

    private final long totalRows;
    private final LongFunction<List<String>> rowGenerator;
    private final List<ReadCall> readCalls = new CopyOnWriteArrayList<>();

    public InMemoryDataSource(long totalRows, LongFunction<List<String>> rowGenerator) {
        if (totalRows < 0) {
            throw new IllegalArgumentException("totalRows must be >= 0");
        }
        this.totalRows = totalRows;
        this.rowGenerator = rowGenerator;
    }

    @Override
    public List<Row> read(long offset, int limit) {
        readCalls.add(new ReadCall(offset, limit));
        List<Row> batch = new ArrayList<>(Math.min(limit, (int) Math.min(limit, totalRows - offset)));
        for (long i = offset; i < Math.min(offset + limit, totalRows); i++) {
            batch.add(new Row(i + 1, rowGenerator.apply(i)));
        }
        return batch;
    }

    public List<ReadCall> readCalls() {
        return List.copyOf(readCalls);
    }
}
