package com.example.gsb.batch;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * 假数据源：由内存 List 支撑，但读取是惰性的——
 * 迭代器只在被消费时才按下标取行，不会为读取做整体拷贝。
 */
public final class InMemoryDataSource implements DataSource {

    private final List<String> rows;

    public InMemoryDataSource(List<String> rows) {
        this.rows = List.copyOf(rows);
    }

    public long size() {
        return rows.size();
    }

    @Override
    public Iterator<RawRecord> readFrom(long startRow) {
        if (startRow < 0 || startRow > rows.size()) {
            throw new IllegalArgumentException("startRow out of range: " + startRow);
        }
        return new Iterator<>() {
            private long cursor = startRow;

            @Override
            public boolean hasNext() {
                return cursor < rows.size();
            }

            @Override
            public RawRecord next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                long rowNumber = cursor++;
                return new RawRecord(rowNumber, rows.get((int) rowNumber));
            }
        };
    }
}
