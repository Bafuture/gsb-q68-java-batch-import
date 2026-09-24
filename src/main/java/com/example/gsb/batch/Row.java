package com.example.gsb.batch;

import java.util.List;

/**
 * 一行待导入的数据。{@code lineNumber} 从 1 开始，全局唯一且连续，
 * 同时充当幂等写入与检查点定位的键。
 */
public record Row(long lineNumber, List<String> fields) {

    public Row {
        fields = List.copyOf(fields);
    }
}
