package com.example.gsb.batch;

import java.util.List;

/**
 * 流式数据源。实现方按需产出数据，不要求一次性物化全部行。
 */
public interface DataSource {

    /**
     * 从 {@code offset}（0 基，已处理的行数）开始读取最多 {@code limit} 行。
     * 返回列表为空表示数据已读完。
     */
    List<Row> read(long offset, int limit);
}
