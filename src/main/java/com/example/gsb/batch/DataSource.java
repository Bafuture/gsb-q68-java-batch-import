package com.example.gsb.batch;

import java.util.Iterator;

/**
 * 数据源抽象。实现必须提供惰性（lazy）迭代器：
 * 只有在 {@link Iterator#next()} 被调用时才物化对应行，
 * 从而保证内存占用与总行数无关。
 */
public interface DataSource {

    /**
     * 从指定行号（含）开始流式读取。
     *
     * @param startRow 起始行号（0 起始）
     * @return 惰性迭代器，按行号升序产出记录
     */
    Iterator<RawRecord> readFrom(long startRow);
}
