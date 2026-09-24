package com.example.gsb.batch;

/**
 * 引擎配置。
 *
 * @param batchSize          每批记录数；内存占用上界 ≈ batchSize × maxInFlightBatches
 * @param threadCount        处理批次的 worker 线程数
 * @param maxInFlightBatches 允许同时在飞（已读取未提交）的最大批次数，提供背压
 */
public record ImportConfig(int batchSize, int threadCount, int maxInFlightBatches) {

    public ImportConfig {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be positive");
        }
        if (threadCount <= 0) {
            throw new IllegalArgumentException("threadCount must be positive");
        }
        if (maxInFlightBatches <= 0) {
            throw new IllegalArgumentException("maxInFlightBatches must be positive");
        }
    }

    public static ImportConfig of(int batchSize, int threadCount) {
        return new ImportConfig(batchSize, threadCount, threadCount * 2);
    }
}
