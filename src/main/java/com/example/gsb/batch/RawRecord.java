package com.example.gsb.batch;

/**
 * 一条从数据源读出的原始记录。rowNumber 为 0 起始的全局行号，
 * 在数据源的生命周期内单调递增，用于失败定位与检查点计算。
 */
public record RawRecord(long rowNumber, String payload) {
}
