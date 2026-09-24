package com.example.gsb.batch;

/**
 * 一条失败明细：行号 + 失败原因。
 */
public record FailureDetail(long rowNumber, String reason) {
}
