package com.example.gsb.batch;

/**
 * 单条校验失败的明细：行号 + 原因。
 */
public record FailureDetail(long lineNumber, String reason) {
}
