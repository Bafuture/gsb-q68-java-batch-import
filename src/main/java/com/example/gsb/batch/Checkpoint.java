package com.example.gsb.batch;

/**
 * 检查点：下一待读取的行号。语义为「该行号之前的所有批次均已成功提交」，
 * 因此从该行号续跑不会重复写入任何已处理数据。
 */
public record Checkpoint(long nextRow) {
}
