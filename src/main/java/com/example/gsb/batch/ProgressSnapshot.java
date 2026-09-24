package com.example.gsb.batch;

/**
 * 进度快照，在每次检查点提交后推送给监听器。
 *
 * @param committedRow 已提交（检查点已覆盖）到的行号，即下一待读行
 * @param successCount 截至目前的累计成功数
 * @param failureCount 截至目前的累计失败数
 */
public record ProgressSnapshot(long committedRow, long successCount, long failureCount) {
}
