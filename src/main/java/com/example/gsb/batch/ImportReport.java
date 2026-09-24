package com.example.gsb.batch;

import java.util.List;

/**
 * 任务结束后的统计报告。
 *
 * @param successCount 成功写入的记录数
 * @param failureCount 校验失败（或批次写入失败被降级）的记录数
 * @param failures     失败明细（行号 + 原因），按提交顺序排列
 * @param interrupted  任务是否因外部中断（{@code requestStop}）而提前结束
 */
public record ImportReport(long successCount,
                           long failureCount,
                           List<FailureDetail> failures,
                           boolean interrupted) {

    public ImportReport {
        failures = List.copyOf(failures);
    }
}
