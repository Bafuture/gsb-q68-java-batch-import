package com.example.gsb.batch;

/**
 * 导入任务被致命错误（如写入器故障）中断时抛出。
 * 此时检查点停留在最后一个已完整提交的批次处，可用同一
 * {@link CheckpointStore} 重新运行以续跑。
 */
public class ImportException extends RuntimeException {

    public ImportException(String message, Throwable cause) {
        super(message, cause);
    }
}
