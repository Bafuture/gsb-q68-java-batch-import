package com.example.gsb.batch;

/**
 * 进度回调，每处理完一批调用一次（可能由多个工作线程并发调用）。
 */
@FunctionalInterface
public interface ProgressListener {

    void onProgress(long processedRows, long successCount, long failureCount);
}
