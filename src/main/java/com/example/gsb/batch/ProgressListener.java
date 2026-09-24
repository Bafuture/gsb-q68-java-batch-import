package com.example.gsb.batch;

/**
 * 进度回调。每当一个或多个批次按序提交、检查点推进后触发。
 */
@FunctionalInterface
public interface ProgressListener {

    ProgressListener NOOP = snapshot -> { };

    void onProgress(ProgressSnapshot snapshot);
}
