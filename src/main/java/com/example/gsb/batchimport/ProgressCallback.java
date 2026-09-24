package com.example.gsb.batchimport;

/**
 * Called after every processed batch from any worker thread. Implementations must be
 * thread-safe and must return quickly; heavy work here serializes the whole pipeline.
 */
@FunctionalInterface
public interface ProgressCallback {

    void onProgress(Progress progress);
}
