package com.example.gsb.batchimport;

import java.util.Objects;

/** Immutable configuration for a {@link BatchImportEngine} run. */
public final class EngineConfig {

    private final String jobId;
    private final int batchSize;
    private final int shardCount;
    private final RowValidator validator;
    private final RecordWriter writer;
    private final CheckpointStore checkpointStore;
    private final ProgressCallback progressCallback;

    private EngineConfig(Builder builder) {
        this.jobId = Objects.requireNonNull(builder.jobId, "jobId");
        if (builder.batchSize < 1) {
            throw new IllegalArgumentException("batchSize must be >= 1");
        }
        if (builder.shardCount < 1) {
            throw new IllegalArgumentException("shardCount must be >= 1");
        }
        this.batchSize = builder.batchSize;
        this.shardCount = builder.shardCount;
        this.validator = Objects.requireNonNull(builder.validator, "validator");
        this.writer = Objects.requireNonNull(builder.writer, "writer");
        this.checkpointStore = Objects.requireNonNull(builder.checkpointStore, "checkpointStore");
        this.progressCallback = builder.progressCallback == null
                ? progress -> { }
                : builder.progressCallback;
    }

    public String jobId() {
        return jobId;
    }

    public int batchSize() {
        return batchSize;
    }

    public int shardCount() {
        return shardCount;
    }

    public RowValidator validator() {
        return validator;
    }

    public RecordWriter writer() {
        return writer;
    }

    public CheckpointStore checkpointStore() {
        return checkpointStore;
    }

    public ProgressCallback progressCallback() {
        return progressCallback;
    }

    public static Builder builder(String jobId,
                                  RowValidator validator,
                                  RecordWriter writer,
                                  CheckpointStore checkpointStore) {
        return new Builder(jobId, validator, writer, checkpointStore);
    }

    public static final class Builder {

        private final String jobId;
        private final RowValidator validator;
        private final RecordWriter writer;
        private final CheckpointStore checkpointStore;
        private int batchSize = 500;
        private int shardCount = 1;
        private ProgressCallback progressCallback;

        private Builder(String jobId,
                        RowValidator validator,
                        RecordWriter writer,
                        CheckpointStore checkpointStore) {
            this.jobId = jobId;
            this.validator = validator;
            this.writer = writer;
            this.checkpointStore = checkpointStore;
        }

        public Builder batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder shardCount(int shardCount) {
            this.shardCount = shardCount;
            return this;
        }

        public Builder progressCallback(ProgressCallback progressCallback) {
            this.progressCallback = progressCallback;
            return this;
        }

        public EngineConfig build() {
            return new EngineConfig(this);
        }
    }
}
