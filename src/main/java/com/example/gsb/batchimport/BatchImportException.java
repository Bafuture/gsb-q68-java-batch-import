package com.example.gsb.batchimport;

/** Fatal engine failure, typically a writer error. Checkpoints remain usable for resume. */
public class BatchImportException extends RuntimeException {

    public BatchImportException(String message, Throwable cause) {
        super(message, cause);
    }
}
