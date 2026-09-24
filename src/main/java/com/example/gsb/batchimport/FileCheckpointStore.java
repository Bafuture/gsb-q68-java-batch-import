package com.example.gsb.batchimport;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * File-backed checkpoint store surviving JVM restarts. One properties file per job is
 * written atomically (tmp file + move) so a crash can never leave a torn file.
 * Synchronized on the file path: only needed for multiple shards of the same job running
 * concurrently in one JVM.
 */
public final class FileCheckpointStore implements CheckpointStore {

    private final Path directory;

    public FileCheckpointStore(Path directory) {
        this.directory = directory;
        try {
            Files.createDirectories(directory);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot create checkpoint directory: " + directory, e);
        }
    }

    private Path fileFor(String jobId) {
        return directory.resolve("checkpoint-" + jobId + ".properties");
    }

    @Override
    public long load(String jobId, String shardId) {
        Path file = fileFor(jobId);
        if (!Files.exists(file)) {
            return 0L;
        }
        Properties props = new Properties();
        try (var in = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            props.load(in);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot read checkpoint file: " + file, e);
        }
        String value = props.getProperty(shardId);
        return value == null ? 0L : Long.parseLong(value);
    }

    @Override
    public void save(String jobId, String shardId, long lineNumber) {
        Path file = fileFor(jobId);
        synchronized (lockFor(file)) {
            Properties props = new Properties();
            if (Files.exists(file)) {
                try (var in = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
                    props.load(in);
                } catch (IOException e) {
                    throw new UncheckedIOException("cannot read checkpoint file: " + file, e);
                }
            }
            props.setProperty(shardId, Long.toString(lineNumber));
            Path tmp;
            try {
                tmp = Files.createTempFile(directory, ".checkpoint-" + jobId + "-", ".tmp");
            } catch (IOException e) {
                throw new UncheckedIOException("cannot create temp checkpoint file", e);
            }
            try (var out = Files.newBufferedWriter(tmp, StandardCharsets.UTF_8)) {
                props.store(out, "batch-import checkpoints for job " + jobId);
            } catch (IOException e) {
                throw new UncheckedIOException("cannot write checkpoint file: " + tmp, e);
            }
            try {
                Files.move(tmp, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException e) {
                try {
                    Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING);
                } catch (IOException ex) {
                    throw new UncheckedIOException("cannot publish checkpoint file: " + file, ex);
                }
            }
        }
    }

    private static Object lockFor(Path file) {
        synchronized (FileCheckpointStore.class) {
            return LOCKS.computeIfAbsent(file.toAbsolutePath().toString(), ignored -> new Object());
        }
    }

    /** Lists checkpoint files, mainly for operational inspection. */
    public List<Path> files() {
        try (var stream = Files.list(directory)) {
            return stream.filter(p -> p.getFileName().toString().startsWith("checkpoint-"))
                    .sorted()
                    .toList();
        } catch (IOException e) {
            throw new UncheckedIOException("cannot list checkpoint directory", e);
        }
    }

    private static final Map<String, Object> LOCKS = new java.util.concurrent.ConcurrentHashMap<>();
}
