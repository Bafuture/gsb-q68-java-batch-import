package com.example.gsb.batchimport.demo;

import com.example.gsb.batchimport.BatchImportEngine;
import com.example.gsb.batchimport.EngineConfig;
import com.example.gsb.batchimport.FileCheckpointStore;
import com.example.gsb.batchimport.GeneratedDataSource;
import com.example.gsb.batchimport.ImportStats;
import com.example.gsb.batchimport.InMemoryWriter;
import com.example.gsb.batchimport.Row;
import java.nio.file.Path;
import java.util.Optional;

/**
 * Tiny end-to-end demo with a generated (never-materialized) data source:
 * every 7th row is invalid.
 *
 * <p>Run after {@code mvn -q package}:
 * <pre>
 * java -cp target/classes com.example.gsb.batchimport.demo.Demo
 * </pre>
 * Re-running resumes from the checkpoint file under {@code target/checkpoints}
 * and writes nothing twice (unless {@code --fresh} is given).
 */
public final class Demo {

    public static void main(String[] args) {
        long totalRows = 100_000;
        boolean fresh = args.length == 1 && "--fresh".equals(args[0]);
        Path checkpointDir = Path.of("target", "checkpoints");
        if (fresh) {
            deleteCheckpoints(checkpointDir);
        }

        var source = new GeneratedDataSource(totalRows,
                line -> "record-" + line);
        var writer = new InMemoryWriter();
        var checkpoints = new FileCheckpointStore(checkpointDir);

        var config = EngineConfig
                .builder("demo-job", Demo::validate, writer, checkpoints)
                .batchSize(1_000)
                .shardCount(4)
                .progressCallback(progress -> {
                    if (progress.processedRows() % 10_000 == 0) {
                        System.out.printf("processed=%d ok=%d failed=%d%n",
                                progress.processedRows(),
                                progress.successCount(),
                                progress.failureCount());
                    }
                })
                .build();

        ImportStats stats = new BatchImportEngine(config).run(source);
        System.out.println("---- report ----");
        System.out.println("total=" + stats.totalRows());
        System.out.println("success=" + stats.successCount());
        System.out.println("failure=" + stats.failureCount());
        System.out.println("duplicate write attempts=" + writer.duplicateAttempts());
        stats.failures().stream().limit(3).forEach(f ->
                System.out.println("  first failures: line=" + f.lineNumber() + " reason=" + f.reason()));
        if (stats.failureCount() > 3) {
            System.out.println("  ... (" + (stats.failureCount() - 3) + " more)");
        }
    }

    private static Optional<String> validate(Row row) {
        if (row.lineNumber() % 7 == 0) {
            return Optional.of("synthetic bad row every 7th line");
        }
        if (row.value() == null || row.value().isBlank()) {
            return Optional.of("blank value");
        }
        return Optional.empty();
    }

    private static void deleteCheckpoints(Path directory) {
        if (!java.nio.file.Files.exists(directory)) {
            return;
        }
        try (var paths = java.nio.file.Files.walk(directory)) {
            paths.sorted(java.util.Comparator.reverseOrder())
                    .forEach(p -> {
                        try {
                            java.nio.file.Files.deleteIfExists(p);
                        } catch (java.io.IOException ignored) {
                            // best effort cleanup for the demo
                        }
                    });
        } catch (java.io.IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Demo() {
    }
}
