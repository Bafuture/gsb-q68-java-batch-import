package com.example.gsb.batchimport;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.LongStream;
import org.junit.jupiter.api.Test;

class ShardedReadTest {

    private static List<String> values(long n) {
        return LongStream.rangeClosed(1, n).mapToObj("v%d"::formatted).toList();
    }

    private static List<Row> drain(ShardReader reader, int batchSize) {
        List<Row> all = new ArrayList<>();
        List<Row> batch;
        while (!(batch = reader.readBatch(batchSize)).isEmpty()) {
            assertThat(batch).hasSizeLessThanOrEqualTo(batchSize);
            all.addAll(batch);
        }
        return all;
    }

    @Test
    void shardsAreContiguousDisjointAndComplete() {
        int shardCount = 3;
        long total = 1007; // base=335, remainder=2 -> 336 / 336 / 335
        InMemoryDataSource source = new InMemoryDataSource(values(total));

        List<Long> allLines = new ArrayList<>();
        for (int shard = 0; shard < shardCount; shard++) {
            try (ShardReader reader = source.openShard(shard, shardCount, 0)) {
                List<Row> rows = drain(reader, 100);
                List<Long> lines = rows.stream().map(Row::lineNumber).toList();
                if (shard == 0) {
                    assertThat(lines).first().isEqualTo(1L);
                    assertThat(lines).last().isEqualTo(336L);
                } else if (shard == 1) {
                    assertThat(lines).first().isEqualTo(337L);
                    assertThat(lines).last().isEqualTo(672L);
                } else {
                    assertThat(lines).first().isEqualTo(673L);
                    assertThat(lines).last().isEqualTo(1007L);
                }
                allLines.addAll(lines);
            }
        }

        assertThat(allLines)
                .hasSize((int) total)
                .doesNotHaveDuplicates()
                .isSorted()
                .containsExactlyElementsOf(LongStream.rangeClosed(1, total).boxed().toList());
    }

    @Test
    void readsInBatchesNoLargerThanBatchSize() {
        InMemoryDataSource source = new InMemoryDataSource(values(50));
        try (ShardReader reader = source.openShard(0, 1, 0)) {
            assertThat(reader.readBatch(16)).hasSize(16);
            assertThat(reader.readBatch(16)).hasSize(16);
            assertThat(reader.readBatch(16)).hasSize(16);
            assertThat(reader.readBatch(16)).hasSize(2);
            assertThat(reader.readBatch(16)).isEmpty();
        }
    }

    @Test
    void checkpointSkipsAlreadyProcessedLines() {
        InMemoryDataSource source = new InMemoryDataSource(values(1007));
        // shard 1 covers lines 337..672; resume after line 400 must start at 401
        try (ShardReader reader = source.openShard(1, 3, 400)) {
            List<Row> rows = drain(reader, 50);
            assertThat(rows).isNotEmpty();
            assertThat(rows.get(0).lineNumber()).isEqualTo(401L);
            assertThat(rows.get(rows.size() - 1).lineNumber()).isEqualTo(672L);
        }
    }

    @Test
    void generatedSourceShardsIdentically() {
        int shardCount = 4;
        GeneratedDataSource source = new GeneratedDataSource(2_500, line -> "g" + line);

        List<Long> allLines = new ArrayList<>();
        for (int shard = 0; shard < shardCount; shard++) {
            try (ShardReader reader = source.openShard(shard, shardCount, 0)) {
                List<Row> rows = drain(reader, 137);
                assertThat(rows).allSatisfy(row ->
                        assertThat(row.value()).isEqualTo("g" + row.lineNumber()));
                allLines.addAll(rows.stream().map(Row::lineNumber).toList());
            }
        }
        assertThat(allLines)
                .hasSize(2500)
                .doesNotHaveDuplicates()
                .containsExactlyElementsOf(LongStream.rangeClosed(1, 2500).boxed().toList());
    }
}
