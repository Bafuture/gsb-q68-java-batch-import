# 批量导入引擎

用内存中的假「数据源」与假「写入器」模拟的流式批量导入引擎，不依赖任何数据库。
解决「一次性读进内存导致 OOM」的问题：内存占用只与批大小和在飞批次数有关，不随总行数增长。

## 运行

```bash
mvn -q verify
```

## 处理模型

整体为「单生产者切批 + 有界线程池消费 + 有序检查点提交」的流水线：

1. **流式切批**：`ImportEngine` 从 `DataSource.readFrom(checkpoint)` 拿到惰性迭代器，
   按 `batchSize` 逐批拉取（`drainBatch`）。数据源只在迭代器被消费时才物化行，
   引擎任何时刻只持有 `batchSize × maxInFlightBatches` 条记录。
2. **背压**：提交批次前必须获取信号量许可（`maxInFlightBatches`，默认 `线程数 × 2`）。
   消费跟不上时读取线程阻塞，内存占用有硬上界。
3. **并行处理**：批次提交到固定大小线程池。批内逐条调用 `RecordValidator`：
   失败的行记录 `(行号, 原因)` 到失败明细并继续；通过的记录整批交给 `RecordWriter`。
4. **有序检查点**：批次可乱序完成，但检查点按批序号严格连续推进——只有所有前序
   批次都提交后，检查点才前移并写入 `CheckpointStore`。因此中断后从检查点续跑，
   已提交的数据不会被重复写入。
5. **进度与报告**：每次检查点推进触发 `ProgressListener.onProgress(ProgressSnapshot)`；
   `run()` 返回 `ImportReport`（成功数、失败数、失败明细、是否被中断）。

行号为 0 起始的全局序号，由数据源在读取时分配，失败明细与检查点均基于该序号。

## 中断与续跑

- `requestStop()`：停止切新批，在飞批次处理完毕并有序提交后退出，检查点保持一致。
- 续跑：用同一个 `CheckpointStore` 新建 `ImportEngine` 再调 `run()`，自动从
  `checkpoint.nextRow` 继续。检查点语义为「该行号之前的批次均已提交」，故无重复写入。

## 使用示例

```java
DataSource source = new InMemoryDataSource(rows);
RecordWriter writer = new InMemoryRecordWriter();
CheckpointStore store = new InMemoryCheckpointStore();

ImportEngine engine = new ImportEngine(
        source,
        payload -> payload.isBlank() ? Optional.of("blank payload") : Optional.empty(),
        writer, store,
        ImportConfig.of(500, 4),                       // 批大小 500，4 线程
        snapshot -> log("committed=" + snapshot.committedRow()));

ImportReport report = engine.run();                    // 成功数 / 失败数 / 失败明细
```

## 已知限制

- **检查点存储在内存**：`InMemoryCheckpointStore` 进程退出即丢失，仅用于演示；
  生产使用需实现持久化 `CheckpointStore`（文件 / 数据库）。
- **检查点粒度为批**：崩溃（非 `requestStop` 的优雅停止）时，已写入但尚未提交
  检查点的在飞批次会在续跑时重放，写入器需自行幂等（如按业务主键去重）。
  优雅停止路径不存在该问题。
- **整批写入失败降级为逐行失败**：`RecordWriter.writeBatch` 抛异常时，该批全部
  记录按失败处理（行号 + 异常原因）并跳过，不会重试。
- **读取端单线程**：切批是单线程顺序的，并行只发生在批处理阶段；读取本身是
  瓶颈时需分片数据源（当前未实现）。
- **失败明细全量驻留内存**：`ImportReport.failures` 随失败行数线性增长；
  失败率极高的场景应改为流式落盘。
