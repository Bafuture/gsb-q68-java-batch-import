# 批量导入引擎

一个从零实现的流式批量导入引擎，用内存中的假「数据源」与假「写入器」模拟几十万行文件导入数据库的场景，**不引入任何数据库或第三方依赖**（测试仅用 JUnit 5 与 AssertJ）。

## 构建与验证

```bash
./mvnw -q verify        # 编译 + 13 个测试
java -cp target/classes com.example.gsb.batchimport.demo.Demo --fresh   # 10 万行端到端演示
java -cp target/classes com.example.gsb.batchimport.demo.Demo           # 再跑一次：从检查点续跑，处理 0 行
```

## 处理模型

```
                    ┌── shard-0: reader → 批(≤batchSize) → 校验 → 写入 → 检查点 → 统计 ─┐
 DataSource 分片     │            ▲ 游标推进，只保留当前批                              │
 (连续行号区间)  →   ├── shard-1: ...                                             固定线程池
                    │                                                            1 线程 / shard
                    └── shard-N: ...                                                    │
                                                                                       ▼
                                                          ProgressCallback（每批回调）/ ImportStats
```

- **流式读取**：`DataSource` 按行号切成互不重叠的连续区间（`InMemoryDataSource` 基于内存列表，`GeneratedDataSource` 按需生成、全程不持有任何行，模拟文件游标）。每个 shard 只有一个游标 `ShardReader`，每次 `readBatch(batchSize)` 只物化一批，读完即释放。**任意时刻存活的数据行上界约为 `batchSize × shardCount`，与总行数无关**（由 `StreamingMemoryTest` 用 20 万行断言验证）。
- **多线程按批处理**：固定线程池，每个 shard 一个 worker 线程；shard 内部严格有序，shard 之间完全并行。
- **校验不中断整批**：每批逐行校验，分流为「有效行」与「失败明细」；有效行整批写入，失败行不写入，记录**行号、原值、原因**（`FailureDetail`），处理继续。
- **进度回调与统计报告**：每批提交后以 `Progress(processed/ok/failed)` 回调（注意回调在 worker 线程上触发，需线程安全且轻量）；结束后返回 `ImportStats(total, successCount, failureCount, failures)`，失败明细按行号排序。

### 检查点与 exactly-once 语义

每个 shard 独立维护一个检查点（已完整处理到的行号），提供两种存储：`InMemoryCheckpointStore` 与 `FileCheckpointStore`（每个 job 一个 properties 文件，临时文件 + 原子 move，崩溃不留半截文件）。

每批严格按以下顺序提交，构成批事务边界：

1. 游标读取一批；
2. 逐行校验，分流有效行 / 失败行；
3. 有效行通过**幂等写入器**整批写入（`InMemoryWriter` 按行 key 去重，模拟数据库 upsert）；
4. 持久化本 shard 检查点到批末行号；
5. 才提交成功/失败计数、失败明细与进度回调。

因此：

- **协作式停止**（`engine.requestStop()` 或线程中断，在两批之间生效）后用同一 `jobId` 重新 `run`：已提交的批不会重放，失败明细也不会重复收集，严格 exactly-once（见 `cooperativeStopThenResumeWritesEveryRowExactlyOnce`）。
- **硬性崩溃在写入中途**：该批检查点未落地，续跑会整批重投；写入器按 key 幂等去重，**已落库的行不会重复**，失败明细仍只收集一次（见 `hardCrashMidBatchReplaysBatchButIdempotentWriterDeduplicates`，会断言到 `duplicateAttempts > 0` 且最终无重复）。

## 主要类型

| 类型 | 职责 |
|------|------|
| `DataSource` / `ShardReader` | 可分片的数据源与流式游标 |
| `InMemoryDataSource` / `GeneratedDataSource` | 内存列表假源 / 按需生成的零持有假源 |
| `RowValidator` | 单行校验，返回失败原因 |
| `RecordWriter` / `InMemoryWriter` | 幂等批量写入器及其内存实现 |
| `CheckpointStore` / `InMemory…` / `File…` | 每 shard 行号位点 |
| `BatchImportEngine` + `EngineConfig` | 引擎主体（`batchSize`、`shardCount` 可配） |
| `ProgressCallback` / `Progress` / `ImportStats` / `FailureDetail` | 进度与报告 |
| `demo/Demo` | 10 万行端到端示例 |

## 已知限制

- **写入幂等是前提**：硬崩溃后批会重投，写入器必须按 key 幂等（upsert）；若真实数据库写入非幂等，可能出现重复行。协作式停止（批边界）不受此限。
- **检查点粒度是批**：检查点记录批末行号，崩溃重跑最多重做一个批；批越大重复窗口越大。
- **分片是连续行号区间，需预先知道总行数**；各 shard 数据量均衡，但单条记录处理耗时不均时 worker 之间不会窃取任务。
- **失败明细保存在内存报告中**：几百万条失败时 `ImportStats.failures` 本身占内存；真实场景应改为增量写入失败表/文件。
- **writer 抛异常视为致命错误**：异常包装为 `BatchImportException`，当前批不记检查点，所有 shard 停止；修复后用同一 job 续跑。校验异常不包含在内（校验器应返回原因而非抛异常）。
- **同 job 不可并发跑两个引擎实例**；文件检查点仅保证单 JVM 内按文件加锁，不做跨机器协调。
- `FileCheckpointStore` 的位点文件是简单 properties，面向单机恢复，不保留检查点历史。
