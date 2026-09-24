# 批量导入引擎

一个纯内存模拟的流式批量导入引擎：用假「数据源」与假「写入器」演示
几十万行数据的导入流程，不依赖任何数据库。解决旧实现"一次性读入内存
导致 OOM"的问题。

## 运行方式

```bash
./mvnw -q verify   # 或 mvn -q verify
```

## 处理模型

核心类为 `com.example.gsb.batch.BatchImportEngine`，围绕五个可替换的
接口组装：

| 接口 | 作用 | 内存假实现 |
|------|------|-----------|
| `DataSource` | 按 `(offset, limit)` 分片读取 | `InMemoryDataSource`（惰性生成行） |
| `RowValidator` | 单行校验，返回失败原因 | 测试中的 lambda |
| `RowWriter` | 批量写入，按行号幂等 | `InMemoryRowWriter`（唯一键去重） |
| `CheckpointStore` | 保存/加载已提交检查点 | `InMemoryCheckpointStore` |
| `ProgressListener` | 每批处理完回调进度 | 调用方传入 |

工作流程：

1. **流式分片读取**：单线程按 `batchSize` 循环调用
   `DataSource.read(offset, limit)`，直到读到空批次。
2. **有界并发处理**：批次提交到固定线程池（`threads` 个工作线程），
   用信号量限制在途批次不超过 `threads * 2`，因此内存占用上界约为
   `batchSize * threads * 2` 行，与总行数无关（对应测试
   `boundsInFlightRowsRegardlessOfTotalRowCount`）。
3. **行级容错**：单条校验失败仅记录 `FailureDetail`（行号 + 原因），
   同批其余合法行照常写入，整批不中断。
4. **顺序检查点**：检查点按批次**提交顺序**（而非完成顺序）推进，
   保证已提交检查点之前的数据绝不重读。任务中断（写入器抛致命异常）
   后以同一 `CheckpointStore` 重跑即可从检查点续跑；在途但未提交
   检查点的批次会被重放，由幂等 `RowWriter` 去重，不产生重复数据。
5. **进度与报告**：每批完成回调 `ProgressListener`；任务结束返回
   `ImportReport`（成功数、失败数、按行号排序的失败明细）。

## 已知限制

- **检查点粒度为批次**：中断时已处理但未提交检查点的在途批次会被
  重放，依赖写入器幂等去重；非幂等写入器会产生重复数据。
- **失败行不重试**：校验失败的行只记入报告，检查点照常越过它们；
  续跑不会自动重试失败行，需根据报告另行处理。
- **数据源需支持按 offset 重读**：续跑模型要求 `DataSource` 能从任意
  offset 重新读取（如文件、数据库游标）；纯前进式流（如 socket）
  不适用。
- **检查点存储非持久**：`InMemoryCheckpointStore` 只存内存，进程退出
  即丢失；生产使用需替换为数据库表或文件实现。
- **进度回调并发触发**：`ProgressListener` 可能被多个工作线程并发
  调用，实现需自行保证线程安全。
