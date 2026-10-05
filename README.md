# event-time-stream-engine-go

本地可运行的事件时间流处理引擎：用内存事件源模拟多分区实时数据流，
在乱序到达、进程重启、重复投递下保持聚合结论确定且可解释；
进度可落盘跨进程恢复，数据源可按位置续传，状态与历史输出均有界。

## 快速开始

```bash
go build ./...        # 编译
go test ./... -v      # 运行测试（日志中打印事件、窗口与水位线变化）
go run ./cmd/demo     # 本地演示：乱序 + 落盘重启 + 按位置续传 + 结果保留
```

## 事件时间语义

- 每条事件携带 `EventTime`（事件时间，毫秒）与全局唯一 `ID`。
- 窗口为对齐到 0 的**滚动事件时间窗口** `[start, start+WindowSize)`，
  由 `AssignWindow` 按 `EventTime` 分配，与到达时间无关。
- **水位线（watermark）**按分区推进，全局水位线 = 所有分区水位线的最小值，
  且单调不回退（分区水位线回退会被忽略）。
- 窗口在**全局水位线越过窗口结束时间**时触发输出；
  触发顺序按键与窗口起始排序，保证输出顺序确定。
- 同一批事件无论以何种顺序到达，最终窗口结果一致
  （`TestOutOfOrderDeterminism` 用顺序、逆序与多种随机排列验证）。

## 迟到与恢复策略

- **允许迟到** `AllowedLateness`：窗口触发后状态仍保留至
  `窗口结束 + AllowedLateness`。此期间到达的迟到事件被接受，
  并引发**修正性重触发**（汇按幂等键覆盖，最终结果确定）。
- 超过保留期（`窗口结束 + AllowedLateness <= 全局水位线`）的事件被**明确拒绝**，
  返回 `*LateEventError`，窗口状态随即被清除。
- **快照**：`Snapshot()` 在已提交水位线处捕获分区水位线、窗口状态、
  去重表与各分区消费位置。
- **恢复**：`Restore()` 校验后整体替换状态。以下请求被明确拒绝且原因可区分
  （`*RestoreError.Reason`）：
  - `watermark-regression`：快照的已提交水位线低于当前全局水位线；
  - `partition-conflict`：快照中某分区水位线低于该分区当前水位线。
- 恢复后继续消费时，重投递的事件被恢复的去重表吸收，
  聚合结果不重复、不丢失（`TestRestartRecovery` 与无重启基线逐窗口比对）。

## 跨进程持久化与重启续传

- **进度落盘**：`FileCheckpointStore.Save(Snapshot())` 把进度以 JSON 原子写入
  本地文件（先写临时文件再改名）；进程退出后文件仍在，
  新进程用 `Load()` 读回并 `Restore()`，从上次提交的进度继续处理，
  不用从头重来（`TestCheckpointAcrossProcesses` 用子进程模拟进程真的退出再重启，
  最终结论与“一次都没停”的基线逐窗口比对一致）。
- **反复恢复一致**：同一份存盘进度可多次加载恢复，结论稳定一致
  （`TestCheckpointRepeatedRestore`）。
- **格式版本与兼容范围**：
  - `v1`：上一版直接序列化的 `Snapshot`（无版本包装、无消费位置）——
    新版本可识别并恢复，消费位置按“未知”处理（调用方应整批重灌，
    由去重表吸收重复）；
  - `v2`：当前版本，带版本包装，含各分区消费位置 `Offsets`；
  - 更高版本或损坏数据：**明确拒绝**，返回 `*CheckpointError`
    （`Reason` 为 `unsupported-version` / `corrupt`），不猜、不崩
    （`TestCheckpointV1Compatibility`、`TestCheckpointRejected`）。
- **按位置续传**：引擎在快照中记录每个分区的消费位置
  `Position{Seq, LastID}`（已消费事件的最大分区内序号及其 ID）。
  重启后用 `Source.ResumeFrom(pos)` 让数据源从该位置之后继续投递，
  跨过中断点的数据只被算一次（`TestResumeFromOffset`）。
  位置与源中数据对不上时**明确拒绝**且原因可区分（`*SeekError.Reason`）：
  - `offset-beyond-end`：位置比源中现有数据还靠后；
  - `offset-trimmed`：位置指向的数据已不在（源被截断）；
  - `offset-mismatch`：位置指向的数据被换过（同序号不同事件 ID）。
  三种情况都不会悄悄从头重来，也不会悄悄跳过（`TestResumeOffsetRejected`）。

## 并发与容量约束

- **多来源并发注入**：`Inject` 为每个源起一个 goroutine，源内步骤严格按序；
  引擎内部用单互斥锁串行处理，因此同一键内的事件按进入顺序处理，
  同键顺序语义在并发下成立（`-race` 下验证）。
- **幂等写入**：`Sink` 以 `(Key, Window)` 为幂等键，同一结果写多次对外只生效一次；
  引擎的重复投递由事件 ID 去重表吸收，同一事件只影响结果一次。
- **有界状态**：
  - 窗口状态在水位线越过 `窗口结束 + AllowedLateness` 后被清除；
  - 去重表容量由 `MaxSeenIDs` 限制，超出后按 FIFO 驱逐最旧条目
    （容量须大于可能的重投递窗口，否则被驱逐后的重投递无法识别）。
  - `TestBoundedMemory` 验证 10000 个事件后去重表与存活窗口数均有界。
- **结果保留**：`ResultRetention`（毫秒）限制历史输出的堆积。
  窗口结果在 `全局水位线 >= Window.End + AllowedLateness + ResultRetention`
  时被回收（汇需实现 `ExpirableSink`，`MemSink` 已实现）；
  `<=0` 表示永久保留（上一版行为）。保留期内的结果绝不被回收，
  因此迟到事件的修正性重触发不受影响。每次回收通过 `Hooks.OnResultExpire`
  逐条上报（回收了什么、窗口是什么），可解释、可审计
  （`TestResultRetention`、`TestRetentionKeepsCorrectableResults`、
  `TestRetentionDisabledByDefault`）。

## 本地验证方法

| 验证点 | 命令 |
| --- | --- |
| 乱序到达结果一致 | `go test ./engine/ -run TestOutOfOrderDeterminism -v` |
| 迟到拒绝与修正 | `go test ./engine/ -run TestLateEventRejected -v` |
| 重启恢复无重复无丢失 | `go test ./engine/ -run TestRestartRecovery -v` |
| 非法恢复被拒绝 | `go test ./engine/ -run TestRestoreRejected -v` |
| 跨进程退出后重启续传 | `go test ./engine/ -run TestCheckpointAcrossProcesses -v` |
| 反复恢复结论一致 | `go test ./engine/ -run TestCheckpointRepeatedRestore -v` |
| 旧版进度兼容 / 认不了明确拒绝 | `go test ./engine/ -run 'TestCheckpointV1Compatibility\|TestCheckpointRejected' -v` |
| 按位置续传不丢不重 | `go test ./engine/ -run TestResumeFromOffset -v` |
| 位置对不上被明确拒绝 | `go test ./engine/ -run TestResumeOffsetRejected -v` |
| 结果保留与回收可解释 | `go test ./engine/ -run 'TestResultRetention\|TestRetentionKeepsCorrectableResults\|TestRetentionDisabledByDefault' -v` |
| 重复投递与幂等写入 | `go test ./engine/ -run 'TestDuplicateDelivery\|TestSinkIdempotency' -v` |
| 并发与有界内存 | `go test ./engine/ -run 'TestConcurrentSources\|TestBoundedMemory' -race -v` |
| 端到端演示 | `go run ./cmd/demo` |

测试日志通过 `Hooks` 打印每条事件的接受/拒绝原因、窗口触发/清除、
水位线推进、结果回收与续传位置，便于核对聚合结论的确定性与可解释性。

## 代码结构

```
engine/
  event.go      事件定义
  watermark.go  分水位线跟踪与全局水位线
  window.go     滚动窗口分配与窗口结果
  state.go      快照（含消费位置）与恢复错误类型
  checkpoint.go 进度落盘：版本化编解码、文件存储、兼容与拒绝
  offset.go     数据源按位置续传与位置校验
  sink.go       幂等汇接口、内存实现与结果回收
  engine.go     引擎核心：摄入、触发、快照、恢复、去重、保留
  source.go     内存多分区事件源与并发注入
cmd/demo/       本地演示程序
```
