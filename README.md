# event-time-stream-engine-go

本地可运行的事件时间流处理引擎：用内存事件源模拟多分区实时数据流，
在乱序到达、重启恢复与重复投递下保持聚合结论确定且可解释。

## 快速开始

```bash
go build ./...        # 编译
go test ./... -v      # 运行测试（日志中打印事件、窗口与水位线变化）
go run ./cmd/demo     # 本地演示：乱序 + 重启恢复 + 重复投递
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
- **快照**：`Snapshot()` 在已提交水位线处捕获分区水位线、窗口状态与去重表。
- **恢复**：`Restore()` 校验后整体替换状态。以下请求被明确拒绝且原因可区分
  （`*RestoreError.Reason`）：
  - `watermark-regression`：快照的已提交水位线低于当前全局水位线；
  - `partition-conflict`：快照中某分区水位线低于该分区当前水位线。
- 恢复后继续消费时，重投递的事件被恢复的去重表吸收，
  聚合结果不重复、不丢失（`TestRestartRecovery` 与无重启基线逐窗口比对）。

## 并发与内存约束

- **多来源并发注入**：`Inject` 为每个源起一个 goroutine，源内步骤严格按序；
  引擎内部用单互斥锁串行处理，因此同一键内的事件按进入顺序处理，
  同键顺序语义在并发下成立（`-race` 下验证）。
- **幂等写入**：`Sink` 以 `(Key, Window)` 为幂等键，同一结果写多次对外只生效一次；
  引擎的重复投递由事件 ID 去重表吸收，同一事件只影响结果一次。
- **有界内存**：
  - 窗口状态在水位线越过 `窗口结束 + AllowedLateness` 后被清除；
  - 去重表容量由 `MaxSeenIDs` 限制，超出后按 FIFO 驱逐最旧条目
    （容量须大于可能的重投递窗口，否则被驱逐后的重投递无法识别）。
  - `TestBoundedMemory` 验证 10000 个事件后去重表与存活窗口数均有界。

## 本地验证方法

| 验证点 | 命令 |
| --- | --- |
| 乱序到达结果一致 | `go test ./engine/ -run TestOutOfOrderDeterminism -v` |
| 迟到拒绝与修正 | `go test ./engine/ -run TestLateEventRejected -v` |
| 重启恢复无重复无丢失 | `go test ./engine/ -run TestRestartRecovery -v` |
| 非法恢复被拒绝 | `go test ./engine/ -run TestRestoreRejected -v` |
| 重复投递与幂等写入 | `go test ./engine/ -run 'TestDuplicateDelivery|TestSinkIdempotency' -v` |
| 并发与有界内存 | `go test ./engine/ -run 'TestConcurrentSources|TestBoundedMemory' -race -v` |
| 端到端演示 | `go run ./cmd/demo` |

测试日志通过 `Hooks` 打印每条事件的接受/拒绝原因、窗口触发/清除与水位线推进，
便于核对聚合结论的确定性与可解释性。

## 代码结构

```
engine/
  event.go      事件定义
  watermark.go  分水位线跟踪与全局水位线
  window.go     滚动窗口分配与窗口结果
  state.go      快照与恢复错误类型
  sink.go       幂等汇接口与内存实现
  engine.go     引擎核心：摄入、触发、快照、恢复、去重
  source.go     内存多分区事件源与并发注入
cmd/demo/       本地演示程序
```
