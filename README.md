# event-time-stream-engine-go

本地可运行的事件时间流处理引擎：用内存/可续传事件源模拟多分区实时数据流，
在乱序到达、**跨进程退出重启**、**按位置续传**、重复投递与**长时间运行的容量
约束**下保持聚合结论确定且可解释。

## 快速开始

```bash
go build ./...          # 编译
go test ./... -v        # 运行全部测试（日志打印场景、事件、窗口与回收变化）
go run ./cmd/demo       # 单进程演示：乱序 + 进程内恢复 + 重复投递

# 真正“两个进程”的跨退出续传演示：
go run ./cmd/worker -dir /tmp/wd -phase 1   # 进程1：跑到中途，提交检查点后退出
go run ./cmd/worker -dir /tmp/wd -phase 2   # 进程2：全新进程，从磁盘检查点续传
cat /tmp/wd/results.json                    # 最终结果
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

## 持久化检查点：进程真的退出后还能接着跑

旧版本只能在**同一进程内**新建引擎读内存快照。新版本把“引擎状态 + 数据源
位置”打成一次**原子提交**的检查点落盘：

```go
// 提交：引擎快照 + 各分区位置与锚点写进同一个文件（临时文件 + fsync + 改名）
cp := engine.Checkpoint{
    Snapshot:  eng.Snapshot(),
    Positions: map[int]uint64{0: p0.Position(), 1: p1.Position()},
    Anchors:   map[int]string{0: p0.Anchor(),   1: p1.Anchor()},
}
_ = engine.SaveCheckpointFile(dir+"/checkpoint.json", cp)

// 全新进程：先读检查点恢复引擎，再用同一位置打开数据源
cp, err := engine.LoadCheckpointFile(dir + "/checkpoint.json")
eng.Restore(cp.Snapshot)
engine.OpenPositions(cp, p0, p1)        // 位置校验不过会明确报错（见下）
engine.InjectRecorded(eng, p0, p1)      // 只投递未处理后缀
```

- 检查点文件带 `format/version` 信封与内容校验和；`SaveCheckpointFile`
  原子落盘，写到一半进程被杀也不会留下半个检查点。
- **最终结论与“一次都没停”完全一致，不丢不重**
  （`TestResumeExactlyOnce` 逐窗口比对；`TestCrossProcessRestart` 真正
  `os/exec` 两次拉起独立二进制验证）。
- **同一份检查点反复恢复结论稳定**（`TestResumeRepeatedStable` 与
  `TestCrossProcessRestart` 末尾的第三次打开：位置已到末尾，不再投递）。
- 汇也要持久化才能跨进程：`FileSink` 把结果幂等写入 `results.json`，
  新进程 `OpenFileSink` 直接读回；迟到修正按 `(Key,Window)` 幂等覆盖。

### 与旧版本进度的兼容范围

| 落盘内容 | 新版本行为 |
| --- | --- |
| v2 带信封检查点（含位置/锚点/校验和） | 直接续传，按位置只投后缀 |
| **v1 裸 `Snapshot`**（旧版本 `json.Marshal(Snapshot())` 的形状） | **能认**：识别为 version=1，恢复引擎状态；因没有数据位置，按“从头重放 + 去重表吸收”语义恢复，结论不重（`TestCheckpointLegacyV1`、`TestCrossProcessAcceptsLegacyV1`） |
| 更高版本（`version > 2`） | **明确拒绝** `ErrUnsupportedVersion`，不猜测不崩（`TestCheckpointUnknownVersion`） |
| 字节损坏 / 魔数不符 / 校验和不符 | **明确拒绝** `ErrCheckpointCorrupt`（`TestCheckpointCorrupt`、`TestCheckpointChecksumMismatch`） |
| 文件不存在（首次运行） | 返回可辨识的 `ErrNoCheckpoint`，按全新启动处理 |

> 注意：`Snapshot` 的 JSON 字段名（`watermarks/committed/windows/seen`）
> 即 v1 落盘形状，改字段名会破坏向前兼容。

## 从续传位置接着读：不丢不重，对不上就拒绝

`RecordedPartition` 给每条记录分配从 0 开始的偏移，并为“已投递最后一条记录”
保存**内容指纹（anchor）**。`Open(start, anchor)` 有三种结果：

- 位置与数据对得上：光标定位到 `start`，`Next()` 只返回未处理后缀，
  **跨中断点的记录恰好投一次**；
- `start` 比当前记录数还靠后：返回 `*PositionError`，
  `Reason=position-ahead`；
- `start-1` 处记录指纹与锚点不符（数据被截断重写/换过），或检查点里的分区
  在当前数据里整体消失：`Reason=position-changed`。

两种不匹配都**明确拒绝**，错误原因一眼可分（`errors.As` 取 `*PositionError`
后比较 `Reason`）；引擎不会悄悄从头重来，也不会悄悄跳过
（`TestResumePositionAhead` / `TestResumePositionChanged` /
`TestResumeMissingPartition` / `TestCrossProcessRejectsTamperedSource`）。

> 指纹只覆盖记录内容（事件字段或水位线值），不含偏移本身。

## 容量与保留：输出历史也不能无限堆

```go
sink := engine.NewRetainingMemSink(engine.RetentionPolicy{RetainMillis: 1000})
// 或落盘：engine.OpenFileSink(path, engine.RetentionPolicy{RetainMillis: 1000})
```

- 水位线每次推进（以及每次恢复）后，引擎自动通知汇回收；结果在
  `窗口结束 + RetainMillis <= 全局水位线` 时过期删除。
- **正确性保护下限**：实际保留时长取 `RetainMillis` 与 `AllowedLateness`
  的较大者——仍在迟到保留期内、后面可能被修正或重放读取的结果**绝不会**
  被回收（`TestRetentionProtectsCorrectableResults`）。
- `RetainMillis <= 0` 表示不主动回收（仅适合短任务，输出会无限增长）。
- 每次回收产生 `ReclaimReport`：在什么水位线、生效保留期、回收前后数量、
  逐条 `ReclaimEntry`（键、窗口、原因），通过 `Hooks.OnReclaim` 可观测，
  “回收了什么、为什么回收”可解释（`TestRetentionWiredIntoEngine`）。
- `FileSink` 的回收同样原子落盘，重开文件后旧结果确实不在了
  （`TestFileSinkExpirePersists`）。
- 处理中状态的有界性维持不变：窗口状态越过迟到保留期即清除，
  去重表由 `MaxSeenIDs` FIFO 限容（`TestBoundedMemory`、
  `TestRetentionBoundedUnderLongRun` 验证 200 个窗口后输出数量仍有界）。

## 并发与幂等

- **多来源并发注入**：`Inject` / `InjectRecorded` 为每个源起一个 goroutine，
  源内步骤严格按序；引擎单锁串行，同键顺序语义在 `-race` 下成立。
- **幂等写入**：汇以 `(Key, Window)` 为幂等键覆盖；重复投递由事件 ID
  去重表吸收，同一事件只影响结果一次。
- 检查点提交顺序约定：**先处理、推进水位线，再提交检查点**。若在提交前
  崩溃，重启会从上一个检查点位置重放一小段，已在去重表里的事件被吸收；
  `MaxSeenIDs` 需大于可能的重投递窗口。

## 本地验证方法

| 验证点 | 命令 |
| --- | --- |
| 检查点 v2 往返 / v1 兼容 / 版本与损坏拒绝 | `go test ./engine/ -run 'TestCheckpoint' -v` |
| 检查点文件原子落盘与覆盖 | `go test ./engine/ -run 'TestCheckpointFile' -v` |
| 按位置续传不丢不重 / 反复恢复稳定 | `go test ./engine/ -run 'TestResumeExactlyOnce|TestResumeRepeatedStable' -v` |
| 位置越界/数据被换/分区消失被拒绝 | `go test ./engine/ -run 'TestResumePosition|TestResumeMissingPartition|TestResumeAnchorAtZero' -v` |
| **真实进程退出后重启续传**（起两个进程） | `go test ./engine/ -run 'TestCrossProcessRestart' -v` |
| 跨进程拒绝换数据 / 拒绝新版本 / 认 v1 | `go test ./engine/ -run 'TestCrossProcessRejects|TestCrossProcessAcceptsLegacyV1' -v` |
| 保留回收生效 / 保护期内不回收 / 长跑有界 | `go test ./engine/ -run 'TestRetention|TestFileSink' -v` |
| 现有能力无回归（乱序/迟到/去重/并发） | `go test ./... -race -v` |
| 手工两步演示 | `go run ./cmd/worker -dir /tmp/wd -phase 1 && go run ./cmd/worker -dir /tmp/wd -phase 2` |

`go test -v` 日志用 `SCENARIO ...` / `PASS ...`、`EVENT/WM/WINDOW/RECLAIM`
等前缀标出覆盖的场景、每个用例结论与回收明细，便于核对。

## 代码结构

```
engine/
  event.go              事件定义
  watermark.go          分水位线跟踪与全局水位线
  window.go             滚动窗口分配与窗口结果
  state.go              快照、恢复错误类型、Reclaimer 接口
  sink.go               幂等汇接口与内存实现
  retention.go          保留策略、回收报告、带回收的内存汇
  file_sink.go          落盘幂等汇（跨进程持久化结果）
  checkpoint.go         带版本检查点编解码（v2 信封 + v1 裸快照兼容）
  checkpoint_store.go   检查点原子落盘/读取（fsync + 改名）
  resume.go             偏移位置续传、锚点指纹校验、PositionError
  engine.go             引擎核心：摄入、触发、快照、恢复、去重、驱动回收
  source.go             内存多分区事件源与并发注入
cmd/demo/               单进程演示
cmd/worker/             两进程“退出再重启”续传演示（测试也用它起真实进程）
```
