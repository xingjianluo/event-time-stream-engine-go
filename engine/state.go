package engine

// Reclaimer 是可选的汇能力：按全局水位线回收不再需要的历史结果。
// 实现方必须保证保留期（至少覆盖允许迟到时间）内的结果不被回收。
type Reclaimer interface {
	Expire(globalWatermark, protectedMillis int64) ReclaimReport
}

// windowState 是单个 (键, 窗口) 的聚合状态。
type windowState struct {
	Sum   int64
	Count int64
	Fired bool // 是否已触发过输出（迟到更新会再次触发）
}

// Snapshot 是引擎在某一已提交水位线处的完整状态快照。
// JSON 字段名同时是旧版本（v1 裸快照）的落盘形状，修改字段名会破坏向前兼容。
type Snapshot struct {
	Watermarks map[int]int64                    `json:"watermarks"` // 各分区水位线
	Committed  int64                            `json:"committed"`  // 快照对应的全局水位线
	Windows    map[string]map[int64]windowState `json:"windows"`    // key -> windowStart -> 状态
	Seen       []string                         `json:"seen"`       // 已见事件 ID（去重表）
}

// RestoreError 描述一次被拒绝的恢复请求，Reason 可区分。
type RestoreError struct {
	Reason string
	Detail string
}

func (e *RestoreError) Error() string {
	return "restore rejected [" + e.Reason + "]: " + e.Detail
}

// 恢复拒绝原因，可用 errors.As 拿到 *RestoreError 后比较 Reason。
const (
	ReasonWatermarkRegression = "watermark-regression" // 快照水位线低于当前已提交水位线
	ReasonPartitionConflict   = "partition-conflict"   // 快照中某分区水位线与当前状态冲突
)
