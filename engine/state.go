package engine

// windowState 是单个 (键, 窗口) 的聚合状态。
type windowState struct {
	Sum   int64
	Count int64
	Fired bool // 是否已触发过输出（迟到更新会再次触发）
}

// Snapshot 是引擎在某一已提交水位线处的完整状态快照。
// JSON 标签用于跨进程持久化；v1 进度没有 offsets 字段，解码后为零值（空表），
// 引擎按“位置未知”处理，不影响状态恢复本身。
type Snapshot struct {
	Watermarks map[int]int64                    `json:"watermarks"`        // 各分区水位线
	Committed  int64                            `json:"committed"`         // 快照对应的全局水位线
	Windows    map[string]map[int64]windowState `json:"windows"`           // key -> windowStart -> 状态
	Seen       []string                         `json:"seen"`              // 已见事件 ID（去重表）
	Offsets    map[int]Position                 `json:"offsets,omitempty"` // 各分区已提交消费位置（v2 起）
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
