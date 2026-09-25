package engine

import "sort"

// WatermarkTracker 跟踪各分区水位线并计算全局水位线。
// 全局水位线 = 所有已注册分区水位线的最小值，且单调不回退。
type WatermarkTracker struct {
	perPartition map[int]int64
	global       int64
}

// NewWatermarkTracker 创建水位线跟踪器。
func NewWatermarkTracker() *WatermarkTracker {
	return &WatermarkTracker{perPartition: make(map[int]int64), global: -1}
}

// Advance 推进指定分区的水位线。
// 分区水位线回退会被忽略（返回 false）；返回全局水位线是否前进。
func (w *WatermarkTracker) Advance(partition int, wm int64) bool {
	cur, ok := w.perPartition[partition]
	if ok && wm < cur {
		return false // 分区水位线不允许回退
	}
	w.perPartition[partition] = wm
	return w.recompute()
}

func (w *WatermarkTracker) recompute() bool {
	if len(w.perPartition) == 0 {
		return false
	}
	vals := make([]int64, 0, len(w.perPartition))
	for _, v := range w.perPartition {
		vals = append(vals, v)
	}
	sort.Slice(vals, func(i, j int) bool { return vals[i] < vals[j] })
	min := vals[0]
	if min > w.global {
		w.global = min
		return true
	}
	return false
}

// Global 返回当前全局水位线。
func (w *WatermarkTracker) Global() int64 { return w.global }

// load 从快照整体恢复各分区水位线并重算全局水位线。
// 与逐条 Advance 不同，load 不受分区恢复顺序影响。
func (w *WatermarkTracker) load(m map[int]int64) {
	w.perPartition = make(map[int]int64, len(m))
	for k, v := range m {
		w.perPartition[k] = v
	}
	w.global = -1
	w.recompute()
}

// Snapshot 返回各分区水位线副本。
func (w *WatermarkTracker) Snapshot() map[int]int64 {
	out := make(map[int]int64, len(w.perPartition))
	for k, v := range w.perPartition {
		out[k] = v
	}
	return out
}
