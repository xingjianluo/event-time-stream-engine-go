package engine

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"time"
)

// windowSnap 是单个窗口的可序列化状态。
type windowSnap struct {
	Key     string    `json:"key"`
	Start   time.Time `json:"start"`
	End     time.Time `json:"end"`
	Sum     int64     `json:"sum"`
	Count   int64     `json:"count"`
	Version int64     `json:"version"`
}

// sinkSnap 是输出汇一条记录的可序列化形式。
type sinkSnap struct {
	Key     string       `json:"key"`
	Start   time.Time    `json:"start"`
	Result  WindowResult `json:"result"`
	Version int64        `json:"version"`
}

// Checkpoint 是引擎在某个已提交水位线处的完整状态快照。
type Checkpoint struct {
	CommittedWatermark  time.Time    `json:"committed_watermark"`
	PartitionWatermarks []pwmSnap    `json:"partition_watermarks"`
	Offsets             []offsetSnap `json:"offsets"`
	Windows             []windowSnap `json:"windows"`
	DedupEntries        []dedupSnap  `json:"dedup_entries"`
	DedupMinTimestamp   time.Time    `json:"dedup_min_timestamp"`
	SinkEntries         []sinkSnap   `json:"sink_entries"`
}

type pwmSnap struct {
	Partition int       `json:"partition"`
	Watermark time.Time `json:"watermark"`
}

type offsetSnap struct {
	Partition int   `json:"partition"`
	Offset    int64 `json:"offset"`
}

type dedupSnap struct {
	ID        string    `json:"id"`
	EventTime time.Time `json:"event_time"`
}

// Snapshot 导出引擎当前状态（字段均排序，保证快照字节级确定）。
func (e *Engine) Snapshot() *Checkpoint {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.snapshotLocked()
}

func (e *Engine) snapshotLocked() *Checkpoint {
	cp := &Checkpoint{
		CommittedWatermark: e.committedWM,
		DedupMinTimestamp:  e.dedup.minTimestamp,
	}

	parts := make([]int, 0, len(e.partitionWM))
	for p := range e.partitionWM {
		parts = append(parts, p)
	}
	sort.Ints(parts)
	for _, p := range parts {
		cp.PartitionWatermarks = append(cp.PartitionWatermarks,
			pwmSnap{Partition: p, Watermark: e.partitionWM[p]})
	}

	offs := make([]int, 0, len(e.offsets))
	for p := range e.offsets {
		offs = append(offs, p)
	}
	sort.Ints(offs)
	for _, p := range offs {
		cp.Offsets = append(cp.Offsets, offsetSnap{Partition: p, Offset: e.offsets[p]})
	}

	// 窗口：按 (start, key) 排序。
	wks := make([]windowKey, 0, len(e.windows.windows))
	for k := range e.windows.windows {
		wks = append(wks, k)
	}
	sort.Slice(wks, func(i, j int) bool {
		if !wks[i].start.Equal(wks[j].start) {
			return wks[i].start.Before(wks[j].start)
		}
		return wks[i].key < wks[j].key
	})
	for _, k := range wks {
		st := e.windows.windows[k]
		sum, count := st.agg.Value()
		cp.Windows = append(cp.Windows, windowSnap{
			Key: st.key, Start: st.start, End: st.end,
			Sum: sum, Count: count, Version: st.version,
		})
	}

	entries, _ := e.dedup.snapshot()
	for _, en := range entries {
		cp.DedupEntries = append(cp.DedupEntries,
			dedupSnap{ID: en.id, EventTime: en.eventTime})
	}

	cp.SinkEntries = make([]sinkSnap, 0, len(e.sink.entries))
	for _, v := range e.sink.orderedEntries() {
		cp.SinkEntries = append(cp.SinkEntries, sinkSnap{
			Key: v.Result.Key, Start: v.Result.WindowStart,
			Result: v.Result, Version: v.Version,
		})
	}
	return cp
}

// Restore 从快照恢复引擎状态。
//
// 明确拒绝两类非法恢复：
//  1. 快照水位线落后于引擎当前已提交水位线（重启只能回到不超过当前的提交点）；
//  2. 引擎已产生与快照冲突的输出/窗口状态（例如快照与当前结果对同一窗口
//     给出不一致版本号）。
func (e *Engine) Restore(cp *Checkpoint) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.committedWM.After(cp.CommittedWatermark) {
		return fmt.Errorf("%w: current=%s snapshot=%s",
			ErrWatermarkRegression,
			e.committedWM.Format(time.RFC3339Nano),
			cp.CommittedWatermark.Format(time.RFC3339Nano))
	}
	// 冲突检测：同一 (key,window) 当前版本不得高于快照版本，
	// 否则恢复会丢弃已确认输出。
	for _, se := range cp.SinkEntries {
		k := windowKey{key: se.Key, start: se.Start}
		if cur, ok := e.sink.entries[k]; ok && cur.Version > se.Version {
			return fmt.Errorf("%w: key=%s window=%s currentVersion=%d snapshotVersion=%d",
				ErrSnapshotConflict, se.Key, se.Start.Format(time.RFC3339Nano),
				cur.Version, se.Version)
		}
	}

	windows := newTumblingWindows(e.cfg.WindowSize)
	for _, w := range cp.Windows {
		// count 需精确还原，使用可直接装载 sum/count 的聚合器。
		st := &windowState{
			key: w.Key, start: w.Start, end: w.End,
			agg:     &restoredAggregator{sum: w.Sum, count: w.Count},
			version: w.Version,
		}
		windows.windows[windowKey{key: w.Key, start: w.Start}] = st
	}

	dedupEntries := make([]dedupEntry, 0, len(cp.DedupEntries))
	for _, d := range cp.DedupEntries {
		dedupEntries = append(dedupEntries, dedupEntry{id: d.ID, eventTime: d.EventTime})
	}

	sink := newIdempotentSink()
	for _, se := range cp.SinkEntries {
		sink.entries[windowKey{key: se.Key, start: se.Start}] =
			ResultLogEntry{Result: se.Result, Version: se.Version}
	}

	e.windows = windows
	e.dedup.restore(dedupEntries, cp.DedupMinTimestamp)
	e.sink = sink
	e.partitionWM = make(map[int]time.Time)
	for _, p := range cp.PartitionWatermarks {
		e.partitionWM[p.Partition] = p.Watermark
	}
	e.offsets = make(map[int]int64)
	for _, o := range cp.Offsets {
		e.offsets[o.Partition] = o.Offset
	}
	e.committedWM = cp.CommittedWatermark
	return nil
}

// restoredAggregator 从快照精确还原 sum/count 的聚合器。
type restoredAggregator struct {
	sum   int64
	count int64
}

func (a *restoredAggregator) Add(v int64)           { a.sum += v; a.count++ }
func (a *restoredAggregator) Value() (int64, int64) { return a.sum, a.count }

// SaveCheckpoint 将快照写入 JSON 文件（先内存序列化再原子重命名）。
func SaveCheckpoint(path string, cp *Checkpoint) error {
	data, err := json.MarshalIndent(cp, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// LoadCheckpoint 从 JSON 文件读取快照。
func LoadCheckpoint(path string) (*Checkpoint, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cp Checkpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return nil, err
	}
	return &cp, nil
}
