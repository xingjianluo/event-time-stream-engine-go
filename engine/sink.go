package engine

import (
	"sort"
	"time"
)

// Sink 是输出汇接口。
type Sink interface {
	// Write 幂等写入一条窗口结果。
	Write(r WindowResult, version int64)
}

// idempotentSink 内存输出汇。
//
// 幂等语义：以 (key, windowStart) 为主键，仅接受更新版本号的写入。
//   - version 相同（重启后重复触发）-> 忽略；
//   - version 更新（迟到事件产生修正结果）-> 覆盖；
//   - version 更旧 -> 拒绝忽略。
//
// 因此同一事件被重复投递时，结果只受一次影响。
type idempotentSink struct {
	entries map[windowKey]ResultLogEntry
}

func newIdempotentSink() *idempotentSink {
	return &idempotentSink{entries: make(map[windowKey]ResultLogEntry)}
}

// Write 实现幂等 upsert，返回是否实际写入。
func (s *idempotentSink) Write(r WindowResult, version int64) bool {
	k := windowKey{key: r.Key, start: r.WindowStart}
	if old, ok := s.entries[k]; ok && old.Version >= version {
		return false
	}
	s.entries[k] = ResultLogEntry{Result: r, Version: version}
	return true
}

func (s *idempotentSink) snapshot() map[windowKey]ResultLogEntry {
	out := make(map[windowKey]ResultLogEntry, len(s.entries))
	for k, v := range s.entries {
		out[k] = v
	}
	return out
}

func (s *idempotentSink) restore(entries map[windowKey]ResultLogEntry) {
	s.entries = make(map[windowKey]ResultLogEntry, len(entries))
	for k, v := range entries {
		s.entries[k] = v
	}
}

// orderedEntries 按 (窗口起点, key) 排序返回，保证日志与快照确定性。
func (s *idempotentSink) orderedEntries() []ResultLogEntry {
	out := make([]ResultLogEntry, 0, len(s.entries))
	for _, v := range s.entries {
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i].Result, out[j].Result
		if !a.WindowStart.Equal(b.WindowStart) {
			return a.WindowStart.Before(b.WindowStart)
		}
		return a.Key < b.Key
	})
	return out
}

// purgeBefore 删除窗口结束时间 <= cutoff 的结果，保证输出汇内存有界。
// 调用时机：窗口彻底关闭（end+allowedLateness <= wm）后，
// 其最终结果不再可能被任何事件修正。
// 需要永久归档结果的场景应自行实现持久化 Sink（Sink 接口）。
func (s *idempotentSink) purgeBefore(cutoff time.Time) {
	for k, v := range s.entries {
		if !v.Result.WindowEnd.After(cutoff) {
			delete(s.entries, k)
		}
	}
}

// SnapshotView 供外部读取结果：结果 -> 版本号（确定性排序由 Results 提供）。
func (e *Engine) SnapshotView() map[WindowResult]int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make(map[WindowResult]int64, len(e.sink.entries))
	for _, v := range e.sink.entries {
		out[v.Result] = v.Version
	}
	return out
}

// Results 按 (窗口起点, key) 确定性顺序返回当前汇中的结果与版本号。
func (e *Engine) Results() []ResultLogEntry {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.sink.orderedEntries()
}
