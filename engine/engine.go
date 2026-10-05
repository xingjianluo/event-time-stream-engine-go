package engine

import (
	"fmt"
	"sort"
	"sync"
)

// LateEventError 表示事件因超过允许迟到时间被明确拒绝。
type LateEventError struct {
	Event     Event
	Window    Window
	Watermark int64
}

func (e *LateEventError) Error() string {
	return fmt.Sprintf("late event rejected: id=%s key=%s eventTime=%d window=%s watermark=%d",
		e.Event.ID, e.Event.Key, e.Event.EventTime, e.Window, e.Watermark)
}

// Config 配置引擎行为。
type Config struct {
	WindowSize      int64 // 滚动窗口大小（毫秒）
	AllowedLateness int64 // 允许迟到时间（毫秒）
	MaxSeenIDs      int   // 去重表容量上限，<=0 表示不限制（有界内存约束下应设正数）
}

// Hooks 是可选的观测钩子，用于测试日志打印事件、窗口与水位线变化。
type Hooks struct {
	OnEvent     func(ev Event, accepted bool, reason string)
	OnEmit      func(r WindowResult)
	OnWatermark func(partition int, wm int64, global int64)
	OnPurge     func(key string, w Window)
	OnReclaim   func(report ReclaimReport)
}

// Engine 是事件时间流处理引擎。
// 所有方法均持有同一把互斥锁，因此并发注入下同一键内的事件
// 按进入 Ingest 的顺序被串行处理，保证同键顺序语义。
type Engine struct {
	mu      sync.Mutex
	cfg     Config
	tracker *WatermarkTracker
	windows map[string]map[int64]*windowState // key -> windowStart -> 状态
	seen    map[string]struct{}               // 幂等去重表
	seenOrd []string                          // FIFO 驱逐顺序，保证有界内存
	sink    Sink
	hooks   Hooks
}

// NewEngine 创建引擎。
func NewEngine(cfg Config, sink Sink, hooks Hooks) *Engine {
	return &Engine{
		cfg:     cfg,
		tracker: NewWatermarkTracker(),
		windows: make(map[string]map[int64]*windowState),
		seen:    make(map[string]struct{}),
		sink:    sink,
		hooks:   hooks,
	}
}

// Ingest 处理一条事件。重复事件被幂等吸收（返回 nil 且不重复计数）；
// 超过允许迟到的事件返回 *LateEventError。
func (e *Engine) Ingest(ev Event) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, dup := e.seen[ev.ID]; dup {
		if e.hooks.OnEvent != nil {
			e.hooks.OnEvent(ev, false, "duplicate")
		}
		return nil // 幂等吸收重复投递
	}

	w := AssignWindow(ev.EventTime, e.cfg.WindowSize)
	global := e.tracker.Global()
	// 窗口的迟到保留期已过：状态已清除，明确拒绝。
	if w.End+e.cfg.AllowedLateness <= global {
		if e.hooks.OnEvent != nil {
			e.hooks.OnEvent(ev, false, "too-late")
		}
		return &LateEventError{Event: ev, Window: w, Watermark: global}
	}

	e.markSeen(ev.ID)
	ws := e.windowState(ev.Key, w.Start)
	ws.Sum += ev.Value
	ws.Count++
	if e.hooks.OnEvent != nil {
		e.hooks.OnEvent(ev, true, "")
	}

	// 窗口已触发过但仍在迟到保留期内：迟到事件引发修正性重触发，
	// 汇的幂等覆盖保证最终结果确定。
	if ws.Fired {
		e.emit(ev.Key, w, ws)
	}
	return nil
}

// AdvanceWatermark 推进指定分区水位线；全局水位线前进时触发到期窗口，
// 并清除超出迟到保留期的窗口状态（有界内存）。
func (e *Engine) AdvanceWatermark(partition int, wm int64) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.tracker.Advance(partition, wm) {
		return
	}
	global := e.tracker.Global()
	if e.hooks.OnWatermark != nil {
		e.hooks.OnWatermark(partition, wm, global)
	}
	e.expireResults(global)

	// 按键与窗口起始排序，保证触发顺序确定。
	keys := make([]string, 0, len(e.windows))
	for k := range e.windows {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		starts := make([]int64, 0, len(e.windows[k]))
		for s := range e.windows[k] {
			starts = append(starts, s)
		}
		sort.Slice(starts, func(i, j int) bool { return starts[i] < starts[j] })
		for _, s := range starts {
			ws := e.windows[k][s]
			w := Window{Start: s, End: s + e.cfg.WindowSize}
			if !ws.Fired && w.End <= global {
				ws.Fired = true
				e.emit(k, w, ws)
			}
			if w.End+e.cfg.AllowedLateness <= global {
				delete(e.windows[k], s)
				if e.hooks.OnPurge != nil {
					e.hooks.OnPurge(k, w)
				}
			}
		}
		if len(e.windows[k]) == 0 {
			delete(e.windows, k)
		}
	}
}

// Watermark 返回当前全局水位线。
func (e *Engine) Watermark() int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.tracker.Global()
}

// Snapshot 在当前已提交水位线处生成一致快照。
func (e *Engine) Snapshot() Snapshot {
	e.mu.Lock()
	defer e.mu.Unlock()

	snap := Snapshot{
		Watermarks: e.tracker.Snapshot(),
		Committed:  e.tracker.Global(),
		Windows:    make(map[string]map[int64]windowState, len(e.windows)),
		Seen:       make([]string, len(e.seenOrd)),
	}
	for k, m := range e.windows {
		snap.Windows[k] = make(map[int64]windowState, len(m))
		for s, ws := range m {
			snap.Windows[k][s] = *ws
		}
	}
	copy(snap.Seen, e.seenOrd)
	return snap
}

// Restore 从快照恢复。水位线回退或与现有分区状态冲突的请求被明确拒绝。
func (e *Engine) Restore(snap Snapshot) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if snap.Committed < e.tracker.Global() {
		return &RestoreError{
			Reason: ReasonWatermarkRegression,
			Detail: fmt.Sprintf("snapshot committed watermark %d < current %d",
				snap.Committed, e.tracker.Global()),
		}
	}
	cur := e.tracker.Snapshot()
	for p, wm := range snap.Watermarks {
		if curWM, ok := cur[p]; ok && wm < curWM {
			return &RestoreError{
				Reason: ReasonPartitionConflict,
				Detail: fmt.Sprintf("partition %d snapshot watermark %d < current %d", p, wm, curWM),
			}
		}
	}

	e.tracker = NewWatermarkTracker()
	e.tracker.load(snap.Watermarks)
	e.windows = make(map[string]map[int64]*windowState, len(snap.Windows))
	for k, m := range snap.Windows {
		e.windows[k] = make(map[int64]*windowState, len(m))
		for s, ws := range m {
			c := ws
			e.windows[k][s] = &c
		}
	}
	e.seen = make(map[string]struct{}, len(snap.Seen))
	e.seenOrd = append([]string(nil), snap.Seen...)
	for _, id := range e.seenOrd {
		e.seen[id] = struct{}{}
	}
	// 恢复后按恢复出的水位线回收历史结果（若汇支持），
	// 使新进程的输出容量与状态保持一致；钩子报告回收内容与原因。
	e.expireResults(e.tracker.Global())
	return nil
}

// expireResults 在全局水位线前进/恢复后通知汇回收过期历史结果。
// 保护下限传 AllowedLateness：仍可能被迟到事件修正的结果不会被回收。
func (e *Engine) expireResults(global int64) {
	rc, ok := e.sink.(Reclaimer)
	if !ok {
		return
	}
	report := rc.Expire(global, e.cfg.AllowedLateness)
	if e.hooks.OnReclaim != nil && (len(report.Evicted) > 0 || report.Before != report.After) {
		e.hooks.OnReclaim(report)
	}
}

// emit 向汇写入窗口结果（幂等），并触发观测钩子。调用方须持有锁。
func (e *Engine) emit(key string, w Window, ws *windowState) {
	r := WindowResult{Key: key, Window: w, Sum: ws.Sum, Count: ws.Count}
	_ = e.sink.Write(r)
	if e.hooks.OnEmit != nil {
		e.hooks.OnEmit(r)
	}
}

// windowState 获取或创建窗口状态。调用方须持有锁。
func (e *Engine) windowState(key string, start int64) *windowState {
	m, ok := e.windows[key]
	if !ok {
		m = make(map[int64]*windowState)
		e.windows[key] = m
	}
	ws, ok := m[start]
	if !ok {
		ws = &windowState{}
		m[start] = ws
	}
	return ws
}

// markSeen 记录事件 ID；超出容量时按 FIFO 驱逐最旧条目，保证有界内存。
// 注意：去重表容量须大于可能的重投递窗口，否则被驱逐后的重投递无法识别。
func (e *Engine) markSeen(id string) {
	if _, ok := e.seen[id]; ok {
		return
	}
	e.seen[id] = struct{}{}
	e.seenOrd = append(e.seenOrd, id)
	if e.cfg.MaxSeenIDs > 0 {
		for len(e.seenOrd) > e.cfg.MaxSeenIDs {
			delete(e.seen, e.seenOrd[0])
			e.seenOrd = e.seenOrd[1:]
		}
	}
}
