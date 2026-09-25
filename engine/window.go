package engine

import "time"

// windowKey 定位一个键的一个窗口。
type windowKey struct {
	key   string
	start time.Time
}

// windowState 保存单个窗口的聚合状态。
type windowState struct {
	key   string
	start time.Time
	end   time.Time
	agg   Aggregator
	// version 随每次（含迟到修正）输出递增，用于幂等覆盖。
	version int64
}

// tumblingWindows 按固定大小划分事件时间的滚动窗口集合。
type tumblingWindows struct {
	size    time.Duration
	windows map[windowKey]*windowState
}

func newTumblingWindows(size time.Duration) *tumblingWindows {
	return &tumblingWindows{
		size:    size,
		windows: make(map[windowKey]*windowState),
	}
}

// windowOf 返回事件时间 t 所属窗口 [start, end)。
func (w *tumblingWindows) windowOf(t time.Time) (start, end time.Time) {
	// 使用 UnixNano 截断，保证不同到达顺序下窗口划分完全确定。
	ns := t.UnixNano()
	sizeNs := w.size.Nanoseconds()
	startNs := ns - (ns % sizeNs)
	start = time.Unix(0, startNs).UTC()
	end = start.Add(w.size)
	return start, end
}

// add 将事件计入对应窗口，返回该窗口状态。
func (w *tumblingWindows) add(ev Event) *windowState {
	start, end := w.windowOf(ev.EventTime)
	k := windowKey{key: ev.Key, start: start}
	st, ok := w.windows[k]
	if !ok {
		st = &windowState{
			key:   ev.Key,
			start: start,
			end:   end,
			agg:   newSumAggregator(),
		}
		w.windows[k] = st
	}
	st.agg.Add(ev.Value)
	return st
}

// emit 生成一份带版本号的窗口结果；每次修正输出 version 递增。
func (st *windowState) emit() WindowResult {
	st.version++
	sum, count := st.agg.Value()
	return WindowResult{
		Key:         st.key,
		WindowStart: st.start,
		WindowEnd:   st.end,
		Sum:         sum,
		Count:       count,
	}
}

// dueWindows 返回所有窗口结束时间 <= wm、尚未被彻底清除的窗口键
// （顺序由调用方排序以保证输出确定性）。
func (w *tumblingWindows) dueWindowStates(wm time.Time) []*windowState {
	var out []*windowState
	for _, st := range w.windows {
		if !st.end.After(wm) {
			out = append(out, st)
		}
	}
	return out
}

// purge 删除 end+allowedLateness <= wm 的窗口，保证状态有界。
func (w *tumblingWindows) purge(wm time.Time, allowedLateness time.Duration) {
	for k, st := range w.windows {
		if !st.end.Add(allowedLateness).After(wm) {
			delete(w.windows, k)
		}
	}
}
