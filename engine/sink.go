package engine

import "sync"

// Sink 接收窗口结果，写入必须幂等：
// 同一 (Key, Window) 的结果无论写多少次，对外可见效果等同于写一次。
type Sink interface {
	Write(result WindowResult) error
}

// MemSink 是内存幂等汇：以 (Key, Window) 为幂等键覆盖保存最新结果，
// 并记录写入次数用于测试验证幂等性。
type MemSink struct {
	mu      sync.Mutex
	results map[string]WindowResult // 幂等键 -> 最新结果
	writes  int
}

// NewMemSink 创建内存幂等汇。
func NewMemSink() *MemSink {
	return &MemSink{results: make(map[string]WindowResult)}
}

func idemKey(key string, w Window) string {
	return key + "@" + w.String()
}

// Write 幂等写入：同键覆盖，重复写不产生额外效果。
func (m *MemSink) Write(r WindowResult) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.results[idemKey(r.Key, r.Window)] = r
	m.writes++
	return nil
}

// Get 返回某 (键, 窗口) 的最终结果。
func (m *MemSink) Get(key string, w Window) (WindowResult, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.results[idemKey(key, w)]
	return r, ok
}

// Writes 返回累计写入次数（含被幂等吸收的重复写）。
func (m *MemSink) Writes() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.writes
}

// Results 返回全部最终结果的副本。
func (m *MemSink) Results() map[string]WindowResult {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]WindowResult, len(m.results))
	for k, v := range m.results {
		out[k] = v
	}
	return out
}
