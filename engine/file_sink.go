package engine

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync"
)

// fileSinkWire 是落盘结果文件的 JSON 形状。
type fileSinkWire struct {
	Format  string                  `json:"format"`
	Results map[string]WindowResult `json:"results"`
}

// FileSink 是落盘的幂等结果汇：进程彻底退出后重开，历史结果仍在；
// 回收结果时同步删除落盘内容。所有写操作互斥并整体重写（原子改名），
// 适合本地演示与单机落地场景。
type FileSink struct {
	mu      sync.Mutex
	path    string
	policy  RetentionPolicy
	results map[string]WindowResult
}

const fileSinkMagic = "event-time-stream-engine-go/results"

// OpenFileSink 打开（或首次创建）落盘汇，已有结果被读回。
func OpenFileSink(path string, policy RetentionPolicy) (*FileSink, error) {
	s := &FileSink{path: path, policy: policy, results: make(map[string]WindowResult)}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return s, nil // 首次运行：空汇
		}
		return nil, fmt.Errorf("open result file: %w", err)
	}
	var w fileSinkWire
	if err := json.Unmarshal(data, &w); err != nil {
		return nil, fmt.Errorf("decode result file %s: %w", path, err)
	}
	if w.Format != fileSinkMagic {
		return nil, fmt.Errorf("result file %s has unexpected format marker %q", path, w.Format)
	}
	if w.Results != nil {
		s.results = w.Results
	}
	return s, nil
}

// Write 幂等写入一条结果并持久化。
func (s *FileSink) Write(r WindowResult) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.results[idemKey(r.Key, r.Window)] = r
	return s.persistLocked()
}

// Get 返回某 (键, 窗口) 的当前结果。
func (s *FileSink) Get(key string, w Window) (WindowResult, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.results[idemKey(key, w)]
	return r, ok
}

// Results 返回全部结果的副本。
func (s *FileSink) Results() map[string]WindowResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]WindowResult, len(s.results))
	for k, v := range s.results {
		out[k] = v
	}
	return out
}

// Len 返回当前保留的结果数量。
func (s *FileSink) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.results)
}

// Expire 按保留期回收过期结果并持久化回收动作。
// 实际保留时长取策略值与 protectedMillis（允许迟到时间）的较大者，
// 保证保留期内结果绝不被回收。
func (s *FileSink) Expire(globalWatermark, protectedMillis int64) ReclaimReport {
	s.mu.Lock()
	defer s.mu.Unlock()

	effective := s.policy.RetainMillis
	report := ReclaimReport{
		Watermark:    globalWatermark,
		RetainMillis: effective,
		Before:       len(s.results),
	}
	if effective <= 0 {
		report.After = len(s.results)
		return report
	}
	if effective < protectedMillis {
		effective = protectedMillis
		report.RetainMillis = effective
	}
	keys := make([]string, 0, len(s.results))
	for k := range s.results {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		r := s.results[k]
		if r.Window.End+effective <= globalWatermark {
			report.Evicted = append(report.Evicted, ReclaimEntry{
				Key: r.Key, Window: r.Window, Reason: ReasonExpired,
			})
			delete(s.results, k)
		}
	}
	report.After = len(s.results)
	if len(report.Evicted) > 0 {
		if err := s.persistLocked(); err != nil {
			// 持久化失败不改变内存回收结论，但必须让调用方知道磁盘可能仍旧。
			report.Evicted = append(report.Evicted, ReclaimEntry{
				Key: "<persist-error>", Reason: "persist-failed: " + err.Error(),
			})
		}
	}
	return report
}

// persistLocked 将当前结果原子重写到磁盘。调用方须持锁。
func (s *FileSink) persistLocked() error {
	w := fileSinkWire{Format: fileSinkMagic, Results: s.results}
	data, err := json.MarshalIndent(w, "", "  ")
	if err != nil {
		return fmt.Errorf("encode results: %w", err)
	}
	return writeFileAtomic(s.path, data)
}

// writeFileAtomic 以临时文件 + fsync + 改名的方式原子写文件。
func writeFileAtomic(path string, data []byte) error {
	dir := dirOf(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	ok := false
	defer func() {
		if !ok {
			_ = f.Close()
			_ = os.Remove(tmp)
		}
	}()
	if _, err := f.Write(data); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	ok = true
	if df, err := os.Open(dir); err == nil {
		_ = df.Sync()
		_ = df.Close()
	}
	return nil
}

func dirOf(path string) string {
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' || path[i] == os.PathSeparator {
			return path[:i]
		}
	}
	return "."
}
