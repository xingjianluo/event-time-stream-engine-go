package engine

import "sort"

const (
	// ReasonExpired 表示结果窗口结束时间已超出保留期。
	ReasonExpired = "expired"
)

// RetentionPolicy 是输出结果的保留策略。
type RetentionPolicy struct {
	// RetainMillis：结果自窗口结束时间起至少保留的时长（毫秒）。
	// <=0 表示不主动回收（永久保留，输出无限增长——仅适合短任务）。
	RetainMillis int64
}

// ReclaimEntry 描述一条被回收的结果及原因。
type ReclaimEntry struct {
	Key    string
	Window Window
	Reason string
}

// ReclaimReport 是一次回收的可解释记录：在什么水位线、按什么保留期、回收了什么。
type ReclaimReport struct {
	Watermark    int64 // 回收时的全局水位线
	RetainMillis int64 // 实际生效的保留时长（已含正确性保护下限）
	Before       int
	After        int
	Evicted      []ReclaimEntry
}

// RetainingMemSink 在内存幂等汇之外增加按保留期回收能力。
type RetainingMemSink struct {
	*MemSink
	policy RetentionPolicy
}

// NewRetainingMemSink 创建带保留策略的内存汇。
func NewRetainingMemSink(policy RetentionPolicy) *RetainingMemSink {
	return &RetainingMemSink{MemSink: NewMemSink(), policy: policy}
}

// Expire 按全局水位线回收过期结果。
// protectedMillis 是引擎给定的正确性保护下限（即允许迟到时间）：
// 实际保留时长取策略值与保护下限的较大者，因此仍在迟到保留期内、
// 后面可能被修正或被重放读取的结果绝不会被回收。
func (s *RetainingMemSink) Expire(globalWatermark, protectedMillis int64) ReclaimReport {
	s.mu.Lock()
	defer s.mu.Unlock()

	effective := s.policy.RetainMillis
	report := ReclaimReport{
		Watermark:    globalWatermark,
		RetainMillis: effective,
		Before:       len(s.results),
	}
	// 未配置保留策略（<=0）：永不主动回收，保护下限也不产生回收动作。
	if effective <= 0 {
		report.After = len(s.results)
		return report
	}
	// 策略启用时，实际保留时长不得短于正确性保护下限（允许迟到时间）。
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
		// 结果在 w.End+保留期 <= 水位线 时才过期；保留期内结果不受影响。
		if r.Window.End+effective <= globalWatermark {
			report.Evicted = append(report.Evicted, ReclaimEntry{
				Key: r.Key, Window: r.Window, Reason: ReasonExpired,
			})
			delete(s.results, k)
		}
	}
	report.After = len(s.results)
	return report
}
