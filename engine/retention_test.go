package engine

import (
	"fmt"
	"testing"
)

// TestResultRetention 验证结果保留策略：
// 超出保留期的历史结果被回收（可解释），保留期内的结果不受影响。
func TestResultRetention(t *testing.T) {
	sink := NewMemSink()
	var expired []WindowResult
	hooks := testHooks(t)
	hooks.OnResultExpire = func(r WindowResult) {
		expired = append(expired, r)
		t.Logf("RESULT expire  key=%s window=%s sum=%d (retention elapsed)", r.Key, r.Window, r.Sum)
	}
	// 窗口 1000ms，允许迟到 500ms，结果额外保留 2000ms：
	// 结果在 全局水位线 >= Window.End + 500 + 2000 时被回收。
	eng := NewEngine(Config{
		WindowSize:      1000,
		AllowedLateness: 500,
		MaxSeenIDs:      10000,
		ResultRetention: 2000,
	}, sink, hooks)

	// 5 个窗口各一条事件，水位线跟随推进到 4499。
	for i := 0; i < 5; i++ {
		ev := Event{
			ID:        fmt.Sprintf("r-%d", i),
			Key:       "a",
			Value:     int64(i + 1),
			EventTime: int64(i) * 1000,
			Seq:       uint64(i + 1),
		}
		if err := eng.Ingest(ev); err != nil {
			t.Fatal(err)
		}
		eng.AdvanceWatermark(0, int64(i)*1000+999)
	}

	// 全局水位线 4999：窗口 [0,1000) 与 [1000,2000) 的结果已过保留期被回收；
	// [2000,3000) 起的结果仍在保留期内，必须完整保留（[4000,5000) 尚未触发）。
	if len(sink.Results()) != 2 {
		t.Fatalf("expected 2 retained results, got %d: %v", len(sink.Results()), sink.Results())
	}
	if _, ok := sink.Get("a", Window{Start: 2000, End: 3000}); !ok {
		t.Fatal("result within retention period was wrongly reclaimed")
	}
	if len(expired) != 2 {
		t.Fatalf("expected 2 expired results, got %d", len(expired))
	}
	for _, r := range expired {
		if r.Window.End+500+2000 > 4999 {
			t.Fatalf("result %s reclaimed before its retention elapsed", r.Window)
		}
		t.Logf("RECLAIMED key=%s window=%s sum=%d count=%d (explainable: past retention)",
			r.Key, r.Window, r.Sum, r.Count)
	}
	t.Logf("retained=%d expired=%d (bounded output history)", len(sink.Results()), len(expired))
}

// TestRetentionKeepsCorrectableResults 验证保留期内的结果仍可被迟到事件修正，
// 回收不会发生在“还可能被修正”的结果上。
func TestRetentionKeepsCorrectableResults(t *testing.T) {
	sink := NewMemSink()
	eng := NewEngine(Config{
		WindowSize:      1000,
		AllowedLateness: 500,
		MaxSeenIDs:      100,
		ResultRetention: 1000,
	}, sink, testHooks(t))

	if err := eng.Ingest(Event{ID: "k1", Key: "a", Value: 1, EventTime: 100, Seq: 1}); err != nil {
		t.Fatal(err)
	}
	eng.AdvanceWatermark(0, 1200) // 触发窗口 [0,1000)，结果进入保留期

	// 迟到但在允许迟到窗口内的事件：修正性重触发，结果必须还在。
	if err := eng.Ingest(Event{ID: "k2", Key: "a", Value: 2, EventTime: 200, Seq: 2}); err != nil {
		t.Fatal(err)
	}
	r, ok := sink.Get("a", Window{Start: 0, End: 1000})
	if !ok || r.Sum != 3 {
		t.Fatalf("correctable result missing or wrong: %+v ok=%v", r, ok)
	}
	t.Logf("corrected result still visible within retention: %+v", r)

	// 水位线 2499：保留期（1000+500+1000=2500）未到达，结果仍可查；
	// 推进到 2500 到达回收边界（等号即回收），结果应被回收。
	eng.AdvanceWatermark(0, 2499)
	if _, ok := sink.Get("a", Window{Start: 0, End: 1000}); !ok {
		t.Fatal("result reclaimed before retention elapsed")
	}
	eng.AdvanceWatermark(0, 2500)
	if _, ok := sink.Get("a", Window{Start: 0, End: 1000}); ok {
		t.Fatal("result should have been reclaimed after retention elapsed")
	}
	t.Logf("result reclaimed exactly after retention boundary (end+lateness+retention <= watermark)")
}

// TestRetentionDisabledByDefault 验证不配置保留策略时行为与上一版一致：
// 历史结果全部保留，不发生回收。
func TestRetentionDisabledByDefault(t *testing.T) {
	sink := NewMemSink()
	eng := NewEngine(Config{WindowSize: 1000, AllowedLateness: 500, MaxSeenIDs: 1000}, sink, Hooks{})

	for i := 0; i < 10; i++ {
		if err := eng.Ingest(Event{
			ID: fmt.Sprintf("d-%d", i), Key: "a", Value: 1,
			EventTime: int64(i) * 1000, Seq: uint64(i + 1),
		}); err != nil {
			t.Fatal(err)
		}
		eng.AdvanceWatermark(0, int64(i)*1000+999)
	}
	// 最后一个窗口 [9000,10000) 尚未触发（水位线 9999 < 10000），故为 9 条结果。
	if got := len(sink.Results()); got != 9 {
		t.Fatalf("retention disabled: expected 9 results kept, got %d", got)
	}
	t.Logf("retention disabled: all 9 emitted historical results kept (previous behavior)")
}
