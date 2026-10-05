package engine

import (
	"testing"
)

func reclaimHooks(t *testing.T) Hooks {
	h := testHooks(t)
	h.OnReclaim = func(rep ReclaimReport) {
		t.Logf("RECLAIM watermark=%d retain=%d before=%d after=%d evicted=%d",
			rep.Watermark, rep.RetainMillis, rep.Before, rep.After, len(rep.Evicted))
		for _, e := range rep.Evicted {
			t.Logf("  evicted key=%s window=%s reason=%s", e.Key, e.Window, e.Reason)
		}
	}
	return h
}

func emitResult(t *testing.T, s Sink, key string, start, end, sum int64) {
	t.Helper()
	if err := s.Write(WindowResult{Key: key, Window: Window{Start: start, End: end}, Sum: sum, Count: 1}); err != nil {
		t.Fatal(err)
	}
}

// TestRetentionEvictsOldResults 验证水位线越过保留期后旧结果被回收，且报告可解释。
func TestRetentionEvictsOldResults(t *testing.T) {
	sink := NewRetainingMemSink(RetentionPolicy{RetainMillis: 1000})
	emitResult(t, sink, "a", 0, 1000, 1)
	emitResult(t, sink, "a", 1000, 2000, 2)
	emitResult(t, sink, "a", 2000, 3000, 3)

	// 水位线 2500：窗口 [0,1000) 的过期点 1000+1000=2000 <= 2500 → 回收；
	// [1000,2000) 过期点 3000 > 2500 → 保留。
	rep := sink.Expire(2500, 0)
	t.Logf("SCENARIO retention eviction at wm=2500 retain=1000")
	if len(rep.Evicted) != 1 || rep.Evicted[0].Window.Start != 0 {
		t.Fatalf("expected exactly window0 evicted, got %+v", rep.Evicted)
	}
	if rep.Before != 3 || rep.After != 2 {
		t.Fatalf("expected 3->2, got %d->%d", rep.Before, rep.After)
	}
	if _, ok := sink.Get("a", Window{Start: 0, End: 1000}); ok {
		t.Fatal("expired result must be removed")
	}
	if _, ok := sink.Get("a", Window{Start: 1000, End: 2000}); !ok {
		t.Fatal("result still within retention must survive")
	}
	if rep.Evicted[0].Reason != ReasonExpired {
		t.Fatalf("eviction reason=%q want %q", rep.Evicted[0].Reason, ReasonExpired)
	}
	t.Logf("PASS old window evicted with explanation: %+v", rep.Evicted[0])

	// 推进到 4000：剩余两个窗口全部过期。
	rep = sink.Expire(4000, 0)
	if rep.After != 0 || len(rep.Evicted) != 2 {
		t.Fatalf("expected all evicted, got after=%d evicted=%d", rep.After, len(rep.Evicted))
	}
	t.Logf("PASS advancing watermark reclaims the rest: after=%d", rep.After)
}

// TestRetentionProtectsCorrectableResults 验证实际保留期不低于允许迟到时间：
// 即使策略值很小，仍在迟到保留期内（可能被修正）的结果绝不回收。
func TestRetentionProtectsCorrectableResults(t *testing.T) {
	sink := NewRetainingMemSink(RetentionPolicy{RetainMillis: 100})
	emitResult(t, sink, "a", 0, 1000, 1)

	// 引擎给的保护下限是 AllowedLateness=500。wm=1200：
	// 过期点按策略 1000 <= 1200 本应回收，但按保护下限 1000+500=1500 > 1200 → 保留。
	rep := sink.Expire(1200, 500)
	if rep.RetainMillis != 500 {
		t.Fatalf("effective retain should be raised to protected 500, got %d", rep.RetainMillis)
	}
	if rep.After != 1 || len(rep.Evicted) != 0 {
		t.Fatalf("correctable result must be protected, after=%d evicted=%d", rep.After, len(rep.Evicted))
	}
	t.Logf("SCENARIO/PASS correctable result protected: effective retain=%d wm=%d -> kept",
		rep.RetainMillis, rep.Watermark)

	// 水位线越过保护下限后才回收。
	rep = sink.Expire(1500, 500)
	if rep.After != 0 {
		t.Fatalf("expected eviction at wm=1500, got after=%d", rep.After)
	}
	t.Log("PASS same result evicted only after protection boundary passed")
}

// TestRetentionDisabledByDefault 验证普通 MemSink 与 RetainMillis<=0 时结果无限保留。
func TestRetentionDisabledByDefault(t *testing.T) {
	plain := NewMemSink()
	emitResult(t, plain, "a", 0, 1000, 1)
	var plainSink Sink = plain
	if _, ok := plainSink.(Reclaimer); ok {
		t.Fatal("plain MemSink should not reclaim")
	}

	sink := NewRetainingMemSink(RetentionPolicy{RetainMillis: 0})
	emitResult(t, sink, "a", 0, 1000, 1)
	rep := sink.Expire(1_000_000_000, 500)
	if rep.After != 1 || len(rep.Evicted) != 0 {
		t.Fatalf("retention disabled should keep everything, after=%d", rep.After)
	}
	t.Log("SCENARIO/PASS retention disabled: results retained indefinitely")
}

// TestRetentionWiredIntoEngine 验证水位线推进/恢复时引擎自动驱动汇回收并打钩子。
func TestRetentionWiredIntoEngine(t *testing.T) {
	var reports []ReclaimReport
	hooks := reclaimHooks(t)
	origin := hooks.OnReclaim
	hooks.OnReclaim = func(rep ReclaimReport) {
		reports = append(reports, rep)
		origin(rep)
	}
	cfg := Config{WindowSize: 1000, AllowedLateness: 500, MaxSeenIDs: 1000}
	sink := NewRetainingMemSink(RetentionPolicy{RetainMillis: 1000})
	eng := NewEngine(cfg, sink, hooks)

	if err := eng.Ingest(Event{ID: "e1", Key: "a", Value: 1, EventTime: 100}); err != nil {
		t.Fatal(err)
	}
	eng.AdvanceWatermark(0, 1200) // 触发窗口0输出
	if _, ok := sink.Get("a", Window{Start: 0, End: 1000}); !ok {
		t.Fatal("window0 should have emitted")
	}
	// 推进到 2000：窗口0过期点 1000+1000=2000，且不低于保护下限 1500 → 回收。
	eng.AdvanceWatermark(0, 2000)
	if _, ok := sink.Get("a", Window{Start: 0, End: 1000}); ok {
		t.Fatal("window0 result should have been reclaimed on watermark advance")
	}
	if len(reports) == 0 {
		t.Fatal("OnReclaim hook should fire")
	}
	t.Log("SCENARIO/PASS engine drives sink reclamation on watermark advance")

	// 恢复后也应按恢复水位线回收：重建一个装着旧结果的汇并恢复到 wm=2000。
	sink2 := NewRetainingMemSink(RetentionPolicy{RetainMillis: 1000})
	emitResult(t, sink2, "old", 0, 1000, 1)
	eng2 := NewEngine(cfg, sink2, reclaimHooks(t))
	cp := eng.Snapshot()
	if err := eng2.Restore(cp); err != nil {
		t.Fatal(err)
	}
	if _, ok := sink2.Get("old", Window{Start: 0, End: 1000}); ok {
		t.Fatal("stale result should be reclaimed after restore")
	}
	t.Log("PASS restore also applies retention to pre-existing sink contents")
}

// TestRetentionBoundedUnderLongRun 验证长跑下输出结果数量被保留窗口数限制。
func TestRetentionBoundedUnderLongRun(t *testing.T) {
	cfg := Config{WindowSize: 1000, AllowedLateness: 0, MaxSeenIDs: 100000}
	sink := NewRetainingMemSink(RetentionPolicy{RetainMillis: 2000})
	eng := NewEngine(cfg, sink, Hooks{})

	for i := 0; i < 200; i++ {
		ev := Event{ID: "e", Key: "a", Value: 1, EventTime: int64(i) * 1000}
		ev.ID = ev.ID + itoa(i)
		if err := eng.Ingest(ev); err != nil {
			t.Fatal(err)
		}
		eng.AdvanceWatermark(0, int64(i)*1000)
	}
	if n := sink.Len(); n > 4 {
		t.Fatalf("output history unbounded: %d retained results", n)
	}
	t.Logf("SCENARIO/PASS long run bounded: %d results retained after 200 windows", sink.Len())
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(b[pos:])
}
