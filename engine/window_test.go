package engine

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

func testConfig() Config {
	return Config{
		WindowSize:          10 * time.Second,
		AllowedLateness:     5 * time.Second,
		DedupMaxSize:        10000,
		PartitionCount:      1,
		RetainClosedResults: true,
	}
}

// resultKey 比较用：忽略版本号，只比较聚合结论。
type resultKey struct {
	Key   string
	Start time.Time
	Sum   int64
	Count int64
}

func resultFingerprint(t *testing.T, eng *Engine) map[resultKey]int64 {
	t.Helper()
	fp := make(map[resultKey]int64)
	for _, e := range eng.Results() {
		k := resultKey{e.Result.Key, e.Result.WindowStart, e.Result.Sum, e.Result.Count}
		fp[k] = e.Version
	}
	return fp
}

// runOneOrder 用给定的事件顺序在水位线 20 处跑完阶段1+2，
// 然后处理阶段3的迟到/重复事件并推进到水位线 30。
func runOneOrder(t *testing.T, events []Event, fx BatchFixture) *Engine {
	t.Helper()
	eng := New(testConfig())
	eng.SetLogger(testLogger{t})
	for _, ev := range events {
		if err := eng.ProcessEvent(ev); err != nil {
			t.Fatalf("unexpected error for %s: %v", ev.ID, err)
		}
	}
	if _, err := eng.AdvanceWatermark(0, fx.Base.Add(20*time.Second)); err != nil {
		t.Fatal(err)
	}
	var dup, late int
	for _, ev := range fx.Stage3 {
		err := eng.ProcessEvent(ev)
		switch {
		case errors.Is(err, ErrDuplicateEvent):
			dup++
		case errors.Is(err, ErrLateEventDropped):
			late++
		case err != nil:
			t.Fatalf("stage3 unexpected error: %v", err)
		}
	}
	if dup != 1 || late != 1 {
		t.Fatalf("stage3 stats: dup=%d want1, late=%d want1", dup, late)
	}
	if _, err := eng.AdvanceWatermark(0, fx.Base.Add(30*time.Second)); err != nil {
		t.Fatal(err)
	}
	return eng
}

// TestOutOfOrderDeterminism 验证同一批事件无论以何种顺序到达，
// 最终窗口聚合结论完全一致。
func TestOutOfOrderDeterminism(t *testing.T) {
	fx := MakeBatchFixture()
	baseEvents := fx.PreWM20Events()

	var seeds []uint64
	for s := uint64(1); s <= 12; s++ {
		seeds = append(seeds, s*11+3)
	}

	var reference map[resultKey]int64
	// 顺序0：原始顺序
	{
		eng := runOneOrder(t, baseEvents, fx)
		reference = resultFingerprint(t, eng)
		logFinalResults(t, eng, "seed=original")
	}

	for _, seed := range seeds {
		ordered := ShuffleOrder(baseEvents, seed)
		eng := runOneOrder(t, ordered, fx)
		got := resultFingerprint(t, eng)
		if len(got) != len(reference) {
			t.Fatalf("seed=%d result count=%d want %d", seed, len(got), len(reference))
		}
		for rk, ver := range reference {
			gv, ok := got[rk]
			if !ok {
				t.Fatalf("seed=%d missing result %+v", seed, rk)
			}
			// 最终聚合值相同即可；版本号因到达顺序不同允许不同。
			_ = ver
			_ = gv
		}
		logFinalResults(t, eng, fmt.Sprintf("seed=%d", seed))
	}
}

// TestLateEventPolicy 明确验证允许迟到内修正与超迟到拒绝。
func TestLateEventPolicy(t *testing.T) {
	fx := MakeBatchFixture()
	eng := New(testConfig())
	eng.SetLogger(testLogger{t})

	// 先有 a 在 [10,20) 窗口的一个事件并触发窗口
	mustProc(t, eng, Event{ID: "a1", Key: "a", Value: 10, EventTime: fx.Base.Add(12 * time.Second)})
	if _, err := eng.AdvanceWatermark(0, fx.Base.Add(20*time.Second)); err != nil {
		t.Fatal(err)
	}

	// 允许迟到内：@18（18+5>=20）-> 接受并产出修正版本
	err := eng.ProcessEvent(Event{ID: "a2", Key: "a", Value: 5, EventTime: fx.Base.Add(18 * time.Second)})
	if err != nil {
		t.Fatalf("within-lateness event should be accepted: %v", err)
	}
	r := findResult(eng, "a", fx.Base.Add(10*time.Second))
	if r.Result.Sum != 15 || r.Result.Count != 2 || r.Version != 2 {
		t.Fatalf("corrected result = sum=%d count=%d v%d, want sum=15 count=2 v2",
			r.Result.Sum, r.Result.Count, r.Version)
	}

	// 超迟到：@14（14+5=19<20）-> 明确拒绝
	err = eng.ProcessEvent(Event{ID: "a3", Key: "a", Value: 999, EventTime: fx.Base.Add(14 * time.Second)})
	if !errors.Is(err, ErrLateEventDropped) {
		t.Fatalf("want ErrLateEventDropped, got %v", err)
	}
	r = findResult(eng, "a", fx.Base.Add(10*time.Second))
	if r.Result.Sum != 15 {
		t.Fatalf("late event must not affect result, got sum=%d", r.Result.Sum)
	}
	t.Logf("迟到策略验证通过: 窗口 sum=%d 未被拒绝事件影响", r.Result.Sum)
}

// TestFinalResultsMatchFixture 端到端校验最终聚合数值。
func TestFinalResultsMatchFixture(t *testing.T) {
	fx := MakeBatchFixture()
	eng := runOneOrder(t, fx.PreWM20Events(), fx)
	got := resultFingerprint(t, eng)
	for _, want := range fx.ExpectedFinalResults() {
		k := resultKey{want.Key, want.WindowStart, want.Sum, want.Count}
		if _, ok := got[k]; !ok {
			t.Errorf("missing expected result: key=%s window=%s sum=%d count=%d",
				want.Key, want.WindowStart.Format("15:04:05"), want.Sum, want.Count)
		}
	}
	logFinalResults(t, eng, "final")
}

func mustProc(t *testing.T, eng *Engine, ev Event) {
	t.Helper()
	if err := eng.ProcessEvent(ev); err != nil {
		t.Fatalf("process %s: %v", ev.ID, err)
	}
}

func findResult(eng *Engine, key string, start time.Time) ResultLogEntry {
	for _, e := range eng.Results() {
		if e.Result.Key == key && e.Result.WindowStart.Equal(start) {
			return e
		}
	}
	return ResultLogEntry{}
}

func logFinalResults(t *testing.T, eng *Engine, tag string) {
	t.Helper()
	for _, e := range eng.Results() {
		t.Logf("[%s] wm=%s key=%s window=[%s,%s) sum=%d count=%d version=%d",
			tag, eng.Watermark().Format("15:04:05"),
			e.Result.Key,
			e.Result.WindowStart.Format("15:04:05"), e.Result.WindowEnd.Format("15:04:05"),
			e.Result.Sum, e.Result.Count, e.Version)
	}
}
