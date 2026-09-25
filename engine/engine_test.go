package engine

import (
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"testing"
)

// testHooks 返回打印事件、窗口与水位线变化的钩子。
func testHooks(t *testing.T) Hooks {
	t.Helper()
	return Hooks{
		OnEvent: func(ev Event, accepted bool, reason string) {
			if accepted {
				t.Logf("EVENT  accepted id=%s key=%s value=%d eventTime=%d partition=%d",
					ev.ID, ev.Key, ev.Value, ev.EventTime, ev.Partition)
			} else {
				t.Logf("EVENT  dropped  id=%s key=%s eventTime=%d reason=%s",
					ev.ID, ev.Key, ev.EventTime, reason)
			}
		},
		OnEmit: func(r WindowResult) {
			t.Logf("WINDOW emit    key=%s window=%s sum=%d count=%d",
				r.Key, r.Window, r.Sum, r.Count)
		},
		OnWatermark: func(partition int, wm int64, global int64) {
			t.Logf("WM     advance  partition=%d wm=%d global=%d", partition, wm, global)
		},
		OnPurge: func(key string, w Window) {
			t.Logf("WINDOW purge   key=%s window=%s (state evicted)", key, w)
		},
	}
}

// batchEvents 构造一批跨两个分区、三个窗口的确定性事件。
func batchEvents() []Event {
	var evs []Event
	id := 0
	next := func(key string, value int64, eventTime int64, partition int) Event {
		id++
		return Event{
			ID:        fmt.Sprintf("ev-%03d", id),
			Key:       key,
			Value:     value,
			EventTime: eventTime,
			Partition: partition,
		}
	}
	// 窗口 [0,1000)
	evs = append(evs, next("a", 1, 100, 0), next("a", 2, 200, 1), next("b", 5, 300, 0))
	// 窗口 [1000,2000)
	evs = append(evs, next("a", 3, 1100, 1), next("b", 7, 1500, 0), next("b", 11, 1900, 1))
	// 窗口 [2000,3000)
	evs = append(evs, next("a", 4, 2100, 0), next("c", 9, 2500, 1))
	return evs
}

// runShuffled 以给定排列顺序把事件分两个分区注入，然后推进水位线使所有窗口触发并清除。
func runShuffled(t *testing.T, order []int) *MemSink {
	t.Helper()
	evs := batchEvents()
	sink := NewMemSink()
	eng := NewEngine(Config{WindowSize: 1000, AllowedLateness: 500, MaxSeenIDs: 10000}, sink, testHooks(t))

	var p0, p1 []Step
	for _, idx := range order {
		ev := evs[idx]
		if ev.Partition == 0 {
			p0 = append(p0, EventStep(ev))
		} else {
			p1 = append(p1, EventStep(ev))
		}
	}
	// 先注入全部事件（Inject 会等待所有源完成），再推进水位线，
	// 避免水位线抢在事件之前导致事件被误判为迟到。
	Inject(eng, &Source{Partition: 0, Steps: p0}, &Source{Partition: 1, Steps: p1})

	// 水位线推进到 3499：所有窗口结束且超过允许迟到 500，状态被清除。
	Inject(eng,
		&Source{Partition: 0, Steps: []Step{WatermarkStep(3499)}},
		&Source{Partition: 1, Steps: []Step{WatermarkStep(3499)}},
	)
	return sink
}

// TestOutOfOrderDeterminism 验证同一批事件以不同到达顺序输入，最终窗口结果一致。
func TestOutOfOrderDeterminism(t *testing.T) {
	n := len(batchEvents())

	// 顺序到达
	sequential := make([]int, n)
	for i := range sequential {
		sequential[i] = i
	}
	base := runShuffled(t, sequential).Results()

	// 多种乱序（含完全逆序与随机排列）
	perms := [][]int{}
	rev := make([]int, n)
	for i := range rev {
		rev[i] = n - 1 - i
	}
	perms = append(perms, rev)
	for seed := int64(1); seed <= 5; seed++ {
		p := rand.New(rand.NewSource(seed)).Perm(n)
		perms = append(perms, p)
	}

	for i, perm := range perms {
		t.Run(fmt.Sprintf("perm-%d", i), func(t *testing.T) {
			got := runShuffled(t, perm).Results()
			if !reflect.DeepEqual(base, got) {
				keys := sortedKeys(base)
				for _, k := range append(keys, sortedKeys(got)...) {
					t.Logf("  %s base=%+v got=%+v", k, base[k], got[k])
				}
				t.Fatalf("results differ under different arrival order")
			}
		})
	}

	// 打印基准结果，便于人工核对。
	for _, k := range sortedKeys(base) {
		t.Logf("FINAL %s -> %+v", k, base[k])
	}
}

// TestLateEventRejected 验证超过允许迟到的乱序事件被明确拒绝。
func TestLateEventRejected(t *testing.T) {
	sink := NewMemSink()
	eng := NewEngine(Config{WindowSize: 1000, AllowedLateness: 500, MaxSeenIDs: 100}, sink, testHooks(t))

	// 全局水位线推进到 2000：窗口 [0,1000) 的保留期 1000+500=1500 已过。
	eng.AdvanceWatermark(0, 2000)
	eng.AdvanceWatermark(1, 2000)

	err := eng.Ingest(Event{ID: "late-1", Key: "a", Value: 99, EventTime: 100, Partition: 0})
	if err == nil {
		t.Fatal("expected late event to be rejected")
	}
	if _, ok := err.(*LateEventError); !ok {
		t.Fatalf("expected *LateEventError, got %T: %v", err, err)
	}
	t.Logf("late event rejected as expected: %v", err)

	// 迟到但在保留期内的事件应被接受并触发修正性输出。
	eng2 := NewEngine(Config{WindowSize: 1000, AllowedLateness: 500, MaxSeenIDs: 100}, NewMemSink(), testHooks(t))
	if err := eng2.Ingest(Event{ID: "e1", Key: "a", Value: 1, EventTime: 100}); err != nil {
		t.Fatal(err)
	}
	eng2.AdvanceWatermark(0, 1200) // 触发窗口 [0,1000)
	if err := eng2.Ingest(Event{ID: "e2", Key: "a", Value: 2, EventTime: 200}); err != nil {
		t.Fatalf("late-but-allowed event should be accepted: %v", err)
	}
	r, ok := eng2.sink.(*MemSink).Get("a", Window{Start: 0, End: 1000})
	if !ok || r.Sum != 3 || r.Count != 2 {
		t.Fatalf("expected corrected window sum=3 count=2, got %+v ok=%v", r, ok)
	}
	t.Logf("allowed late event corrected window: %+v", r)
}

func sortedKeys(m map[string]WindowResult) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
