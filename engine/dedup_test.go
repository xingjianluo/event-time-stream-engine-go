package engine

import (
	"fmt"
	"sync"
	"testing"
)

// TestDuplicateDelivery 验证同一事件重复投递（含并发重复）只影响结果一次。
func TestDuplicateDelivery(t *testing.T) {
	sink := NewMemSink()
	eng := NewEngine(Config{WindowSize: 1000, AllowedLateness: 500, MaxSeenIDs: 10000}, sink, testHooks(t))

	ev := Event{ID: "dup-1", Key: "a", Value: 42, EventTime: 100, Partition: 0}

	// 并发重复投递同一事件 100 次。
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := eng.Ingest(ev); err != nil {
				t.Errorf("duplicate delivery should be absorbed: %v", err)
			}
		}()
	}
	wg.Wait()

	eng.AdvanceWatermark(0, 1200)
	r, ok := sink.Get("a", Window{Start: 0, End: 1000})
	if !ok {
		t.Fatal("window result missing")
	}
	if r.Sum != 42 || r.Count != 1 {
		t.Fatalf("duplicate delivery affected result: got sum=%d count=%d, want sum=42 count=1", r.Sum, r.Count)
	}
	t.Logf("100 duplicate deliveries collapsed to one: %+v", r)
}

// TestSinkIdempotency 验证输出汇的幂等写入：重复写同一结果对外只生效一次。
func TestSinkIdempotency(t *testing.T) {
	sink := NewMemSink()
	r := WindowResult{Key: "a", Window: Window{Start: 0, End: 1000}, Sum: 7, Count: 2}

	for i := 0; i < 10; i++ {
		if err := sink.Write(r); err != nil {
			t.Fatal(err)
		}
	}
	if sink.Writes() != 10 {
		t.Fatalf("writes = %d, want 10", sink.Writes())
	}
	if len(sink.Results()) != 1 {
		t.Fatalf("idempotent sink should expose 1 result, got %d", len(sink.Results()))
	}
	got, _ := sink.Get("a", Window{Start: 0, End: 1000})
	if got != r {
		t.Fatalf("got %+v, want %+v", got, r)
	}
	t.Logf("10 idempotent writes -> 1 visible result: %+v", got)
}

// TestConcurrentSources 验证多来源并发注入下同键顺序语义与结果确定性。
func TestConcurrentSources(t *testing.T) {
	sink := NewMemSink()
	eng := NewEngine(Config{WindowSize: 10000, AllowedLateness: 0, MaxSeenIDs: 100000}, sink, testHooks(t))

	const sources = 8
	const perSource = 500

	// 每个源一个 goroutine，全部写入同一个键、同一个窗口。
	srcs := make([]*Source, 0, sources)
	for s := 0; s < sources; s++ {
		steps := make([]Step, 0, perSource)
		for i := 0; i < perSource; i++ {
			steps = append(steps, EventStep(Event{
				ID:        fmt.Sprintf("s%d-%d", s, i),
				Key:       "hot-key",
				Value:     1,
				EventTime: int64(i), // 同一窗口内
				Seq:       uint64(i),
			}))
		}
		srcs = append(srcs, &Source{Partition: s, Steps: steps})
	}
	Inject(eng, srcs...)

	for s := 0; s < sources; s++ {
		eng.AdvanceWatermark(s, 20000)
	}

	r, ok := sink.Get("hot-key", Window{Start: 0, End: 10000})
	if !ok {
		t.Fatal("window result missing")
	}
	want := int64(sources * perSource)
	if r.Sum != want || r.Count != want {
		t.Fatalf("got sum=%d count=%d, want %d", r.Sum, r.Count, want)
	}
	t.Logf("concurrent injection: %d sources x %d events -> sum=%d count=%d",
		sources, perSource, r.Sum, r.Count)
}

// TestBoundedMemory 验证长时间运行下窗口状态与去重表均有界。
func TestBoundedMemory(t *testing.T) {
	sink := NewMemSink()
	const maxSeen = 1000
	eng := NewEngine(Config{WindowSize: 1000, AllowedLateness: 500, MaxSeenIDs: maxSeen}, sink, Hooks{})

	// 模拟长时间运行：10000 个事件横跨 10000 个窗口。
	for i := 0; i < 10000; i++ {
		ev := Event{
			ID:        fmt.Sprintf("long-%d", i),
			Key:       fmt.Sprintf("k%d", i%10),
			Value:     1,
			EventTime: int64(i) * 1000,
		}
		if err := eng.Ingest(ev); err != nil {
			t.Fatal(err)
		}
		// 水位线跟随事件时间推进。
		eng.AdvanceWatermark(0, int64(i)*1000)
	}

	snap := eng.Snapshot()
	if len(snap.Seen) > maxSeen {
		t.Fatalf("seen set unbounded: %d > %d", len(snap.Seen), maxSeen)
	}
	windowCount := 0
	for _, m := range snap.Windows {
		windowCount += len(m)
	}
	// 水位线推进后，超出迟到保留期的窗口状态应被清除，仅剩最近窗口。
	if windowCount > 20 {
		t.Fatalf("window state unbounded: %d live windows", windowCount)
	}
	t.Logf("after 10000 events: seen=%d (cap %d), live windows=%d", len(snap.Seen), maxSeen, windowCount)
}
