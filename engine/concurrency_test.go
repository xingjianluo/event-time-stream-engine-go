package engine

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestConcurrentPartitionInjection 多来源并发注入，最终结果与顺序无关。
func TestConcurrentPartitionInjection(t *testing.T) {
	fx := MakePartitionedFixture()
	cfg := testConfig()
	cfg.PartitionCount = 2

	// 多个 goroutine 以不同乱序种子反复并发跑同一批数据，
	// 每次用全新引擎，验证最终聚合指纹始终一致。
	var reference map[resultKey]int64
	for round := 0; round < 8; round++ {
		eng := New(cfg)
		eng.SetLogger(testLogger{t})
		parts := make([][]Event, 2)
		for p := 0; p < 2; p++ {
			parts[p] = ShuffleOrder(fx.Partitions[p], uint64(round*7+p+1))
		}
		// 并发乱序可能破坏分区内偏移顺序，因此这里按"来源并发"模型：
		// 每个分区一个 goroutine；分区内再用极小调度扰动模拟实时到达。
		var wg sync.WaitGroup
		for p := 0; p < 2; p++ {
			wg.Add(1)
			go func(p int, evs []Event) {
				defer wg.Done()
				for _, ev := range evs {
					if err := eng.ProcessEvent(ev); err != nil {
						t.Errorf("round=%d p=%d event=%s: %v", round, p, ev.ID, err)
						return
					}
					if (round+p)%3 == 0 {
						time.Sleep(time.Microsecond)
					}
				}
			}(p, parts[p])
		}
		wg.Wait()

		// 两分区都上报水位线（顺序也做扰动）。
		p0, p1 := 0, 1
		if round%2 == 1 {
			p0, p1 = 1, 0
		}
		for _, p := range []int{p0, p1} {
			for _, wm := range fx.Watermarks[p] {
				if _, err := eng.AdvanceWatermark(p, wm); err != nil {
					t.Fatalf("advance p=%d: %v", p, err)
				}
			}
		}

		got := resultFingerprint(t, eng)
		if reference == nil {
			reference = got
			logFinalResults(t, eng, fmt.Sprintf("concurrent round=%d (reference)", round))
		} else if len(got) != len(reference) {
			t.Fatalf("round=%d result count mismatch", round)
		} else {
			for rk := range reference {
				if _, ok := got[rk]; !ok {
					t.Fatalf("round=%d missing %+v", round, rk)
				}
			}
			t.Logf("round=%d 并发结果与参考一致（%d 个窗口）", round, len(got))
		}
	}
}

// TestSourceRunWithRestart 用 PartitionedSource 跑到中途水位线快照，
// 从偏移恢复后继续：最终结果与一次性跑完一致，且无重复无丢失。
func TestSourceRunWithRestart(t *testing.T) {
	fx := MakePartitionedFixture()
	cfg := testConfig()
	cfg.PartitionCount = 2

	// 参考：一次性跑完
	refEng := New(cfg)
	src0 := NewPartitionedSource(fx.Partitions, 0)
	if err := src0.Run(context.Background(), refEng,
		map[int]int64{0: 0, 1: 0}, fx.Watermarks); err != nil {
		t.Fatal(err)
	}
	ref := resultFingerprint(t, refEng)

	// 分段：先只跑到 wm=10（每分区仅上报第一个水位线）
	eng := New(cfg)
	eng.SetLogger(testLogger{t})
	wms10 := map[int][]time.Time{
		0: {fx.Watermarks[0][0]},
		1: {fx.Watermarks[1][0]},
	}
	src1 := NewPartitionedSource(fx.Partitions, 0)
	if err := src1.Run(context.Background(), eng,
		map[int]int64{0: 0, 1: 0}, wms10); err != nil {
		t.Fatal(err)
	}
	cp := eng.Snapshot()
	t.Logf("中途快照: wm=%s offsets=%v",
		cp.CommittedWatermark.Format("15:04:05"), cp.Offsets)

	// 重启：恢复快照，从已提交偏移继续，上报剩余水位线
	eng2 := New(cfg)
	eng2.SetLogger(testLogger{t})
	if err := eng2.Restore(cp); err != nil {
		t.Fatal(err)
	}
	offsets := map[int]int64{}
	for _, o := range cp.Offsets {
		offsets[o.Partition] = o.Offset
	}
	wmsRest := map[int][]time.Time{
		0: {fx.Watermarks[0][1]},
		1: {fx.Watermarks[1][1]},
	}
	src2 := NewPartitionedSource(fx.Partitions, 0)
	if err := src2.Run(context.Background(), eng2, offsets, wmsRest); err != nil {
		t.Fatal(err)
	}
	if src2.Stats().Delivered != 0 {
		t.Fatalf("resumed run accepted %d events, want 0 (all offsets already consumed)",
			src2.Stats().Delivered)
	}
	got := resultFingerprint(t, eng2)
	if len(got) != len(ref) {
		t.Fatalf("restart result count %d != %d", len(got), len(ref))
	}
	for rk := range ref {
		if _, ok := got[rk]; !ok {
			t.Fatalf("restart missing %+v", rk)
		}
	}
	logFinalResults(t, eng2, "restart-continued")
}

// TestIdempotentWrite 输出汇层面直接验证幂等：相同版本忽略，更新版本覆盖。
func TestIdempotentWrite(t *testing.T) {
	s := newIdempotentSink()
	r1 := WindowResult{Key: "a", Sum: 1, Count: 1}
	if !s.Write(r1, 1) {
		t.Fatal("first write should apply")
	}
	if s.Write(r1, 1) {
		t.Fatal("same version write must be ignored (idempotent)")
	}
	if s.Write(r1, 0) {
		t.Fatal("older version write must be ignored")
	}
	r2 := r1
	r2.Sum, r2.Count = 3, 2
	if !s.Write(r2, 2) {
		t.Fatal("newer version write should overwrite")
	}
	entries := s.snapshot()
	if len(entries) != 1 {
		t.Fatalf("sink size=%d want 1", len(entries))
	}
	for _, e := range entries {
		if e.Result.Sum != 3 || e.Version != 2 {
			t.Fatalf("sink holds %+v, want sum=3 v2", e)
		}
	}
	t.Log("幂等写入验证通过: 同版本忽略、旧版本拒绝、新版本覆盖")
}

// TestBoundedMemory 长时间运行下窗口与去重表均不得无界增长。
func TestBoundedMemory(t *testing.T) {
	cfg := Config{
		WindowSize:      time.Second,
		AllowedLateness: time.Second,
		DedupMaxSize:    64,
		PartitionCount:  1,
	}
	eng := New(cfg)
	base := time.Unix(1_000_000, 0).UTC()

	const n = 5000
	for i := 0; i < n; i++ {
		ev := Event{
			ID:        fmt.Sprintf("e-%d", i),
			Key:       fmt.Sprintf("k-%d", i%5),
			Value:     int64(i),
			EventTime: base.Add(time.Duration(i) * 200 * time.Millisecond),
		}
		if err := eng.ProcessEvent(ev); err != nil {
			t.Fatalf("event %d: %v", i, err)
		}
		if i%5 == 0 {
			// 推进水位线（略落后事件时间），触发窗口清理
			wm := ev.EventTime.Add(-time.Second)
			if _, err := eng.AdvanceWatermark(0, wm); err != nil {
				t.Fatal(err)
			}
		}
	}

	eng.mu.Lock()
	nWin := len(eng.windows.windows)
	nDedup := len(eng.dedup.seen)
	nSink := len(eng.sink.entries)
	eng.mu.Unlock()

	t.Logf("处理 %d 事件后: 存活窗口=%d 去重表=%d 输出汇=%d", n, nWin, nDedup, nSink)
	if nDedup > cfg.DedupMaxSize {
		t.Fatalf("dedup table grew unbounded: %d > %d", nDedup, cfg.DedupMaxSize)
	}
	// 窗口只可能覆盖少数个最近窗口（windowSize=1s，事件步长 0.2s）。
	if nWin > 20 {
		t.Fatalf("window state grew unbounded: %d", nWin)
	}
	// 输出汇是最终结果存储，键空间固定为 5 个 key * 少量尚未 GC 的近期窗口。
	if nSink > 5*20 {
		t.Fatalf("sink retained too much: %d", nSink)
	}
}
