// demo 本地演示：多分区内存事件源 -> 事件时间窗口聚合 -> 进度落盘 ->
// 进程重启后从磁盘恢复、数据源按位置续传 -> 幂等输出 + 结果保留回收。
package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/xingjianluo/event-time-stream-engine-go/engine"
)

func main() {
	hooks := engine.Hooks{
		OnEvent: func(ev engine.Event, accepted bool, reason string) {
			status := "ACCEPT"
			if !accepted {
				status = "DROP(" + reason + ")"
			}
			log.Printf("event   %-12s id=%s key=%s value=%d eventTime=%d partition=%d",
				status, ev.ID, ev.Key, ev.Value, ev.EventTime, ev.Partition)
		},
		OnEmit: func(r engine.WindowResult) {
			log.Printf("window  EMIT         key=%s window=%s sum=%d count=%d", r.Key, r.Window, r.Sum, r.Count)
		},
		OnWatermark: func(partition int, wm int64, global int64) {
			log.Printf("watermark ADVANCE    partition=%d wm=%d global=%d", partition, wm, global)
		},
		OnPurge: func(key string, w engine.Window) {
			log.Printf("window  PURGE        key=%s window=%s", key, w)
		},
		OnResultExpire: func(r engine.WindowResult) {
			log.Printf("result  EXPIRE       key=%s window=%s sum=%d (retention elapsed)", r.Key, r.Window, r.Sum)
		},
	}

	cfg := engine.Config{
		WindowSize:      1000,
		AllowedLateness: 500,
		MaxSeenIDs:      10000,
		ResultRetention: 2500, // 结果过保留期后被回收，历史输出有界
	}

	// 两个分区的完整数据（真实部署中是外部消息源，这里用内存脚本模拟）。
	srcs := []*engine.Source{
		{Partition: 0, Steps: []engine.Step{
			engine.EventStep(engine.Event{ID: "e1", Key: "a", Value: 1, EventTime: 100, Seq: 1}),
			engine.EventStep(engine.Event{ID: "e3", Key: "b", Value: 5, EventTime: 300, Seq: 2}),
			engine.EventStep(engine.Event{ID: "e5", Key: "a", Value: 10, EventTime: 150, Seq: 3}), // 迟到修正
			engine.EventStep(engine.Event{ID: "e7", Key: "b", Value: 7, EventTime: 1500, Seq: 4}),
		}},
		{Partition: 1, Steps: []engine.Step{
			engine.EventStep(engine.Event{ID: "e2", Key: "a", Value: 2, EventTime: 200, Seq: 1}),
			engine.EventStep(engine.Event{ID: "e4", Key: "a", Value: 3, EventTime: 1100, Seq: 2}),
			engine.EventStep(engine.Event{ID: "e6", Key: "c", Value: 9, EventTime: 2500, Seq: 3}),
		}},
	}

	dir, err := os.MkdirTemp("", "engine-demo")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	store := &engine.FileCheckpointStore{Path: filepath.Join(dir, "checkpoint.json")}

	log.Println("=== process A: consume first half, persist checkpoint, exit ===")
	sinkA := engine.NewMemSink()
	engA := engine.NewEngine(cfg, sinkA, hooks)
	engine.Inject(engA,
		&engine.Source{Partition: 0, Steps: srcs[0].Steps[:2]},
		&engine.Source{Partition: 1, Steps: srcs[1].Steps[:2]},
	)
	engA.AdvanceWatermark(0, 1200)
	engA.AdvanceWatermark(1, 1200)
	if err := store.Save(engA.Snapshot()); err != nil {
		log.Fatal(err)
	}
	log.Printf("checkpoint saved to %s (process A exits, memory state gone)", store.Path)

	log.Println("=== process B: load checkpoint from disk, resume sources from offsets ===")
	snap, err := store.Load()
	if err != nil {
		log.Fatalf("load checkpoint: %v", err)
	}
	sinkB := engine.NewMemSink()
	engB := engine.NewEngine(cfg, sinkB, hooks)
	if err := engB.Restore(snap); err != nil {
		log.Fatalf("restore: %v", err)
	}
	offsets := engB.Offsets()
	log.Printf("restored committed=%d offsets=%v", snap.Committed, offsets)

	resumed := make([]*engine.Source, 0, len(srcs))
	for _, s := range srcs {
		r, err := s.ResumeFrom(offsets[s.Partition])
		if err != nil {
			log.Fatalf("resume partition %d: %v", s.Partition, err)
		}
		log.Printf("partition %d resumes after seq=%d (%d steps skipped, %d remaining)",
			s.Partition, offsets[s.Partition].Seq, len(s.Steps)-len(r.Steps), len(r.Steps))
		resumed = append(resumed, r)
	}
	engine.Inject(engB, resumed...)
	engB.AdvanceWatermark(0, 3499)
	engB.AdvanceWatermark(1, 3499)

	log.Println("=== final merged results (idempotent by key@window) ===")
	merged := make(map[string]engine.WindowResult)
	for k, v := range sinkA.Results() {
		merged[k] = v
	}
	for k, v := range sinkB.Results() {
		merged[k] = v
	}
	for k, v := range merged {
		fmt.Printf("  %-18s sum=%d count=%d\n", k, v.Sum, v.Count)
	}

	log.Println("=== retention: advance watermark, expired history is reclaimed ===")
	engB.AdvanceWatermark(0, 6000)
	engB.AdvanceWatermark(1, 6000)
	log.Printf("sinkB now holds %d result(s) (bounded history)", len(sinkB.Results()))
}
