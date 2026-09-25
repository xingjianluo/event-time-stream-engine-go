// demo 本地演示：多分区内存事件源 -> 事件时间窗口聚合 -> 快照重启恢复 -> 幂等输出。
package main

import (
	"fmt"
	"log"

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
	}

	cfg := engine.Config{WindowSize: 1000, AllowedLateness: 500, MaxSeenIDs: 10000}
	sink := engine.NewMemSink()
	eng := engine.NewEngine(cfg, sink, hooks)

	log.Println("=== phase 1: ingest out-of-order events from 2 partitions ===")
	engine.Inject(eng,
		&engine.Source{Partition: 0, Steps: []engine.Step{
			engine.EventStep(engine.Event{ID: "e1", Key: "a", Value: 1, EventTime: 100}),
			engine.EventStep(engine.Event{ID: "e3", Key: "b", Value: 5, EventTime: 300}),
			engine.EventStep(engine.Event{ID: "e1", Key: "a", Value: 1, EventTime: 100}), // 重复投递
		}},
		&engine.Source{Partition: 1, Steps: []engine.Step{
			engine.EventStep(engine.Event{ID: "e2", Key: "a", Value: 2, EventTime: 200}),
			engine.EventStep(engine.Event{ID: "e4", Key: "a", Value: 3, EventTime: 1100}),
		}},
	)
	eng.AdvanceWatermark(0, 1200)
	eng.AdvanceWatermark(1, 1200)

	log.Println("=== snapshot at committed watermark, simulate crash & restart ===")
	snap := eng.Snapshot()
	log.Printf("snapshot committed=%d partitions=%v", snap.Committed, snap.Watermarks)

	sink2 := engine.NewMemSink()
	eng2 := engine.NewEngine(cfg, sink2, hooks)
	if err := eng2.Restore(snap); err != nil {
		log.Fatalf("restore failed: %v", err)
	}

	log.Println("=== phase 2: redelivered + late + new events after restart ===")
	engine.Inject(eng2,
		&engine.Source{Partition: 0, Steps: []engine.Step{
			engine.EventStep(engine.Event{ID: "e3", Key: "b", Value: 5, EventTime: 300}),  // 重投递，被去重表吸收
			engine.EventStep(engine.Event{ID: "e5", Key: "a", Value: 10, EventTime: 150}), // 迟到但在保留期内
		}},
		&engine.Source{Partition: 1, Steps: []engine.Step{
			engine.EventStep(engine.Event{ID: "e6", Key: "b", Value: 7, EventTime: 1500}),
		}},
	)
	eng2.AdvanceWatermark(0, 3499)
	eng2.AdvanceWatermark(1, 3499)

	log.Println("=== final merged results (idempotent by key@window) ===")
	merged := make(map[string]engine.WindowResult)
	for k, v := range sink.Results() {
		merged[k] = v
	}
	for k, v := range sink2.Results() {
		merged[k] = v
	}
	for k, v := range merged {
		fmt.Printf("  %-18s sum=%d count=%d\n", k, v.Sum, v.Count)
	}
}
