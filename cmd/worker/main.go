// worker 演示“进程彻底退出后再新起一个进程”的续传：
//
//	phase=1：新进程从空目录起跑，消费两段分区记录的前缀，提交检查点（含数据
//	         位置与锚点）后进程结束；
//	phase=2：另一个全新进程从同一目录打开落盘结果汇、读取检查点、校验数据位置，
//	         只投递未处理后缀，再次提交检查点后结束。
//
// 两个进程之间没有任何内存共享，只有目录里的 checkpoint.json 与 results.json。
package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/xingjianluo/event-time-stream-engine-go/engine"
)

func main() {
	dir := flag.String("dir", "", "state/output directory (checkpoint.json, results.json)")
	phase := flag.Int("phase", 1, "1=first process, 2=resumed process")
	flag.Parse()
	if *dir == "" {
		log.Fatal("--dir is required")
	}
	cpPath := *dir + "/checkpoint.json"
	sinkPath := *dir + "/results.json"

	cfg := engine.Config{WindowSize: 1000, AllowedLateness: 500, MaxSeenIDs: 10000}
	sink, err := engine.OpenFileSink(sinkPath, engine.RetentionPolicy{RetainMillis: 100000})
	if err != nil {
		log.Fatalf("open sink: %v", err)
	}
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
			log.Printf("window  EMIT  key=%s window=%s sum=%d count=%d", r.Key, r.Window, r.Sum, r.Count)
		},
		OnReclaim: func(rep engine.ReclaimReport) {
			log.Printf("reclaim wm=%d retain=%d before=%d after=%d evicted=%d",
				rep.Watermark, rep.RetainMillis, rep.Before, rep.After, len(rep.Evicted))
		},
	}

	eng := engine.NewEngine(cfg, sink, hooks)
	var cp engine.Checkpoint
	loaded, err := engine.LoadCheckpointFile(cpPath)
	switch {
	case err == nil:
		if err := eng.Restore(loaded.Snapshot); err != nil {
			log.Fatalf("restore: %v", err)
		}
		cp = loaded
		log.Printf("RESUMED from checkpoint version=%d committed=%d positions=%v",
			loaded.Version, loaded.Snapshot.Committed, loaded.Positions)
	case errors.Is(err, engine.ErrNoCheckpoint):
		log.Println("FRESH start (no checkpoint)")
	default:
		log.Fatalf("load checkpoint: %v", err)
	}

	p0 := engine.NewRecordedPartition(0, workerSteps(0))
	p1 := engine.NewRecordedPartition(1, workerSteps(1))
	if err := engine.OpenPositions(cp, p0, p1); err != nil {
		log.Fatalf("open source positions: %v", err)
	}

	if *phase == 1 {
		// 中断点：p0 投前 3 步，p1 投前 2 步，然后提交检查点并让进程结束。
		deliver(eng, p0, 3)
		deliver(eng, p1, 2)
	} else {
		// 新进程从检查点位置打开后，只投递未处理后缀。
		engine.InjectRecorded(eng, p0, p1)
	}

	cp = engine.Checkpoint{
		Snapshot:  eng.Snapshot(),
		Positions: map[int]uint64{0: p0.Position(), 1: p1.Position()},
		Anchors:   map[int]string{0: p0.Anchor(), 1: p1.Anchor()},
	}
	if err := engine.SaveCheckpointFile(cpPath, cp); err != nil {
		log.Fatalf("save checkpoint: %v", err)
	}
	log.Printf("COMMITTED checkpoint committed=%d positions=%v",
		cp.Snapshot.Committed, cp.Positions)
	fmt.Fprintf(os.Stderr, "phase=%d done, sink results=%d\n", *phase, sink.Len())
}

// workerSteps 与引擎包测试使用同形状的确定性数据（跨包复制，模拟外部数据源）。
func workerSteps(p int) []engine.Step {
	if p == 0 {
		return []engine.Step{
			engine.EventStep(engine.Event{ID: "w-p0-e1", Key: "a", Value: 1, EventTime: 100}),
			engine.EventStep(engine.Event{ID: "w-p0-e2", Key: "a", Value: 2, EventTime: 200}),
			engine.WatermarkStep(1200),
			engine.EventStep(engine.Event{ID: "w-p0-e3", Key: "a", Value: 4, EventTime: 1100}),
			engine.WatermarkStep(3499),
		}
	}
	return []engine.Step{
		engine.EventStep(engine.Event{ID: "w-p1-e1", Key: "b", Value: 7, EventTime: 300}),
		engine.WatermarkStep(1200),
		engine.EventStep(engine.Event{ID: "w-p1-e2", Key: "b", Value: 9, EventTime: 1500}),
		engine.EventStep(engine.Event{ID: "w-p1-e3", Key: "a", Value: 10, EventTime: 150}),
		engine.WatermarkStep(3499),
	}
}

// deliver 确定性地从一个分区投递至多 n 条记录。
func deliver(eng *engine.Engine, p *engine.RecordedPartition, n int) {
	for i := 0; i < n; i++ {
		r, ok := p.Next()
		if !ok {
			return
		}
		if r.Event != nil {
			if err := eng.Ingest(*r.Event); err != nil {
				log.Fatalf("ingest: %v", err)
			}
		} else {
			eng.AdvanceWatermark(p.Partition, *r.Watermark)
		}
	}
}
