package main

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/xingjianluo/event-time-stream-engine-go/engine"
)

func main() {
	cfg := engine.Config{
		WindowSize:          10 * time.Second,
		AllowedLateness:     5 * time.Second,
		DedupMaxSize:        10000,
		PartitionCount:      1,
		RetainClosedResults: true,
	}
	fx := engine.MakeBatchFixture()

	fmt.Println("=== 阶段1：乱序注入（wm=10 之前），随后提交水位线 10 ===")
	eng := engine.New(cfg)
	// 故意打乱 Stage1 顺序，证明到达顺序无关
	for _, ev := range engine.ShuffleOrder(fx.Stage1, 7) {
		must(eng.ProcessEvent(ev))
	}
	wm, err := eng.AdvanceWatermark(0, fx.Base.Add(10*time.Second))
	must(err)
	printResults(eng, wm)

	fmt.Println("=== 阶段2：注入 wm=10..20 之间的事件，随后提交水位线 20 ===")
	for _, ev := range fx.Stage2 {
		must(eng.ProcessEvent(ev))
	}
	wm, err = eng.AdvanceWatermark(0, fx.Base.Add(20*time.Second))
	must(err)
	printResults(eng, wm)

	fmt.Println("=== 阶段3：迟到修正 / 重复投递 / 超迟到拒绝 ===")
	var dup, late int
	for _, ev := range fx.Stage3 {
		err := eng.ProcessEvent(ev)
		switch {
		case errors.Is(err, engine.ErrDuplicateEvent):
			dup++
		case errors.Is(err, engine.ErrLateEventDropped):
			late++
		case err != nil:
			fail(err)
		}
	}
	fmt.Printf("重复忽略=%d 超迟到拒绝=%d\n", dup, late)
	wm, _ = eng.AdvanceWatermark(0, fx.Base.Add(30*time.Second))
	printResults(eng, wm)

	fmt.Println("=== 阶段4：在已提交水位线 30 处做快照并模拟崩溃 ===")
	cpPath := "checkpoint-demo.json"
	cp := eng.Snapshot()
	if err := engine.SaveCheckpoint(cpPath, cp); err != nil {
		fail(err)
	}
	fmt.Printf("快照: watermark=%s -> %s\n",
		cp.CommittedWatermark.Format(time.RFC3339), cpPath)
	must(eng.Close())

	fmt.Println("=== 阶段5：重启恢复，重放全部事件，验证不重复不丢失 ===")
	eng2 := engine.New(cfg)
	loaded, err := engine.LoadCheckpoint(cpPath)
	if err != nil {
		fail(err)
	}
	if err := eng2.Restore(loaded); err != nil {
		fail(err)
	}
	// 模拟消息队列把整批消息重新投递一遍：
	// 已在快照前处理过的事件必须被去重或迟到策略拦截，结论不变。
	replayed := 0
	replayAll := func(evs []engine.Event) {
		for _, ev := range evs {
			err := eng2.ProcessEvent(ev)
			switch {
			case errors.Is(err, engine.ErrDuplicateEvent),
				errors.Is(err, engine.ErrLateEventDropped):
				replayed++
			case err != nil:
				fail(err)
			default:
				replayed++
			}
		}
	}
	replayAll(fx.Stage1)
	replayAll(fx.Stage2)
	replayAll(fx.Stage3)
	fmt.Printf("重放事件 %d 条，全部幂等处理，结果未变化：\n", replayed)
	printResults(eng2, eng2.Watermark())

	fmt.Println("=== 阶段6：非法恢复与水位线回退必须被明确拒绝 ===")
	bad := *loaded
	bad.CommittedWatermark = loaded.CommittedWatermark.Add(-time.Second)
	if err := eng2.Restore(&bad); err != nil {
		fmt.Printf("  拒绝旧快照恢复: %v\n", err)
	}
	if _, err := eng2.AdvanceWatermark(0, loaded.CommittedWatermark.Add(-time.Second)); err != nil {
		fmt.Printf("  拒绝水位线回退: %v\n", err)
	}
	must(eng2.Close())
}

func printResults(eng *engine.Engine, wm time.Time) {
	fmt.Printf("当前水位线: %s\n", wm.Format("15:04:05"))
	for _, e := range eng.Results() {
		fmt.Printf("  key=%s window=[%s,%s) sum=%d count=%d v%d\n",
			e.Result.Key, e.Result.WindowStart.Format("15:04:05"),
			e.Result.WindowEnd.Format("15:04:05"), e.Result.Sum, e.Result.Count, e.Version)
	}
}

func must(err error) {
	if err != nil {
		fail(err)
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "fatal:", err)
	os.Exit(1)
}
