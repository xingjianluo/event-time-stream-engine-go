package engine

import "sync"

// Step 是事件源脚本中的一步：投递一条事件，或推进本分区水位线。
type Step struct {
	Event     *Event // 非空时投递事件
	Watermark *int64 // 非空时推进水位线
}

// EventStep 构造投递事件的步骤。
func EventStep(ev Event) Step { return Step{Event: &ev} }

// WatermarkStep 构造推进水位线的步骤。
func WatermarkStep(wm int64) Step { return Step{Watermark: &wm} }

// Source 是内存事件源，模拟一个分区的实时数据流。
// Steps 按序执行，源内顺序即分区内顺序。
type Source struct {
	Partition int
	Steps     []Step
}

// Inject 并发地将多个源的脚本注入引擎：每个源一个 goroutine，
// 源内步骤严格按序，源间并发交错。返回被拒绝（迟到）的事件列表。
// 无论源间如何交错，同一批事件的最终窗口结果保持一致。
func Inject(e *Engine, sources ...*Source) []Event {
	var wg sync.WaitGroup
	var mu sync.Mutex
	var rejected []Event

	for _, src := range sources {
		wg.Add(1)
		go func(s *Source) {
			defer wg.Done()
			for _, st := range s.Steps {
				switch {
				case st.Event != nil:
					ev := *st.Event
					ev.Partition = s.Partition
					if err := e.Ingest(ev); err != nil {
						if _, ok := err.(*LateEventError); ok {
							mu.Lock()
							rejected = append(rejected, ev)
							mu.Unlock()
						}
					}
				case st.Watermark != nil:
					e.AdvanceWatermark(s.Partition, *st.Watermark)
				}
			}
		}(src)
	}
	wg.Wait()
	return rejected
}
