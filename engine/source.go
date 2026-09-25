package engine

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"
)

// SourceStats 统计一次注入运行的结果。
type SourceStats struct {
	Delivered int // 实际被接受并影响聚合的事件数
	Duplicate int // 被去重忽略的事件数
	Late      int // 因超过允许迟到被拒绝的事件数
}

// PartitionedSource 内存多分区事件源，模拟实时数据流。
//
// 每个分区由独立 goroutine 按 Offset 顺序注入（保证同一分区/来源内
// 同一键的事件顺序语义）；不同分区并发注入，引擎内部用互斥锁串行化处理。
type PartitionedSource struct {
	partitions [][]Event
	delay      time.Duration

	mu    sync.Mutex
	stats SourceStats
}

// NewPartitionedSource 按分区构造事件源。
// 若事件未设置 Offset，则按传入顺序补齐；各分区事件按 Offset 排序。
func NewPartitionedSource(events [][]Event, delay time.Duration) *PartitionedSource {
	cp := make([][]Event, len(events))
	for p, evs := range events {
		cp[p] = append([]Event(nil), evs...)
		for i := range cp[p] {
			if cp[p][i].Offset == 0 {
				cp[p][i].Offset = int64(i)
			}
			if cp[p][i].Partition == 0 && p != 0 {
				cp[p][i].Partition = p
			}
		}
		sort.SliceStable(cp[p], func(i, j int) bool {
			return cp[p][i].Offset < cp[p][j].Offset
		})
	}
	return &PartitionedSource{partitions: cp, delay: delay}
}

// Stats 返回本次运行的注入统计。
func (s *PartitionedSource) Stats() SourceStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stats
}

// Run 并发注入各分区事件。
//
//   - offsets：各分区起始偏移（重启时从快照恢复，实现不重复不丢失）；
//   - watermarks：各分区计划上报的分区水位线（按事件时间排序），
//     当分区流到相应事件时间时上报，最后补发剩余水位线。
//
// 重复与迟到事件计入统计而不视为失败。
func (s *PartitionedSource) Run(
	ctx context.Context,
	eng *Engine,
	offsets map[int]int64,
	watermarks map[int][]time.Time,
) error {
	var (
		wg       sync.WaitGroup
		errOnce  sync.Once
		firstErr error
	)
	record := func(err error) {
		if err != nil {
			errOnce.Do(func() { firstErr = err })
		}
	}

	for p, evs := range s.partitions {
		wg.Add(1)
		go func(partition int, events []Event) {
			defer wg.Done()
			start := offsets[partition]
			// 复制并排序水位线计划。
			sched := append([]time.Time(nil), watermarks[partition]...)
			sort.Slice(sched, func(i, j int) bool { return sched[i].Before(sched[j]) })
			wi := 0

			for _, ev := range events {
				if ev.Offset < start {
					continue // 重启恢复：跳过快照之前已消费的偏移
				}
				select {
				case <-ctx.Done():
					record(ctx.Err())
					return
				default:
				}

				if err := eng.ProcessEvent(ev); err != nil {
					switch {
					case errors.Is(err, ErrDuplicateEvent):
						s.bump(func(st *SourceStats) { st.Duplicate++ })
					case errors.Is(err, ErrLateEventDropped):
						s.bump(func(st *SourceStats) { st.Late++ })
					default:
						record(err)
						return
					}
				} else {
					s.bump(func(st *SourceStats) { st.Delivered++ })
				}

				// 随流推进：上报所有不晚于当前事件时间的计划水位线。
				for wi < len(sched) && !sched[wi].After(ev.EventTime) {
					if _, err := eng.AdvanceWatermark(partition, sched[wi]); err != nil {
						record(err)
						return
					}
					wi++
				}

				if s.delay > 0 {
					select {
					case <-time.After(s.delay):
					case <-ctx.Done():
						record(ctx.Err())
						return
					}
				}
			}

			// 流结束后补发剩余水位线（触发全部窗口输出）。
			for ; wi < len(sched); wi++ {
				select {
				case <-ctx.Done():
					record(ctx.Err())
					return
				default:
				}
				if _, err := eng.AdvanceWatermark(partition, sched[wi]); err != nil {
					record(err)
					return
				}
			}
		}(p, evs)
	}
	wg.Wait()
	return firstErr
}

func (s *PartitionedSource) bump(f func(*SourceStats)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	f(&s.stats)
}
