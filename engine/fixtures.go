package engine

import "time"

// BatchFixture 是一组手动控制注入阶段的确定性数据。
// 配置固定为 WindowSize=10s、AllowedLateness=5s。
type BatchFixture struct {
	Base time.Time
	// Stage1 在水位线 10 之前注入（含乱序）。
	Stage1 []Event
	// Stage2 在水位线 10 之后、20 之前注入。
	Stage2 []Event
	// Stage3 在水位线 20 之后注入：含允许迟到修正、超迟到拒绝、重复投递。
	Stage3 []Event
}

// MakeBatchFixture 构造分阶段数据（单分区即可精确驱动语义；
// 多分区并发场景见 MakePartitionedFixture）。
func MakeBatchFixture() BatchFixture {
	base := time.Date(2026, 9, 26, 10, 0, 0, 0, time.UTC)
	t := func(sec int) time.Time { return base.Add(time.Duration(sec) * time.Second) }
	ev := func(id string, key string, v int64, sec int) Event {
		return Event{ID: id, Partition: 0, Key: key, Value: v, EventTime: t(sec)}
	}
	return BatchFixture{
		Base: base,
		// 窗口 [00,10): a@1=1 b@3=10；窗口 [10,20): a@11=2（乱序：时间在后面）
		Stage1: []Event{
			ev("a-01", "a", 1, 1),
			ev("b-01", "b", 10, 3),
			ev("a-03", "a", 2, 11),
		},
		// wm=10 后继续到 wm=20 前：
		// a@15=4 与 a@11=2 同窗口；b@12=20 进入 b 的 [10,20) 窗口。
		Stage2: []Event{
			ev("a-04", "a", 4, 15),
			ev("b-02", "b", 20, 12),
		},
		// wm=20 后（去重保留切点=15、迟到阈值边界=15）：
		//  a@18=8  => 18+5 >= 20，允许迟到内修正 a 的 [10,20)：2+4+8=14
		//  a-04@15 重投 => 仍在去重表（切点为开区间 Before(15)），重复忽略
		//  a@2=100 => 2+5 < 20，超迟到，明确拒绝
		//  b@16=5  => 允许迟到内修正 b 的 [10,20)：20+5=25
		Stage3: []Event{
			ev("a-06", "a", 8, 18),
			{ID: "a-04", Partition: 0, Key: "a", Value: 999, EventTime: t(15)},
			ev("a-07-late", "a", 100, 2),
			ev("b-03", "b", 5, 16),
		},
	}
}

// ExpectedFinalResults 返回分阶段数据跑完后的最终确定性结论。
func (f BatchFixture) ExpectedFinalResults() []WindowResult {
	t := func(sec int) time.Time { return f.Base.Add(time.Duration(sec) * time.Second) }
	return []WindowResult{
		{Key: "a", WindowStart: t(0), WindowEnd: t(10), Sum: 1, Count: 1},
		{Key: "b", WindowStart: t(0), WindowEnd: t(10), Sum: 10, Count: 1},
		{Key: "a", WindowStart: t(10), WindowEnd: t(20), Sum: 14, Count: 3},
		{Key: "b", WindowStart: t(10), WindowEnd: t(20), Sum: 25, Count: 2},
	}
}

// PreWM20Events 返回水位线 20 之前应被接受的全部事件，
// 用于"不同到达顺序结果一致"测试中的排列输入。
func (f BatchFixture) PreWM20Events() []Event {
	out := append([]Event(nil), f.Stage1...)
	out = append(out, f.Stage2...)
	return out
}

// PartitionedFixture 用于多分区并发注入测试。
type PartitionedFixture struct {
	Base       time.Time
	Partitions [][]Event
	Watermarks map[int][]time.Time
}

// MakePartitionedFixture 构造两分区、事件在各自分区内按偏移单调的数据。
// 分区0为键 a 的事件，分区1为键 b 的事件；
// 每个分区最后上报足够大的水位线以触发全部窗口。
func MakePartitionedFixture() PartitionedFixture {
	base := time.Date(2026, 9, 26, 10, 0, 0, 0, time.UTC)
	t := func(sec int) time.Time { return base.Add(time.Duration(sec) * time.Second) }
	p0 := []Event{
		{ID: "a-01", Partition: 0, Offset: 0, Key: "a", Value: 1, EventTime: t(1)},
		{ID: "a-02", Partition: 0, Offset: 1, Key: "a", Value: 2, EventTime: t(4)},
		{ID: "a-03", Partition: 0, Offset: 2, Key: "a", Value: 4, EventTime: t(12)},
		{ID: "a-04", Partition: 0, Offset: 3, Key: "a", Value: 8, EventTime: t(16)},
	}
	p1 := []Event{
		{ID: "b-01", Partition: 1, Offset: 0, Key: "b", Value: 10, EventTime: t(2)},
		{ID: "b-02", Partition: 1, Offset: 1, Key: "b", Value: 20, EventTime: t(13)},
	}
	wms := []time.Time{t(10), t(20)}
	return PartitionedFixture{
		Base:       base,
		Partitions: [][]Event{p0, p1},
		Watermarks: map[int][]time.Time{0: wms, 1: append([]time.Time(nil), wms...)},
	}
}

// ExpectedPartitionedResults 返回并发注入跑完后的确定性结论。
func (f PartitionedFixture) ExpectedResults() []WindowResult {
	t := func(sec int) time.Time { return f.Base.Add(time.Duration(sec) * time.Second) }
	return []WindowResult{
		{Key: "a", WindowStart: t(0), WindowEnd: t(10), Sum: 3, Count: 2},
		{Key: "b", WindowStart: t(0), WindowEnd: t(10), Sum: 10, Count: 1},
		{Key: "a", WindowStart: t(10), WindowEnd: t(20), Sum: 12, Count: 2},
		{Key: "b", WindowStart: t(10), WindowEnd: t(20), Sum: 20, Count: 1},
	}
}

// SplitByPartition 将事件列表按分区切分。
func SplitByPartition(events []Event, partitions int) [][]Event {
	out := make([][]Event, partitions)
	for _, e := range events {
		p := e.Partition
		if p >= partitions {
			p = partitions - 1
		}
		out[p] = append(out[p], e)
	}
	return out
}

// ShuffleOrder 以固定算法打乱事件顺序（确定性 xorshift，按 seed）。
func ShuffleOrder(events []Event, seed uint64) []Event {
	out := append([]Event(nil), events...)
	state := seed
	if state == 0 {
		state = 1
	}
	next := func() uint64 {
		state ^= state << 13
		state ^= state >> 7
		state ^= state << 17
		return state
	}
	for i := len(out) - 1; i > 0; i-- {
		j := int(next() % uint64(i+1))
		out[i], out[j] = out[j], out[i]
	}
	return out
}
