package engine

import (
	"fmt"
	"log"
	"os"
	"sort"
	"sync"
	"time"
)

// Logger 引擎日志接口，测试可注入以打印事件、窗口与水位线变化。
type Logger interface {
	Printf(format string, args ...any)
}

// Engine 是事件时间流处理引擎：
// 事件去重 -> 迟到判定 -> 窗口聚合 -> 水位线触发 -> 幂等输出。
//
// 所有公开方法均由单一互斥锁串行化，从而在多来源并发注入时，
// 同一键（乃至同一引擎内所有键）的事件保持确定的处理顺序语义。
type Engine struct {
	cfg Config

	mu          sync.Mutex
	windows     *tumblingWindows
	dedup       *dedup
	sink        *idempotentSink
	partitionWM map[int]time.Time
	offsets     map[int]int64
	committedWM time.Time
	closed      bool
	logger      Logger
}

// Config 引擎配置。
type Config struct {
	WindowSize      time.Duration
	AllowedLateness time.Duration
	DedupMaxSize    int
	PartitionCount  int
	// RetainClosedResults 是否在输出汇中保留已彻底关闭窗口的结果。
	// false（默认）：窗口超过 end+allowedLateness 后从内存输出汇清除，
	//                保证长时间运行下引擎全部内存结构有界；
	//                需要永久留存结果时应实现持久化 Sink（Sink 接口）。
	// true：内存输出汇保留全部历史结果（等价于物化结果表，随输出基数增长）。
	RetainClosedResults bool
}

// DefaultConfig 返回合理默认配置。
func DefaultConfig() Config {
	return Config{
		WindowSize:      10 * time.Second,
		AllowedLateness: 5 * time.Second,
		DedupMaxSize:    10000,
		PartitionCount:  2,
	}
}

// New 创建引擎。
func New(cfg Config) *Engine {
	if cfg.WindowSize <= 0 {
		cfg.WindowSize = 10 * time.Second
	}
	if cfg.DedupMaxSize <= 0 {
		cfg.DedupMaxSize = 10000
	}
	if cfg.PartitionCount <= 0 {
		cfg.PartitionCount = 1
	}
	return &Engine{
		cfg:         cfg,
		windows:     newTumblingWindows(cfg.WindowSize),
		dedup:       newDedup(cfg.DedupMaxSize),
		sink:        newIdempotentSink(),
		partitionWM: make(map[int]time.Time),
		offsets:     make(map[int]int64),
		logger:      log.New(os.Stderr, "[engine] ", log.LstdFlags|log.Lmicroseconds),
	}
}

// SetLogger 替换日志输出（测试可注入 testing.T）。
func (e *Engine) SetLogger(l Logger) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.logger = l
}

// ProcessEvent 处理一条事件。
//
// 返回 ErrLateEventDropped / ErrDuplicateEvent 表示事件被明确忽略（非致命），
// 调用方可据此计数或告警。
func (e *Engine) ProcessEvent(ev Event) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return ErrEngineClosed
	}

	// 记录来源消费位置（无论事件最终是否被接受）。
	if n := ev.Offset + 1; n > e.offsets[ev.Partition] {
		e.offsets[ev.Partition] = n
	}

	// 1) 幂等去重优先（只读判定）：同一事件 ID 只产生一次影响。
	// 去重表的淘汰切点与迟到阈值一致（wm - allowedLateness），
	// 因此仍留在表内的重复事件必然不是"超迟到"事件，先判定重复语义更准确。
	if e.dedup.has(ev.ID) {
		e.logger.Printf("DUP event=%s key=%s eventTime=%s",
			ev.ID, ev.Key, ev.EventTime.Format(time.RFC3339Nano))
		return fmt.Errorf("%w: event=%s", ErrDuplicateEvent, ev.ID)
	}

	// 2) 迟到判定：eventTime + allowedLateness < 当前水位线 => 明确拒绝。
	if !e.committedWM.IsZero() && ev.EventTime.Add(e.cfg.AllowedLateness).Before(e.committedWM) {
		e.logger.Printf("DROP(late) event=%s key=%s eventTime=%s wm=%s",
			ev.ID, ev.Key, ev.EventTime.Format(time.RFC3339Nano),
			e.committedWM.Format(time.RFC3339Nano))
		return fmt.Errorf("%w: event=%s eventTime=%s watermark=%s",
			ErrLateEventDropped, ev.ID, ev.EventTime.Format(time.RFC3339Nano),
			e.committedWM.Format(time.RFC3339Nano))
	}

	// 3) 确认接受后写入去重表，再落入窗口。
	e.dedup.add(ev.ID, ev.EventTime)
	st := e.windows.add(ev)
	e.logger.Printf("ACCEPT event=%s key=%s value=%d eventTime=%s window=[%s,%s) wm=%s",
		ev.ID, ev.Key, ev.Value, ev.EventTime.Format(time.RFC3339Nano),
		st.start.Format(time.RFC3339Nano), st.end.Format(time.RFC3339Nano),
		e.committedWM.Format(time.RFC3339Nano))

	// 4) 若窗口此前已随水位线触发过，则这是允许范围内的迟到事件，
	// 立即产出修正版本（版本号递增，输出汇幂等覆盖）。
	if !st.end.After(e.committedWM) {
		r := st.emit()
		written := e.sink.Write(r, st.version)
		e.logger.Printf("CORRECT key=%s window=[%s,%s) sum=%d count=%d version=%d applied=%v",
			r.Key, r.WindowStart.Format(time.RFC3339Nano), r.WindowEnd.Format(time.RFC3339Nano),
			r.Sum, r.Count, st.version, written)
	}
	return nil
}

// AdvanceWatermark 推进某个分区的水位线；
// 全局水位线取所有分区水位线的最小值，推进后触发到期窗口输出。
// 分区水位线回退会被明确拒绝（ErrWatermarkRegression）。
func (e *Engine) AdvanceWatermark(partition int, wm time.Time) (time.Time, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return time.Time{}, ErrEngineClosed
	}
	if partition < 0 || partition >= e.cfg.PartitionCount {
		return time.Time{}, fmt.Errorf("invalid partition %d (have %d)",
			partition, e.cfg.PartitionCount)
	}
	if old := e.partitionWM[partition]; wm.Before(old) {
		return time.Time{}, fmt.Errorf("%w: partition=%d old=%s new=%s",
			ErrWatermarkRegression, partition,
			old.Format(time.RFC3339Nano), wm.Format(time.RFC3339Nano))
	}
	e.partitionWM[partition] = wm

	global := wm
	for p := 0; p < e.cfg.PartitionCount; p++ {
		pwm, ok := e.partitionWM[p]
		if !ok {
			// 尚未上报水位线的分区视为时间零点，拖住全局水位线。
			global = time.Time{}
			break
		}
		if pwm.Before(global) {
			global = pwm
		}
	}

	if global.After(e.committedWM) {
		e.onWatermark(global)
	}
	return global, nil
}

// onWatermark 提交新的全局水位线：触发到期窗口 -> 清除过期窗口/去重记录。
func (e *Engine) onWatermark(wm time.Time) {
	// 到期窗口按 (窗口起点, key) 排序输出，保证确定性。
	due := e.windows.dueWindowStates(wm)
	sort.Slice(due, func(i, j int) bool {
		if !due[i].start.Equal(due[j].start) {
			return due[i].start.Before(due[j].start)
		}
		return due[i].key < due[j].key
	})
	for _, st := range due {
		r := st.emit()
		written := e.sink.Write(r, st.version)
		e.logger.Printf("EMIT key=%s window=[%s,%s) sum=%d count=%d version=%d applied=%v wm=%s",
			r.Key, r.WindowStart.Format(time.RFC3339Nano), r.WindowEnd.Format(time.RFC3339Nano),
			r.Sum, r.Count, st.version, written, wm.Format(time.RFC3339Nano))
	}

	// 水位线提交后状态才允许被清理：
	// 超过 "窗口结束 + 允许迟到" 的窗口不再可能被修正，予以清除。
	e.windows.purge(wm, e.cfg.AllowedLateness)
	// 输出汇：默认同步 GC 已彻底关闭窗口的结果，保证内存结构全部有界。
	// 清除条件与窗口清除一致：WindowEnd + allowedLateness <= wm。
	if !e.cfg.RetainClosedResults {
		e.sink.purgeBefore(wm.Add(-e.cfg.AllowedLateness))
	}
	// 去重表淘汰切点与窗口彻底关闭点对齐：
	// eventTime < wm - allowedLateness - windowSize 的事件不可能再影响
	// 任何存活窗口。切点之前的事件重投必然被迟到策略拒绝；
	// 切点之后（含边界）的事件重投必然被去重拦截，二者语义清晰且无遗漏。
	cutoff := wm.Add(-e.cfg.AllowedLateness).Add(-e.cfg.WindowSize)
	if !cutoff.IsZero() {
		e.dedup.evictBefore(cutoff)
	}

	e.committedWM = wm
	e.logger.Printf("WATERMARK committed=%s", wm.Format(time.RFC3339Nano))
}

// Watermark 返回当前已提交的全局水位线。
func (e *Engine) Watermark() time.Time {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.committedWM
}

// Offsets 返回各分区已消费到的偏移（快照/重启用）。
func (e *Engine) Offsets() map[int]int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make(map[int]int64, len(e.offsets))
	for k, v := range e.offsets {
		out[k] = v
	}
	return out
}

// Close 关闭引擎。
func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.closed = true
	return nil
}
