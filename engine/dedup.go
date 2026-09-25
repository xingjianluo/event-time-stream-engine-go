package engine

import (
	"sort"
	"time"
)

// dedupEntry 记录事件 ID 及其事件时间。
type dedupEntry struct {
	id        string
	eventTime time.Time
}

// dedup 基于事件 ID 的有界去重器。
//
// 内存有界策略：
//  1. 水印推进时调用 evictBefore 淘汰事件时间早于
//     "水印 - 允许迟到" 的记录（这些事件即使重投也会被迟到策略拒绝）；
//  2. 若记录数仍超过 maxSize，则按事件时间最老优先硬淘汰，
//     保证长时间运行下内存不无限增长。
type dedup struct {
	maxSize      int
	seen         map[string]time.Time
	minTimestamp time.Time
}

func newDedup(maxSize int) *dedup {
	if maxSize <= 0 {
		maxSize = 10000
	}
	return &dedup{
		maxSize: maxSize,
		seen:    make(map[string]time.Time),
	}
}

// has 只读判断 ID 是否存在，不改变去重表（迟到事件不应污染去重表）。
func (d *dedup) has(id string) bool {
	_, ok := d.seen[id]
	return ok
}

// add 写入一条新记录（调用前需先以 has 确认不存在）。
func (d *dedup) add(id string, eventTime time.Time) {
	d.seen[id] = eventTime
	if len(d.seen) > d.maxSize {
		d.evictOldest(len(d.seen) - d.maxSize)
	}
}

// evictBefore 清理事件时间早于 cutoff 的所有记录，并推进 minTimestamp。
func (d *dedup) evictBefore(cutoff time.Time) {
	if cutoff.After(d.minTimestamp) {
		d.minTimestamp = cutoff
	}
	for id, t := range d.seen {
		if t.Before(cutoff) {
			delete(d.seen, id)
		}
	}
}

// evictOldest 按事件时间升序淘汰 n 条最老记录；时间相同按 ID 排序保证确定性。
func (d *dedup) evictOldest(n int) {
	if n <= 0 {
		return
	}
	entries := make([]dedupEntry, 0, len(d.seen))
	for id, t := range d.seen {
		entries = append(entries, dedupEntry{id: id, eventTime: t})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].eventTime.Equal(entries[j].eventTime) {
			return entries[i].id < entries[j].id
		}
		return entries[i].eventTime.Before(entries[j].eventTime)
	})
	if n > len(entries) {
		n = len(entries)
	}
	for i := 0; i < n; i++ {
		delete(d.seen, entries[i].id)
	}
}

// snapshot 导出确定性排序的 ID 列表与对应事件时间。
func (d *dedup) snapshot() ([]dedupEntry, time.Time) {
	entries := make([]dedupEntry, 0, len(d.seen))
	for id, t := range d.seen {
		entries = append(entries, dedupEntry{id: id, eventTime: t})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].eventTime.Equal(entries[j].eventTime) {
			return entries[i].id < entries[j].id
		}
		return entries[i].eventTime.Before(entries[j].eventTime)
	})
	return entries, d.minTimestamp
}

// restore 从快照重建去重状态。
func (d *dedup) restore(entries []dedupEntry, minTimestamp time.Time) {
	d.seen = make(map[string]time.Time, len(entries))
	d.minTimestamp = minTimestamp
	for _, e := range entries {
		d.seen[e.id] = e.eventTime
	}
}
