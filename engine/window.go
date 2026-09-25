package engine

import "fmt"

// Window 表示一个滚动事件时间窗口 [Start, End)。
type Window struct {
	Start int64
	End   int64
}

func (w Window) String() string { return fmt.Sprintf("[%d,%d)", w.Start, w.End) }

// WindowResult 是窗口触发后的聚合输出。
type WindowResult struct {
	Key    string
	Window Window
	Sum    int64
	Count  int64
}

// AssignWindow 按窗口大小将事件时间分配到窗口（对齐到 0 的滚动窗口）。
func AssignWindow(eventTime, size int64) Window {
	start := eventTime - mod(eventTime, size)
	return Window{Start: start, End: start + size}
}

// mod 是对负数也正确的取模。
func mod(a, b int64) int64 {
	m := a % b
	if m < 0 {
		m += b
	}
	return m
}
