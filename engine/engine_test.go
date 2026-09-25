package engine

import (
	"testing"
	"time"
)

func TestEngineConstructAndClose(t *testing.T) {
	eng := New(DefaultConfig())
	if eng == nil {
		t.Fatal("expected engine")
	}
	if eng.Watermark().String() != (time.Time{}).String() {
		t.Fatal("new engine should have zero watermark")
	}
	if err := eng.Close(); err != nil {
		t.Fatal(err)
	}
	if err := eng.Close(); err != nil {
		t.Fatalf("double close should be harmless: %v", err)
	}
	if err := eng.ProcessEvent(Event{ID: "x"}); err != ErrEngineClosed {
		t.Fatalf("closed engine must reject events, got %v", err)
	}
}
