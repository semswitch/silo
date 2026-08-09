//go:build !integration

package devicegroup

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestDirtyIdleWaitBlocksUntilStateChange(t *testing.T) {
	wake := make(chan struct{})
	returned := make(chan struct{})
	var calls atomic.Int32
	hooks := &MigrateDirtyHooks{WaitWhenIdle: func(name string) error {
		if name != "memory" {
			t.Fatalf("idle device = %q, want memory", name)
		}
		calls.Add(1)
		<-wake
		return nil
	}}

	go func() {
		_ = hooks.WaitWhenIdle("memory")
		close(returned)
	}()

	time.Sleep(25 * time.Millisecond)
	if calls.Load() != 1 {
		t.Fatalf("idle wait calls = %d, want 1", calls.Load())
	}
	select {
	case <-returned:
		t.Fatal("idle wait returned before state changed")
	default:
	}
	close(wake)
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("idle wait did not wake after state changed")
	}
}
