package fitz

import (
	"context"
	"sync"
)

// WakeGate is a small versioned condition primitive for notification-driven
// consumer loops. Wake increments the version; WaitAfter blocks until a newer
// version is available or the context is canceled.
type WakeGate struct {
	mu      sync.Mutex
	version uint64
	waitCh  chan struct{}
}

// NewWakeGate returns a ready-to-use wake gate.
func NewWakeGate() *WakeGate {
	return &WakeGate{waitCh: make(chan struct{})}
}

// Wake increments the gate version and releases all current waiters.
func (g *WakeGate) Wake() uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.ensureLocked()
	g.version++
	close(g.waitCh)
	g.waitCh = make(chan struct{})
	return g.version
}

// Wait blocks until the next wake and returns the observed version.
func (g *WakeGate) Wait(ctx context.Context) (uint64, error) {
	g.mu.Lock()
	g.ensureLocked()
	version := g.version
	g.mu.Unlock()
	return g.WaitAfter(ctx, version)
}

// WaitAfter blocks until the gate version is greater than version.
func (g *WakeGate) WaitAfter(ctx context.Context, version uint64) (uint64, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		g.mu.Lock()
		g.ensureLocked()
		if g.version > version {
			next := g.version
			g.mu.Unlock()
			return next, nil
		}
		waitCh := g.waitCh
		g.mu.Unlock()

		select {
		case <-waitCh:
		case <-ctx.Done():
			return version, ctx.Err()
		}
	}
}

func (g *WakeGate) ensureLocked() {
	if g.waitCh == nil {
		g.waitCh = make(chan struct{})
	}
}
