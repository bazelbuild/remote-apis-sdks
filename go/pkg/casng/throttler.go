package casng

import (
	"context"
)

// throttler provides a simple semaphore interface to limit in-flight goroutines.
type throttler struct {
	ch chan struct{}
}

// acquire blocks until a token can be acquired from the pool.
//
// Returns false if ctx expires before a token is available. Otherwise returns true.
func (t *throttler) acquire(ctx context.Context) bool {
	for {
		select {
		case t.ch <- struct{}{}:
			return true
		case <-ctx.Done():
			return false
		}
	}
}

// release returns a token to the pool. Must be called after acquire. Otherwise, it will block until acquire is called.
func (t *throttler) release() {
	<-t.ch
}

// len returns the number of acquired tokens.
func (t *throttler) len() int {
	return len(t.ch)
}

// newThrottler creates a new instance that allows up to n tokens to be acquired.
func newThrottler(n int64) *throttler {
	return &throttler{ch: make(chan struct{}, n)}
}
