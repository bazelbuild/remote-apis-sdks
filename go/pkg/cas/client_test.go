package cas

import (
	"context"
	"testing"
	"time"
)

func TestPerCallTimeout(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ctx, cancel, withTimeout := withPerCallTimeout(ctx, time.Millisecond)
	defer cancel()

	t.Run("succeeded", func(t *testing.T) {
		withTimeout(func() {})
		if ctx.Err() != nil {
			t.Fatalf("want nil, got %s", ctx.Err())
		}
	})

	t.Run("canceled", func(t *testing.T) {
		withTimeout(func() {
			select {
			case <-ctx.Done():
			case <-time.After(time.Second):
			}
		})

		if ctx.Err() != context.Canceled {
			t.Fatalf("want %s, got %s", context.Canceled, ctx.Err())
		}
	})
}
