package balancer

import (
	"context"
	"fmt"
	"testing"

	"google.golang.org/grpc"
)

func TestHeapConnPool(t *testing.T) {
	ctx := context.Background()
	dial := func(ctx context.Context) (*grpc.ClientConn, error) {
		return grpc.DialContext(ctx, "example.com", grpc.WithInsecure())
	}
	heapPool, err := NewHeapConnPool(ctx, 5, dial)
	if err != nil {
		t.Fatal(err)
	}

	if len(heapPool.pq) != 5 {
		t.Fatal(fmt.Sprintf("pool size is incorrect, want 5, got %d", len(heapPool.pq)))
	}

	// When used for Invoke (or NewStream) only, it should act as round-robin.
	for i := 0; i < 5; i++ {
		if c := heapPool.pq.peek().invokeCount; c > 0 {
			t.Errorf(fmt.Sprintf("invokeCount for item #%d should be 0, not %d", i, c))
		}
		_ = heapPool.Invoke(ctx, "foo", nil, nil)
	}
	if c := heapPool.pq.peek().invokeCount; c != 1 {
		t.Errorf(fmt.Sprintf("invokeCount should be 1, not %d", c))

	}

	// With mixed-use, NewStream calls ignore invokeCount to prioritize spreading streams across the pool.
	for i := 0; i < 5; i++ {
		if c := heapPool.pq.peek().streamCount; c > 0 {
			t.Errorf(fmt.Sprintf("streamCount for item #%d should be 0, not %d", i, c))
		}
		_, _ = heapPool.NewStream(ctx, nil, "foo")
	}
	if c := heapPool.pq.peek().streamCount; c != 1 {
		t.Errorf(fmt.Sprintf("streamCount should be 1, not %d", c))

	}
}
