package balancer

import (
	"context"

	"google.golang.org/grpc"
)

// DialFunc defines the dial function used in creating the pool.
type DialFunc func(ctx context.Context) (*grpc.ClientConn, error)
