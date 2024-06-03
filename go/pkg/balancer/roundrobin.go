// Package balancer implements a simple gRPC client-side load balancer.
// It mitigates https://github.com/grpc/grpc/issues/21386 by having multiple connections
// which effectively multplies the concurrent streams limit.
package balancer

import (
	"context"
	"errors"
	"io"
	"sync/atomic"

	"google.golang.org/grpc"
)

// RRConnPool is a pool of *grpc.ClientConn that are selected in a round-robin fashion.
type RRConnPool struct {
	grpc.ClientConnInterface
	io.Closer

	conns []*grpc.ClientConn
	idx   uint32 // access via sync/atomic
}

// Conn picks the next connection from the pool in a round-robin fasion.
func (p *RRConnPool) Conn() *grpc.ClientConn {
	i := atomic.AddUint32(&p.idx, 1)
	return p.conns[i%uint32(len(p.conns))]
}

// Close closes all connections in the bool.
func (p *RRConnPool) Close() error {
	var errs error
	for _, conn := range p.conns {
		if err := conn.Close(); err != nil {
			errs = errors.Join(errs, err)
		}
	}
	return errs
}

// Invoke picks up a connection from the pool and delegates the call to it.
func (p *RRConnPool) Invoke(ctx context.Context, method string, args interface{}, reply interface{}, opts ...grpc.CallOption) error {
	return p.Conn().Invoke(ctx, method, args, reply, opts...)
}

// NewStream picks up a connection from the pool and delegates the call to it.
func (p *RRConnPool) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	return p.Conn().NewStream(ctx, desc, method, opts...)
}

// DialFunc defines the dial function used in creating the pool.
type DialFunc func(ctx context.Context) (*grpc.ClientConn, error)

// NewRRConnPool makes a new instance of the round-robin connection pool and dials as many as poolSize connections
// using the provided dialFn.
func NewRRConnPool(ctx context.Context, poolSize int, dialFn DialFunc) (*RRConnPool, error) {
	pool := &RRConnPool{}
	for i := 0; i < poolSize; i++ {
		conn, err := dialFn(ctx)
		if err != nil {
			defer pool.Close()
			return nil, err
		}
		pool.conns = append(pool.conns, conn)
	}
	return pool, nil
}
