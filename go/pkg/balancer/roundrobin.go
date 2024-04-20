package balancer

import (
	"context"
	"errors"
	"io"
	"sync/atomic"

	"google.golang.org/grpc"
)

type roundRobinConnPool struct {
	grpc.ClientConnInterface
	io.Closer

	conns []*grpc.ClientConn
	idx   uint32 // access via sync/atomic
}

func (p *roundRobinConnPool) Conn() *grpc.ClientConn {
	i := atomic.AddUint32(&p.idx, 1)
	return p.conns[i%uint32(len(p.conns))]
}

func (p *roundRobinConnPool) Close() error {
	var errs error
	for _, conn := range p.conns {
		if err := conn.Close(); err != nil {
			errs = errors.Join(errs, err)
		}
	}
	return errs
}

func (p *roundRobinConnPool) Invoke(ctx context.Context, method string, args interface{}, reply interface{}, opts ...grpc.CallOption) error {
	return p.Conn().Invoke(ctx, method, args, reply, opts...)
}

func (p *roundRobinConnPool) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	return p.Conn().NewStream(ctx, desc, method, opts...)
}

type DialFunc func(ctx context.Context) (*grpc.ClientConn, error)

func NewRoundRobinBalancer(ctx context.Context, poolSize int, dialFn DialFunc) (grpc.ClientConnInterface, error) {
	pool := &roundRobinConnPool{}
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
