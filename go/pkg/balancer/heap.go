package balancer

import (
	"container/heap"
	"context"
	"errors"
	"io"
	"sync"

	"google.golang.org/grpc"
)

// item is a connection in the queue.
type item struct {
	conn        *grpc.ClientConn
	invokeCount int
	streamCount int
}

// priorityQueue implements heap.Interface and holds Items.
type priorityQueue []*item

func (pq priorityQueue) Len() int { return len(pq) }

func (pq priorityQueue) Less(i, j int) bool {
	// Prioritize spearding streams first.
	if pq[i].streamCount < pq[j].streamCount {
		return true
	}
	return pq[i].invokeCount < pq[j].invokeCount
}

func (pq priorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

// Not used in this implementation.
func (pq *priorityQueue) Push(x any) {}

// Not used in this implementation.
func (pq *priorityQueue) Pop() any {
	return nil
}

func (pq *priorityQueue) peek() *item {
	return (*pq)[0]
}

func (pq *priorityQueue) fix() {
	heap.Fix(pq, 0)
}

// HeapConnPool is a pool of *grpc.ClientConn that are selected using a min-heap.
// That is, the least used connection is selected.
type HeapConnPool struct {
	grpc.ClientConnInterface
	io.Closer

	pq priorityQueue
	mu sync.Mutex
}

// Invoke picks up a connection from the pool and delegates the call to it.
func (p *HeapConnPool) Invoke(ctx context.Context, method string, args interface{}, reply interface{}, opts ...grpc.CallOption) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	connItem := p.pq.peek()
	connItem.invokeCount += 1
	p.pq.fix()
	return connItem.conn.Invoke(ctx, method, args, reply, opts...)
}

// NewStream picks up a connection from the pool and delegates the call to it.
func (p *HeapConnPool) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	connItem := p.pq.peek()
	connItem.streamCount += 1
	p.pq.fix()
	return connItem.conn.NewStream(ctx, desc, method, opts...)
}

// Close closes all connections in the bool.
func (p *HeapConnPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	var errs error
	for i, item := range p.pq {
		item.invokeCount = 0
		item.streamCount = 0
		heap.Fix(&p.pq, i)

		if err := item.conn.Close(); err != nil {
			errs = errors.Join(errs, err)
		}
	}
	return errs
}

// NewHeapConnPool makes a new instance of the min-heap connection pool and dials as many as poolSize connections
// using the provided dialFn.
func NewHeapConnPool(ctx context.Context, poolSize int, dialFn DialFunc) (*HeapConnPool, error) {
	pool := &HeapConnPool{}

	for i := 0; i < poolSize; i++ {
		conn, err := dialFn(ctx)
		if err != nil {
			defer pool.Close()
			return nil, err
		}
		pool.pq = append(pool.pq, &item{
			conn: conn,
		})
	}
	heap.Init(&pool.pq)
	return pool, nil
}
