package balancer

import (
	"sync"
	"testing"
	"time"

	"github.com/pborman/uuid"

	grpcbalancer "google.golang.org/grpc/balancer"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/resolver"
)

func TestGCPBalancer_UpdatingConnectionStateIsMutuallyExclusive(t *testing.T) {
	builder := newBuilder()
	var subconns []*fakeSubConn
	for i := 0; i < MinConnections; i++ {
		subconns = append(subconns, &fakeSubConn{id: uuid.New()})
	}

	cc := fakeClientConn{subconns: subconns}
	balancer := builder.Build(cc, grpcbalancer.BuildOptions{})
	var wg sync.WaitGroup
	wg.Add(2)

	// Invoke both UpdateClientConnState() and UpdatesubConnState() simultaneously
	// and make sure that it doesn't result in a race condition. The test would fail
	// if there's a race condition.
	go func() {
		balancer.UpdateClientConnState(grpcbalancer.ClientConnState{})
		wg.Done()
	}()
	go func() {
		for i := 0; i < MinConnections; i++ {
			// Sending a connectivity.Ready state will actually trigger regenerating the picker
			// which is where we expect the race condition to occur.
			balancer.UpdateSubConnState(subconns[i], grpcbalancer.SubConnState{ConnectivityState: connectivity.Ready})
		}
		wg.Done()
	}()
	wg.Wait()
}

type fakeClientConn struct {
	grpcbalancer.ClientConn

	subconns []*fakeSubConn
}

var (
	// Note: These cannot be moved to fakeClientConn since gRPC Builder() interface
	// does not accept a pointer to fakeClientConn and so the mutex will be copied if
	// passed by value.
	// https://godoc.org/google.golang.org/grpc/balancer#Builder
	idx int
	mu  sync.Mutex
)

func (f fakeClientConn) NewSubConn([]resolver.Address, grpcbalancer.NewSubConnOptions) (grpcbalancer.SubConn, error) {
	mu.Lock()
	defer mu.Unlock()
	idx++
	idx = idx % MinConnections
	return f.subconns[idx], nil
}

func (f fakeClientConn) RemoveSubConn(grpcbalancer.SubConn) {}

func (f fakeClientConn) UpdateState(grpcbalancer.State) {}

func (f fakeClientConn) ResolveNow(resolver.ResolveNowOptions) {}

func (f fakeClientConn) Target() string {
	return ""
}

type fakeSubConn struct {
	id string
}

func (fakeSubConn) UpdateAddresses([]resolver.Address) {}

func (fakeSubConn) Connect() {
	// Sleep to simulate connecting to an actual server.
	time.Sleep(100 * time.Millisecond)
}

func (fakeSubConn) GetOrBuildProducer(grpcbalancer.ProducerBuilder) (grpcbalancer.Producer, func()) {
	return nil, nil
}

func (fakeSubConn) Shutdown() {}
