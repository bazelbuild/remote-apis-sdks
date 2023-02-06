// Package balancer is a forked version of https://github.com/GoogleCloudPlatform/grpc-gcp-go.
package balancer

import (
	"sync"
	"sync/atomic"

	"google.golang.org/grpc/balancer"

	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/resolver"
)

const (
	// Name is the name of grpc_gcp balancer.
	Name = "grpc_gcp"

	healthCheckEnabled = true
)

var (
	// DefaultMinConnections is the default number of gRPC sub-connections the
	// gRPC balancer should create during SDK initialization.
	DefaultMinConnections = 5

	// MinConnections is the minimum number of gRPC sub-connections the gRPC balancer
	// should create during SDK initialization.
	// It is initialized in flags package.
	MinConnections = DefaultMinConnections
)

func init() {
	balancer.Register(newBuilder())
}

type gcpBalancerBuilder struct {
	name string
}

// Build returns a grpc balancer initialized with given build options.
func (bb *gcpBalancerBuilder) Build(
	cc balancer.ClientConn,
	opt balancer.BuildOptions,
) balancer.Balancer {
	return &gcpBalancer{
		cc:          cc,
		affinityMap: make(map[string]balancer.SubConn),
		scRefs:      make(map[balancer.SubConn]*subConnRef),
		scStates:    make(map[balancer.SubConn]connectivity.State),
		csEvltr:     &connectivityStateEvaluator{},
		// Initialize picker to a picker that always return
		// ErrNoSubConnAvailable, because when state of a SubConn changes, we
		// may call UpdateState with this picker.
		picker: newErrPicker(balancer.ErrNoSubConnAvailable),
	}
}

// Name returns the name of the balancer.
func (*gcpBalancerBuilder) Name() string {
	return Name
}

// newBuilder creates a new grpcgcp balancer builder.
func newBuilder() balancer.Builder {
	return &gcpBalancerBuilder{
		name: Name,
	}
}

// connectivityStateEvaluator gets updated by addrConns when their
// states transition, based on which it evaluates the state of
// ClientConn.
type connectivityStateEvaluator struct {
	numReady            uint64 // Number of addrConns in ready state.
	numConnecting       uint64 // Number of addrConns in connecting state.
	numTransientFailure uint64 // Number of addrConns in transientFailure.
}

// recordTransition records state change happening in every subConn and based on
// that it evaluates what aggregated state should be.
// It can only transition between Ready, Connecting and TransientFailure. Other states,
// Idle and Shutdown are transitioned into by ClientConn; in the beginning of the connection
// before any subConn is created ClientConn is in idle state. In the end when ClientConn
// closes it is in Shutdown state.
//
// recordTransition should only be called synchronously from the same goroutine.
func (cse *connectivityStateEvaluator) recordTransition(
	oldState,
	newState connectivity.State,
) connectivity.State {
	// Update counters.
	for idx, state := range []connectivity.State{oldState, newState} {
		updateVal := 2*uint64(idx) - 1 // -1 for oldState and +1 for new.
		switch state {
		case connectivity.Ready:
			cse.numReady += updateVal
		case connectivity.Connecting:
			cse.numConnecting += updateVal
		case connectivity.TransientFailure:
			cse.numTransientFailure += updateVal
		}
	}

	// Evaluate.
	if cse.numReady > 0 {
		return connectivity.Ready
	}
	if cse.numConnecting > 0 {
		return connectivity.Connecting
	}
	return connectivity.TransientFailure
}

// subConnRef keeps reference to the real SubConn with its
// connectivity state, affinity count and streams count.
type subConnRef struct {
	subConn     balancer.SubConn
	affinityCnt int32 // Keeps track of the number of keys bound to the subConn
	streamsCnt  int32 // Keeps track of the number of streams opened on the subConn
}

func (ref *subConnRef) getAffinityCnt() int32 {
	return atomic.LoadInt32(&ref.affinityCnt)
}

func (ref *subConnRef) getStreamsCnt() int32 {
	return atomic.LoadInt32(&ref.streamsCnt)
}

func (ref *subConnRef) affinityIncr() {
	atomic.AddInt32(&ref.affinityCnt, 1)
}

func (ref *subConnRef) affinityDecr() {
	atomic.AddInt32(&ref.affinityCnt, -1)
}

func (ref *subConnRef) streamsIncr() {
	atomic.AddInt32(&ref.streamsCnt, 1)
}

func (ref *subConnRef) streamsDecr() {
	atomic.AddInt32(&ref.streamsCnt, -1)
}

type gcpBalancer struct {
	balancer.Balancer // Embed V1 Balancer so it compiles with Builder
	addrs             []resolver.Address
	cc                balancer.ClientConn
	csEvltr           *connectivityStateEvaluator
	state             connectivity.State

	mu          sync.RWMutex
	affinityMap map[string]balancer.SubConn
	scStates    map[balancer.SubConn]connectivity.State
	scRefs      map[balancer.SubConn]*subConnRef

	picker balancer.Picker
}

func (gb *gcpBalancer) UpdateClientConnState(ccs balancer.ClientConnState) error {
	addrs := ccs.ResolverState.Addresses
	gb.addrs = addrs

	createdSubCons := false
	for len(gb.scRefs) < MinConnections {
		gb.newSubConn()
		createdSubCons = true
	}
	if createdSubCons {
		return nil
	}

	for _, scRef := range gb.scRefs {
		scRef.subConn.UpdateAddresses(addrs)
		scRef.subConn.Connect()
	}

	return nil
}

func (gb *gcpBalancer) ResolverError(err error) {
	grpclog.Warningf(
		"grpcgcp.gcpBalancer: ResolverError: %v",
		err,
	)
}

// check current connection pool size
func (gb *gcpBalancer) getConnectionPoolSize() int {
	gb.mu.Lock()
	defer gb.mu.Unlock()
	return len(gb.scRefs)
}

// newSubConn creates a new SubConn using cc.NewSubConn and initialize the subConnRef.
func (gb *gcpBalancer) newSubConn() {
	gb.mu.Lock()
	defer gb.mu.Unlock()

	// there are chances the newly created subconns are still connecting,
	// we can wait on those new subconns.
	for _, scState := range gb.scStates {
		if scState == connectivity.Connecting {
			return
		}
	}

	sc, err := gb.cc.NewSubConn(
		gb.addrs,
		balancer.NewSubConnOptions{HealthCheckEnabled: healthCheckEnabled},
	)
	if err != nil {
		grpclog.Errorf("grpcgcp.gcpBalancer: failed to NewSubConn: %v", err)
		return
	}
	gb.scRefs[sc] = &subConnRef{
		subConn: sc,
	}
	gb.scStates[sc] = connectivity.Idle
	sc.Connect()
}

// getReadySubConnRef returns a subConnRef and a bool. The bool indicates whether
// the boundKey exists in the affinityMap. If returned subConnRef is a nil, it
// means the underlying subconn is not READY yet.
func (gb *gcpBalancer) getReadySubConnRef(boundKey string) (*subConnRef, bool) {
	gb.mu.RLock()
	defer gb.mu.RUnlock()

	if sc, ok := gb.affinityMap[boundKey]; ok {
		if gb.scStates[sc] != connectivity.Ready {
			// It's possible that the bound subconn is not in the readySubConns list,
			// If it's not ready yet, we throw ErrNoSubConnAvailable
			return nil, true
		}
		return gb.scRefs[sc], true
	}
	return nil, false
}

// bindSubConn binds the given affinity key to an existing subConnRef.
func (gb *gcpBalancer) bindSubConn(bindKey string, sc balancer.SubConn) {
	gb.mu.Lock()
	defer gb.mu.Unlock()
	_, ok := gb.affinityMap[bindKey]
	if !ok {
		gb.affinityMap[bindKey] = sc
	}
	gb.scRefs[sc].affinityIncr()
}

// unbindSubConn removes the existing binding associated with the key.
func (gb *gcpBalancer) unbindSubConn(boundKey string) {
	gb.mu.Lock()
	defer gb.mu.Unlock()
	boundSC, ok := gb.affinityMap[boundKey]
	if ok {
		gb.scRefs[boundSC].affinityDecr()
		if gb.scRefs[boundSC].getAffinityCnt() <= 0 {
			delete(gb.affinityMap, boundKey)
		}
	}
}

// regeneratePicker takes a snapshot of the balancer, and generates a picker
// from it. The picker is
//   - errPicker with ErrTransientFailure if the balancer is in TransientFailure,
//   - built by the pickerBuilder with all READY SubConns otherwise.
func (gb *gcpBalancer) regeneratePicker() {
	gb.mu.RLock()
	defer gb.mu.RUnlock()

	if gb.state == connectivity.TransientFailure {
		gb.picker = newErrPicker(balancer.ErrTransientFailure)
		return
	}
	readyRefs := []*subConnRef{}

	// Select ready subConns from subConn map.
	for sc, scState := range gb.scStates {
		if scState == connectivity.Ready {
			readyRefs = append(readyRefs, gb.scRefs[sc])
		}
	}
	gb.picker = newGCPPicker(readyRefs, gb)
}

func (gb *gcpBalancer) UpdateSubConnState(sc balancer.SubConn, scs balancer.SubConnState) {
	s := scs.ConnectivityState
	grpclog.Infof("grpcgcp.gcpBalancer: handle SubConn state change: %p, %v", sc, s)

	gb.mu.Lock()
	oldS, ok := gb.scStates[sc]
	if !ok {
		grpclog.Infof(
			"grpcgcp.gcpBalancer: got state changes for an unknown SubConn: %p, %v",
			sc,
			s,
		)
		gb.mu.Unlock()
		return
	}
	gb.scStates[sc] = s
	switch s {
	case connectivity.Idle:
		sc.Connect()
	case connectivity.Shutdown:
		delete(gb.scRefs, sc)
		delete(gb.scStates, sc)
	}
	gb.mu.Unlock()

	oldAggrState := gb.state
	gb.state = gb.csEvltr.recordTransition(oldS, s)

	// Regenerate picker when one of the following happens:
	//  - this sc became ready from not-ready
	//  - this sc became not-ready from ready
	//  - the aggregated state of balancer became TransientFailure from non-TransientFailure
	//  - the aggregated state of balancer became non-TransientFailure from TransientFailure
	if (s == connectivity.Ready) != (oldS == connectivity.Ready) ||
		(gb.state == connectivity.TransientFailure) != (oldAggrState == connectivity.TransientFailure) {
		gb.regeneratePicker()
		gb.cc.UpdateState(balancer.State{ConnectivityState: gb.state, Picker: gb.picker})
	}
}

func (gb *gcpBalancer) Close() {
}
