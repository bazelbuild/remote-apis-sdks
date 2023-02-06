// Package cache implements a cache that supports single-flight value computation.
//
// Single-flight value computation means that for concurrent callers of the cache, only one caller
// will compute the value to avoid redundant work which could be system intensive.
package cache

import (
	"sync"
)

// SingleFlight is a cache that supports single-flight value computation.
type SingleFlight struct {
	mu    sync.RWMutex // protects `store` field itself
	store sync.Map
}

type entry struct {
	compute sync.Once
	val     interface{}
	err     error
}

// LoadOrStore is similar to a sync.Map except that it receives a function that computes the value
// to store instead of the value directly. It ensures that the function is only executed once for
// concurrent callers of the LoadOrStore function.
func (s *SingleFlight) LoadOrStore(key interface{}, valFn func() (val interface{}, err error)) (interface{}, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	eUntyped, _ := s.store.LoadOrStore(key, &entry{})
	e := eUntyped.(*entry)
	e.compute.Do(func() {
		e.val, e.err = valFn()
	})
	return e.val, e.err
}

// Load is similar to a sync.Map.Load.
//
// Callers must check loaded before val and err. Their value is meaninful only
// if loaded is true. The err is not the last returned value because it would
// likely lead the reader to think that err must be checked before loaded.
//
//lint:ignore ST1008 loaded must be last, see above.
func (s *SingleFlight) Load(key interface{}) (val interface{}, err error, loaded bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var eUntyped interface{}
	eUntyped, loaded = s.store.Load(key)
	if loaded {
		e := eUntyped.(*entry)
		val = e.val
		err = e.err
	}
	return
}

// Store forcefully updates the given cache key with val. Note that unlike LoadOrStore,
// Store accepts a value instead of a valFn since it is intended to be only used in
// cases where updates are lightweight and do not involve computing the cache value.
func (s *SingleFlight) Store(key interface{}, val interface{}) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e := &entry{val: val}
	e.compute.Do(func() {}) // mark as computed
	s.store.Store(key, e)
}

// Delete removes a key from the cache.
func (s *SingleFlight) Delete(key interface{}) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.store.Delete(key)
}

// Reset invalidates all cache entries.
func (s *SingleFlight) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.store = sync.Map{}
}
