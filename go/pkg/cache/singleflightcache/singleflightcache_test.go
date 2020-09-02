package singleflightcache

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

const (
	key1 = "key1"
	key2 = "key2"
	val1 = "val1"
	val2 = "val2"
	key3 = "key3"
	val3 = "val3"
)

func TestSimpleValueStore(t *testing.T) {
	c := &Cache{}
	val, err := c.LoadOrStore(key1, func() (interface{}, error) { return val1, nil })
	if err != nil {
		t.Errorf("LoadOrStore(%v) failed: %v", key1, err)
	}
	if val != val1 {
		t.Errorf("LoadOrStore(%v) loaded wrong value: got %v, want %v", key1, val, val1)
	}

	val, err = c.LoadOrStore(key2, func() (interface{}, error) { return val2, nil })
	if err != nil {
		t.Errorf("LoadOrStore(%v) failed: %v", key2, err)
	}
	if val != val2 {
		t.Errorf("LoadOrStore(%v) loaded wrong value: got %v, want %v", key2, val, val2)
	}
}

func TestSingleFlightStore(t *testing.T) {
	c := &Cache{}
	var ops uint64
	loadFn := func() (interface{}, error) {
		atomic.AddUint64(&ops, 1)
		return val1, nil
	}
	wg := &sync.WaitGroup{}
	load := func() {
		val, err := c.LoadOrStore(key1, loadFn)
		if err != nil {
			t.Errorf("LoadOrStore(%v) failed: %v", key1, err)
		}
		if val != val1 {
			t.Errorf("LoadOrStore(%v) loaded wrong value: got %v, want %v", key1, val, val1)
		}
		wg.Done()
	}
	wg.Add(50)
	for i := 0; i < 50; i++ {
		go load()
	}
	wg.Wait()

	if ops != 1 {
		t.Errorf("Wrong number of loads executed: got %v, want 1", ops)
	}
}

func TestValFnFailure(t *testing.T) {
	c := &Cache{}
	val, err := c.LoadOrStore(key1, func() (interface{}, error) { return nil, errors.New("error") })
	if err == nil {
		t.Errorf("LoadOrStore(%v) failed: %v", key1, err)
	}

	val, err = c.LoadOrStore(key1, func() (interface{}, error) { return val1, nil })
	if err != nil {
		t.Errorf("LoadOrStore(%v) failed: %v", key1, err)
	}
	if val != val1 {
		t.Errorf("LoadOrStore(%v) loaded wrong value: got %v, want %v", key1, val, val1)
	}
}

func TestValFnMultipleFailure(t *testing.T) {
	c := &Cache{}

	wg := &sync.WaitGroup{}
	want := errors.New("error")
	var ops uint64
	load := func() {
		_, got := c.LoadOrStore(key1, func() (interface{}, error) {
			atomic.AddUint64(&ops, 1)
			return nil, want
		})
		if want != got {
			t.Errorf("LoadOrStore(%v) = _,%v, want _,%v", key1, got, want)
		}
		wg.Done()
	}
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go load()
	}
	wg.Wait()

	if ops != 50 {
		t.Errorf("Wrong number of loads executed: got %v, want 50", ops)
	}
}

func TestDelete(t *testing.T) {
	c := &Cache{}
	val, err := c.LoadOrStore(key1, func() (interface{}, error) { return val1, nil })
	if err != nil {
		t.Fatalf("LoadOrStore(%v) failed: %v", key1, err)
	}
	if val != val1 {
		t.Fatalf("LoadOrStore(%v) loaded wrong value: got %v, want %v", key1, val, val1)
	}

	c.Delete(key1)

	val, err = c.LoadOrStore(key1, func() (interface{}, error) { return val2, nil })
	if err != nil {
		t.Errorf("LoadOrStore(%v) failed: %v", key1, err)
	}
	if val != val2 {
		t.Errorf("LoadOrStore(%v) loaded wrong value: got %v, want %v", key1, val, val2)
	}
}

// This test launches several concurrent goroutines to load/store and delete keys from the map. The
// purpose of the test is to expose whether a race condition would result in an inconsistent state
// of the internal maps of the cache, which would result in an error reported by LoadOrStore.
func TestLoadDelete(t *testing.T) {
	c := &Cache{}
	wg := &sync.WaitGroup{}
	load := func() {
		val, err := c.LoadOrStore(key1, func() (interface{}, error) { return val1, nil })
		if err != nil {
			t.Errorf("LoadOrStore(%v) failed: %v", key1, err)
		}
		if val != val1 {
			t.Errorf("LoadOrStore(%v) loaded wrong value: got %v, want %v", key1, val, val1)
		}
		val, err = c.LoadOrStore(key2, func() (interface{}, error) { return val2, nil })
		if err != nil {
			t.Errorf("LoadOrStore(%v) failed: %v", key2, err)
		}
		if val != val2 {
			t.Errorf("LoadOrStore(%v) loaded wrong value: got %v, want %v", key2, val, val2)
		}
		wg.Done()
	}
	del := func() {
		c.Delete(key1)
		c.Delete(key2)
		wg.Done()
	}
	wg.Add(100)
	for i := 0; i < 50; i++ {
		go load()
		go del()
	}
	wg.Wait()
}

func TestStore(t *testing.T) {
	c := &Cache{}
	wg := &sync.WaitGroup{}
	var mu sync.Mutex
	load := func() {
		mu.Lock()
		if err := c.Store(key3, val3); err != nil {
			t.Errorf("Store(%v) failed: %v", key3, err)
		}
		// LoadOrStore should load the already loaded value "val3" and shouldn't
		// overwrite "val1" to "key3".
		val, err := c.LoadOrStore(key3, func() (interface{}, error) { return val1, nil })
		if err != nil {
			t.Errorf("LoadOrStore(%v) failed: %v", key3, err)
		}
		mu.Unlock()
		if val != val3 {
			t.Errorf("LoadOrStore(%v) = %v, want %v", key3, val, val3)
		}
		wg.Done()
	}
	del := func() {
		mu.Lock()
		c.Delete(key3)
		mu.Unlock()
		wg.Done()
	}
	wg.Add(100)
	for i := 0; i < 50; i++ {
		go load()
		go del()
	}
	wg.Wait()
}

func TestStoreOverwrite(t *testing.T) {
	c := &Cache{}

	val, err := c.LoadOrStore(key3, func() (interface{}, error) { return val1, nil })
	if err != nil {
		t.Errorf("LoadOrStore(%v) failed: %v", key3, err)
	}
	if val != val1 {
		t.Errorf("LoadOrStore(%v) = %v, want %v", key3, val, val1)
	}

	if err := c.Store(key3, val3); err != nil {
		t.Errorf("Store(%v) failed: %v", key3, err)
	}

	// LoadOrStore should load the already loaded value "val3" and shouldn't
	// overwrite "val1" to "key3".
	val, err = c.LoadOrStore(key3, func() (interface{}, error) { return val1, nil })
	if err != nil {
		t.Errorf("LoadOrStore(%v) failed: %v", key3, err)
	}
	if val != val3 {
		t.Errorf("LoadOrStore(%v) = %v, want %v", key3, val, val3)
	}
}

// This test purposefully tests if there is a race condition in concurrent
// go-routines writing / deleting to the same key value - it is not expected
// that the end result of the key in the cache is a specific value which is why
// the output of LoadOrStore is not tested for correctness. You can run the race
// detector using: "bazelisk test --features race //go/pkg/cache/..."
func TestRacedValueStore(t *testing.T) {
	c := &Cache{}
	wg := &sync.WaitGroup{}
	load := func() {
		if _, err := c.LoadOrStore(key1, func() (interface{}, error) { return val1, nil }); err != nil {
			t.Errorf("LoadOrStore(%v) failed: %v", key1, err)
		}
		wg.Done()
	}
	load2 := func() {
		if _, err := c.LoadOrStore(key1, func() (interface{}, error) { return val2, nil }); err != nil {
			t.Errorf("LoadOrStore(%v) failed: %v", key1, err)
		}
		wg.Done()
	}
	del := func() {
		c.Delete(key1)
		wg.Done()
	}
	wg.Add(150)
	for i := 0; i < 50; i++ {
		go load()
		go load2()
		go del()
	}
	wg.Wait()
}
