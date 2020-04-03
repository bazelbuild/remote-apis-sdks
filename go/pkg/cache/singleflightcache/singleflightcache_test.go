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
