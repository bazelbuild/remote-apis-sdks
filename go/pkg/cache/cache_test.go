package cache

import (
	"testing"
)

const (
	ns1  = "namespace1"
	ns2  = "namespace2"
	key1 = "key1"
	key2 = "key2"
	val1 = "val1"
	val2 = "val2"
)

func TestMultipleNamespaces(t *testing.T) {
	c := GetInstance()
	defer c.Reset()

	got1, err := c.LoadOrStore(ns1, key1, func() (interface{}, error) { return val1, nil })
	if err != nil {
		t.Errorf("LoadOrStore(%v,%v) returned error: %v", ns1, key1, err)
	}
	if got1 != val1 {
		t.Errorf("LoadOrStore(%v,%v) loaded wrong value: got %v, want %v", ns1, key1, got1, val1)
	}
	got2, err := c.LoadOrStore(ns2, key1, func() (interface{}, error) { return val2, nil })
	if err != nil {
		t.Errorf("LoadOrStore(%v,%v) returned error: %v", ns2, key1, err)
	}
	if got2 != val2 {
		t.Errorf("LoadOrStore(%v,%v) loaded wrong value: got %v, want %v", ns2, key1, got2, val2)
	}
	got3, err := c.LoadOrStore(ns1, key1, func() (interface{}, error) { return val1, nil })
	if err != nil {
		t.Errorf("LoadOrStore(%v,%v) returned error: %v", ns1, key1, err)
	}
	if got3 != val1 {
		t.Errorf("LoadOrStore(%v,%v) loaded wrong value: got %v, want %v", ns1, key1, got3, val1)
	}
}

func TestDelete(t *testing.T) {
	c := GetInstance()
	defer c.Reset()

	got1, err := c.LoadOrStore(ns1, key1, func() (interface{}, error) { return val1, nil })
	if err != nil {
		t.Fatalf("LoadOrStore(%v,%v) returned error: %v", ns1, key1, err)
	}
	if got1 != val1 {
		t.Fatalf("LoadOrStore(%v,%v) loaded wrong value: got %v, want %v", ns1, key1, got1, val1)
	}
	got2, err := c.LoadOrStore(ns2, key1, func() (interface{}, error) { return val2, nil })
	if err != nil {
		t.Fatalf("LoadOrStore(%v,%v) returned error: %v", ns2, key1, err)
	}
	if got2 != val2 {
		t.Fatalf("LoadOrStore(%v,%v) loaded wrong value: got %v, want %v", ns2, key1, got2, val2)
	}

	err = c.Delete(ns1, key1)
	if err != nil {
		t.Errorf("Delete(%v,%v) returned error: %v", ns1, key1, err)
	}

	got1, err = c.LoadOrStore(ns1, key1, func() (interface{}, error) { return val2, nil })
	if err != nil {
		t.Errorf("LoadOrStore(%v,%v) returned error: %v", ns1, key1, err)
	}
	if got1 != val2 {
		t.Errorf("LoadOrStore(%v,%v) loaded wrong value: got %v, want %v", ns1, key1, got1, val2)
	}
	got2, err = c.LoadOrStore(ns2, key1, func() (interface{}, error) { return val1, nil })
	if err != nil {
		t.Errorf("LoadOrStore(%v,%v) returned error: %v", ns2, key1, err)
	}
	if got2 != val2 {
		t.Errorf("LoadOrStore(%v,%v) loaded wrong value: got %v, want %v", ns2, key1, got2, val2)
	}
}
