package moreflag

import (
	"flag"
	"os"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestParseFromEnv(t *testing.T) {
	f := flag.String("value", "", "Some value")

	ParseFromEnv()
	if *f != "" {
		t.Errorf("Flag has wrong value, want '', got %q", *f)
	}

	os.Setenv("FLAG_value", "test")
	defer os.Setenv("FLAG_value", "")
	ParseFromEnv()
	if *f != "test" {
		t.Errorf("Flag has wrong value, want 'test', got %q", *f)
	}
}

func TestMapValueSet(t *testing.T) {
	tests := []struct {
		name    string
		str     string
		wantMap map[string]string
		wantStr string
	}{
		{
			name:    "ok - single pair",
			str:     "type=compile",
			wantMap: map[string]string{"type": "compile"},
			wantStr: "type=compile",
		},
		{
			name:    "ok - multiple pairs",
			str:     "type=compile,lang=cpp",
			wantMap: map[string]string{"type": "compile", "lang": "cpp"},
			wantStr: "lang=cpp,type=compile",
		},
		{
			name:    "ok - extra comma",
			str:     "type=compile,",
			wantMap: map[string]string{"type": "compile"},
			wantStr: "type=compile",
		},
		{
			name:    "empty",
			str:     "",
			wantMap: map[string]string{},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var m map[string]string
			mv := (*StringMapValue)(&m)
			if err := mv.Set(test.str); err != nil {
				t.Errorf("StringMapValue.Set(%v) returned error: %v", test.str, err)
			}
			if diff := cmp.Diff(test.wantMap, (map[string]string)(*mv)); diff != "" {
				t.Errorf("StringMapValue.Set(%v) produced diff in map, (-want +got): %s", test.str, diff)
			}
			got := mv.String()
			if test.wantStr != got {
				t.Errorf("StringMapValue.String() produced diff. Want %s, got %s", test.wantStr, got)
			}
		})
	}
}

func TestMapValueMultipleSet(t *testing.T) {
	var m map[string]string
	mv := (*StringMapValue)(&m)
	pair1 := "key1=value1"
	if err := mv.Set(pair1); err != nil {
		t.Errorf("StringMapValue.Set(%v) returned error: %v", pair1, err)
	}
	if diff := cmp.Diff(map[string]string{"key1": "value1"}, (map[string]string)(*mv)); diff != "" {
		t.Errorf("StringMapValue.Set(%v) produced diff in map, (-want +got): %s", pair1, diff)
	}
	pair2 := "key2=value2"
	if err := mv.Set(pair2); err != nil {
		t.Errorf("StringMapValue.Set(%v) returned error: %v", pair2, err)
	}
	if diff := cmp.Diff(map[string]string{"key2": "value2"}, (map[string]string)(*mv)); diff != "" {
		t.Errorf("StringMapValue.Set(%v) produced diff in map, (-want +got): %s", pair2, diff)
	}
}

func TestMapValueMultipleSetDuplicate(t *testing.T) {
	var m map[string]string
	mv := (*StringMapValue)(&m)
	pair1 := "key1=value1"
	if err := mv.Set(pair1); err != nil {
		t.Errorf("StringMapValue.Set(%v) returned error: %v", pair1, err)
	}
	if diff := cmp.Diff(map[string]string{"key1": "value1"}, (map[string]string)(*mv)); diff != "" {
		t.Errorf("StringMapValue.Set(%v) produced diff in map, (-want +got): %s", pair1, diff)
	}
	pair2 := "key1=value2"
	if err := mv.Set(pair2); err != nil {
		t.Errorf("StringMapValue.Set(%v) returned error: %v", pair2, err)
	}
	if diff := cmp.Diff(map[string]string{"key1": "value2"}, (map[string]string)(*mv)); diff != "" {
		t.Errorf("StringMapValue.Set(%v) produced diff in map, (-want +got): %s", pair2, diff)
	}
}

func TestMapValueSetErrors(t *testing.T) {
	tests := []struct {
		name string
		str  string
	}{
		{
			name: "bad format",
			str:  "type=compile,langcpp",
		},
		{
			name: "no key",
			str:  "=val",
		},
		{
			name: "multiple equalities",
			str:  "type=a=b",
		},
		{
			name: "duplicate keys",
			str:  "type=compile,type=link",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var m map[string]string
			mv := (*StringMapValue)(&m)
			if err := mv.Set(test.str); err == nil {
				t.Errorf("StringMapValue.Set(%v) = nil, want error", test.str)
			}
		})
	}
}

func TestListValueSet(t *testing.T) {
	tests := []struct {
		name     string
		str      string
		wantList []string
		wantStr  string
	}{
		{
			name:     "ok - single val",
			str:      "foo",
			wantList: []string{"foo"},
			wantStr:  "foo",
		},
		{
			name:     "ok - multiple vals",
			str:      "foo,bar",
			wantList: []string{"foo", "bar"},
			wantStr:  "foo,bar",
		},
		{
			name:     "ok - extra comma",
			str:      "foo,",
			wantList: []string{"foo"},
			wantStr:  "foo",
		},
		{
			name:     "ok - double comma",
			str:      "foo,,bar",
			wantList: []string{"foo", "bar"},
			wantStr:  "foo,bar",
		},
		{
			name:     "empty",
			str:      "",
			wantList: []string{},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var l []string
			lv := (*StringListValue)(&l)
			if err := lv.Set(test.str); err != nil {
				t.Errorf("StringListValue.Set(%v) returned error: %v", test.str, err)
			}
			if diff := cmp.Diff(test.wantList, ([]string)(*lv)); diff != "" {
				t.Errorf("StringListValue.Set(%v) produced diff in map, (-want +got): %s", test.str, diff)
			}
			got := lv.String()
			if test.wantStr != got {
				t.Errorf("StringListValue.String() produced diff. Want %s, got %s", test.wantStr, got)
			}
		})
	}
}
