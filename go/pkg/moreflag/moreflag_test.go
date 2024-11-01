package moreflag

import (
	"flag"
	"os"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestParseUnset(t *testing.T) {
	cleanup := setCommandLine(t, []string{"cmd"})
	defer cleanup()
	f := flag.String("value", "", "Some value")

	Parse()
	if *f != "" {
		t.Errorf("Flag has wrong value, want '', got %q", *f)
	}
}

func TestParseSet(t *testing.T) {
	cleanup := setCommandLine(t, []string{"cmd"})
	defer cleanup()
	f := flag.String("value", "", "Some value")
	os.Setenv("FLAG_value", "test")
	defer os.Setenv("FLAG_value", "")
	Parse()
	if *f != "test" {
		t.Errorf("Flag has wrong value, want 'test', got %q", *f)
	}
}

func TestParseCommandLineWins(t *testing.T) {
	cleanup := setCommandLine(t, []string{"cmd", "--value=cmd"})
	defer cleanup()
	f := flag.String("value", "", "Some value")
	os.Setenv("FLAG_value", "test")
	defer os.Setenv("FLAG_value", "")
	Parse()
	if *f != "cmd" {
		t.Errorf("Flag has wrong value, want 'cmd', got %q", *f)
	}
}

func setCommandLine(t *testing.T, args []string) func() {
	t.Helper()
	oldArgs := os.Args
	os.Args = args
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	return func() { os.Args = oldArgs }
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

func TestListMapValueSet(t *testing.T) {
	tests := []struct {
		name    string
		str     string
		wantMap map[string][]string
		wantStr string
		wantErr bool
	}{
		{
			name:    "empty",
			str:     "",
			wantMap: map[string][]string{},
		},
		{
			name:    "ok - single pair",
			str:     "type=compile",
			wantMap: map[string][]string{"type": {"compile"}},
			wantStr: "type=compile",
		},
		{
			name:    "ok - multiple pairs",
			str:     "type=compile,lang=cpp",
			wantMap: map[string][]string{"type": {"compile"}, "lang": {"cpp"}},
			wantStr: "lang=cpp,type=compile",
		},
		{
			name:    "ok - extra comma",
			str:     "type=compile,",
			wantMap: map[string][]string{"type": {"compile"}},
			wantStr: "type=compile",
		},
		{
			name:    "ok - duplicate key",
			str:     "tag=b,tag=a",
			wantMap: map[string][]string{"tag": {"b", "a"}},
			wantStr: "tag=b,tag=a",
		},
		{
			name:    "bad format",
			str:     "type=compile,langcpp",
			wantErr: true,
		},
		{
			name:    "no key",
			str:     "=val",
			wantErr: true,
		},
		{
			name:    "multiple equalities",
			str:     "type=a=b",
			wantErr: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var m map[string][]string
			slmv := (*StringListMapValue)(&m)
			if err := slmv.Set(test.str); err == nil && test.wantErr {
				t.Fatalf("StringListMapValue.Set(%v) succeeded unexpectedly", test.str)
			} else if err != nil && !test.wantErr {
				t.Fatalf("StringListMapValue.Set(%v) returned error: %v", test.str, err)
			}
			if test.wantErr {
				return
			}
			if diff := cmp.Diff(test.wantMap, m); diff != "" {
				t.Errorf("StringListMapValue.Set(%v) produced diff in map, (-want, +got): %s", test.str, diff)
			}
			got := slmv.String()
			if test.wantStr != got {
				t.Errorf("StringListMapValue.String() produced diff. Want %s, got %s", test.wantStr, got)
			}
		})
	}
}
