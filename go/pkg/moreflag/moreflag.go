// Package moreflag contains definitions for some useful flag types, such as maps.
package moreflag

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
)

// Parse parses flags which are set in environment variables using the FLAG_ prefix. That is
// for a flag named x, x=$FLAG_x if $FLAG_x is set. If the flag is set in the command line, the
// command line value of the flag takes precedence over the environment variable value.
// It also calls flag.Parse() to parse flags sent directly as arguments.
func Parse() {
	ParseFromEnv()
	flag.Parse()
}

// ParseFromEnv parses flags which are set in environment variables using the FLAG_ prefix. That is
// for a flag named x, x=$FLAG_x if $FLAG_x is set. If the flag is set in the command line, the
// command line value of the flag takes precedence over the environment variable value.
func ParseFromEnv() {
	flag.VisitAll(func(f *flag.Flag) {
		v := os.Getenv("FLAG_" + f.Name)
		if v != "" {
			flag.Set(f.Name, v)
		}
	})
}

// StringMapValue is a command line flag that interprets a string in the format key1=value1,key2=value2
// as a map.
type StringMapValue map[string]string

// String retrieves the flag's map in the format key1=value1,key2=value, sorted by keys.
func (m *StringMapValue) String() string {
	// Construct the output in key sorted order
	keys := make([]string, 0, len(*m))
	for key := range *m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var b bytes.Buffer
	for i, key := range keys {
		if i > 0 {
			b.WriteRune(',')
		}
		b.WriteString(key)
		b.WriteRune('=')
		b.WriteString((*m)[key])
	}
	return b.String()
}

// Set updates the map with key and value pair(s) in the format key1=value1,key2=value2.
func (m *StringMapValue) Set(s string) error {
	*m = make(map[string]string)
	pairs := strings.Split(s, ",")
	for _, p := range pairs {
		if p == "" {
			continue
		}
		pair := strings.Split(p, "=")
		if len(pair) != 2 {
			return fmt.Errorf("wrong format for key-value pair: %v", p)
		}
		if pair[0] == "" {
			return fmt.Errorf("key not provided")
		}
		if _, ok := (*m)[pair[0]]; ok {
			return fmt.Errorf("key %v already defined in list of key-value pairs %v", pair[0], s)
		}
		(*m)[pair[0]] = pair[1]
	}
	return nil
}

// StringListValue is a command line flag that interprets a string as a list of comma-separated values.
type StringListValue []string

// String returns the list value.
func (m *StringListValue) String() string {
	return strings.Join(*m, ",")
}

// Set for StringListValue accepts one list of comma-separated values.
func (m *StringListValue) Set(s string) error {
	splitFn := func(c rune) bool {
		return c == ','
	}
	*m = StringListValue(strings.FieldsFunc(s, splitFn))
	return nil
}
