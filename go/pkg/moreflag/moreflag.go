// Package moreflag contains definitions for some useful flag types, such as maps.
package moreflag

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
)

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
	*m = StringListValue(strings.Split(s, ","))
	return nil
}
