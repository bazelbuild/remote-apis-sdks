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
// It also calls flag.CommandLine.Parse() to parse flags sent directly as arguments, unless flag.Parse
// has been previously called.
func Parse() {
	if !flag.Parsed() {
		ParseFromEnv()
		flag.CommandLine.Parse(os.Args[1:])
	}
}

// ParseFromEnv parses flags which are set in environment variables using the FLAG_ prefix. That is
// for a flag named x, x=$FLAG_x if $FLAG_x is set. If the flag is set in the command line, the
// command line value of the flag takes precedence over the environment variable value.
func ParseFromEnv() {
	flag.VisitAll(func(f *flag.Flag) {
		v, ok := os.LookupEnv("FLAG_" + f.Name)
		if ok {
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
	pairs, err := parsePairs(s)
	if err != nil {
		return err
	}
	for i := 0; i < len(pairs); i += 2 {
		k, v := pairs[i], pairs[i+1]
		if _, ok := (*m)[k]; ok {
			return fmt.Errorf("key %v already defined in list of key-value pairs %v", k, s)
		}
		(*m)[k] = v
	}
	return nil
}

// Get returns the flag value as a map of strings.
func (m *StringMapValue) Get() interface{} {
	return map[string]string(*m)
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

// Get returns the flag value as a list of strings.
func (m *StringListValue) Get() interface{} {
	return []string(*m)
}

// StringListMapValue is like StringMapValue, but it allows a key to be used
// with multiple values. The command-line syntax is the same: for example,
// the string key1=a,key1=b,key2=c parses as a map with "key1" having values
// "a" and "b", and "key2" having the value "c".
type StringListMapValue map[string][]string

func (m *StringListMapValue) String() string {
	keys := make([]string, 0, len(*m))
	for key := range *m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var b bytes.Buffer
	for _, key := range keys {
		for _, value := range (*m)[key] {
			if b.Len() > 0 {
				b.WriteRune(',')
			}
			b.WriteString(key)
			b.WriteRune('=')
			b.WriteString(value)
		}
	}
	return b.String()
}

func (m *StringListMapValue) Set(s string) error {
	*m = make(map[string][]string)
	pairs, err := parsePairs(s)
	if err != nil {
		return err
	}
	for i := 0; i < len(pairs); i += 2 {
		k, v := pairs[i], pairs[i+1]
		(*m)[k] = append((*m)[k], v)
	}
	return nil
}

func (m *StringListMapValue) Get() interface{} {
	return map[string][]string(*m)
}

var matchingBracket = map[rune]rune{
	'[': ']',
	'{': '}',
	'(': ')',
}

// parsePairs parses a string of the form "key1=value1,key2=value2", returning
// a slice with an even number of strings like "key1", "value1", "key2",
// "value2". Pairs are separated by ','; keys and values are separated by '='.
//
// Commas are ignored as pair delimiters if they are:
//   - Escaped with a backslash (e.g. '\,')
//   - Contained within single (”) or double ("") quotes
//   - Nested inside matching brackets ('[]'), braces ('{}'), or parentheses ('()')
func parsePairs(s string) ([]string, error) {
	var pairs []string
	var currentPair strings.Builder
	var escaped bool
	var inQuote rune
	var stack []rune

	addPair := func(p string) error {
		if p == "" {
			return nil
		}
		k, v, ok := strings.Cut(p, "=")
		if !ok {
			return fmt.Errorf("wrong format for key=value pair: %v", p)
		}
		if k == "" {
			return fmt.Errorf("key not provided")
		}
		pairs = append(pairs, k, v)
		return nil
	}

	for _, r := range s {
		switch {
		case escaped:
			currentPair.WriteRune(r)
			escaped = false
		case r == '\\':
			escaped = true
		case inQuote != 0:
			if r == inQuote {
				inQuote = 0
			}
			currentPair.WriteRune(r)
		case r == '"' || r == '\'':
			inQuote = r
			currentPair.WriteRune(r)
		case r == ',' && len(stack) == 0:
			if err := addPair(currentPair.String()); err != nil {
				return nil, err
			}
			currentPair.Reset()
		default:
			if closing, ok := matchingBracket[r]; ok {
				stack = append(stack, closing)
			} else if len(stack) > 0 && stack[len(stack)-1] == r {
				stack = stack[:len(stack)-1]
			}
			currentPair.WriteRune(r)
		}
	}

	if err := addPair(currentPair.String()); err != nil {
		return nil, err
	}

	return pairs, nil
}
