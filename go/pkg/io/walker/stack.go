package walker

import "fmt"

type stack struct {
	stack []any
}

func (s *stack) push(items ...any) {
	s.stack = append(s.stack, items...)
}

func (s *stack) insert(item any, i int) {
	if i < 0 || i > len(s.stack) {
		panic(fmt.Errorf("stack index %d out of range with stack length %d", i, len(s.stack)))
	}

	if i == len(s.stack) {
		s.stack = append(s.stack, item)
		return
	}

	// Duplicate item at index i and shift subsequent items right.
	s.stack = append(s.stack[:i+1], s.stack[i:]...)
	// Replace the duplicated item with the specified one.
	s.stack[i] = item
}

func (s *stack) pop() any {
	p := s.peek()
	if p == nil {
		return nil
	}
	s.stack = s.stack[:len(s.stack)-1]
	return p
}

func (s *stack) peek() any {
	i := len(s.stack) - 1
	if i < 0 {
		return nil
	}
	p := s.stack[i]
	return p
}

func (s *stack) len() int {
	return len(s.stack)
}
