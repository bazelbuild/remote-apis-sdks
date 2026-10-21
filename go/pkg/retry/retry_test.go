package retry

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func alwaysRetry(error) bool { return true }

// failer returns an error until its counter reaches 0, at which point it returns finalErr, which is
// nil by default (no error).
type failer struct {
	attempts int
	finalErr error
}

func (f *failer) run() error {
	f.attempts--
	if f.attempts < 0 {
		return f.finalErr
	}
	return errors.New("failing")
}

func policyString(bp BackoffPolicy) string {
	base := fmt.Sprintf("baseDelay: %v, maxDelay: %v", bp.baseDelay, bp.maxDelay)
	if bp.maxAttempts == 0 {
		return base + ": unlimited retries"
	}
	return base + fmt.Sprintf(": max %d attempts", bp.maxAttempts)
}

func TestRetries(t *testing.T) {
	cases := []struct {
		policy      BackoffPolicy
		sr          ShouldRetry
		attempts    int
		finalErr    error
		wantError   bool
		wantErrCode codes.Code
	}{
		{
			policy:   ExponentialBackoff(time.Millisecond, time.Millisecond, UnlimitedAttempts),
			sr:       alwaysRetry,
			attempts: 5,
		},
		{
			policy:      ExponentialBackoff(time.Millisecond, time.Millisecond, 5),
			sr:          alwaysRetry,
			attempts:    5,
			finalErr:    status.Error(codes.Unimplemented, "unimplemented!"),
			wantError:   true,
			wantErrCode: codes.Unimplemented,
		},
		{
			policy:    ExponentialBackoff(time.Millisecond, time.Millisecond, 1),
			sr:        alwaysRetry,
			wantError: true,
			attempts:  1,
		},
		{
			policy:    ExponentialBackoff(time.Millisecond, time.Millisecond, 2),
			sr:        alwaysRetry,
			wantError: true,
			attempts:  2,
		},
		{
			policy:   ExponentialBackoff(time.Millisecond, time.Millisecond, 5),
			sr:       alwaysRetry,
			attempts: 5,
		},
	}
	ctx := context.Background()
	for _, c := range cases {
		f := failer{
			attempts: 4,
			finalErr: c.finalErr,
		}
		err := WithPolicy(context.WithValue(ctx, TimeAfterContextKey, func(time.Duration) <-chan time.Time {
			c := make(chan time.Time)
			close(c) // Reading from the closed channel will immediately succeed.
			return c
		}), c.sr, c.policy, f.run)
		attempts := 4 - f.attempts
		if attempts != c.attempts {
			t.Errorf("%s: expected %d attempts, got %d", policyString(c.policy), c.attempts, attempts)
		}
		switch {
		case c.wantError:
			if err == nil {
				t.Errorf("%s: want error, got no error", policyString(c.policy))
			}
			if s, ok := status.FromError(err); c.wantErrCode != 0 && (!ok || s.Code() != c.wantErrCode) {
				t.Errorf("%s: want error with code %v, got %v", policyString(c.policy), c.wantErrCode.String(), err)
			}
		case err != nil:
			t.Errorf("%s: want success, got error: %v", policyString(c.policy), err)
		}
	}
}

func TestWithPolicy_RetryBudgetExhaustedError(t *testing.T) {
	policy := ExponentialBackoff(time.Millisecond, time.Millisecond, 2)
	sr := alwaysRetry
	f := &failer{attempts: 5} // Will fail more times than allowed

	ctx := context.WithValue(context.Background(), TimeAfterContextKey, func(time.Duration) <-chan time.Time {
		c := make(chan time.Time)
		close(c)
		return c
	})

	err := WithPolicy(ctx, sr, policy, f.run)

	var budgetErr *ErrRetryBudgetExhausted
	if !errors.As(err, &budgetErr) {
		t.Fatalf("expected ErrRetryBudgetExhausted, got %T: %v", err, err)
	}

	if budgetErr.Attempts != 2 {
		t.Errorf("expected 2 attempts in error, got %d", budgetErr.Attempts)
	}

	expectedMsg := "retry budget exhausted (2 attempts): failing"
	if err.Error() != expectedMsg {
		t.Errorf("expected error message %q, got %q", expectedMsg, err.Error())
	}
}

func TestWithPolicy_RetryBudgetExhaustedGRPCStatus(t *testing.T) {
	policy := ExponentialBackoff(time.Millisecond, time.Millisecond, 1)
	sr := alwaysRetry
	grpcErr := status.Error(codes.Unavailable, "service unavailable")
	f := func() error { return grpcErr }

	ctx := context.WithValue(context.Background(), TimeAfterContextKey, func(time.Duration) <-chan time.Time {
		c := make(chan time.Time)
		close(c)
		return c
	})

	err := WithPolicy(ctx, sr, policy, f)

	s, ok := status.FromError(err)
	if !ok {
		t.Fatal("expected gRPC status error")
	}

	if s.Code() != codes.Unavailable {
		t.Errorf("expected code %v, got %v", codes.Unavailable, s.Code())
	}

	expectedMsg := "retry budget exhausted (1 attempts): service unavailable"
	if s.Message() != expectedMsg {
		t.Errorf("expected message %q, got %q", expectedMsg, s.Message())
	}
}

type mockNetTimeoutError struct{ error }

func (e mockNetTimeoutError) Timeout() bool   { return true }
func (e mockNetTimeoutError) Temporary() bool { return true }

var _ net.Error = mockNetTimeoutError{}

func TestErrRetryBudgetExhausted_GRPCStatus(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		wantCode codes.Code
	}{
		{
			name:     "wrapped gRPC status",
			err:      fmt.Errorf("wrapped: %w", status.Error(codes.AlreadyExists, "already exists")),
			wantCode: codes.AlreadyExists,
		},
		{
			name:     "context deadline exceeded",
			err:      context.DeadlineExceeded,
			wantCode: codes.DeadlineExceeded,
		},
		{
			name:     "context canceled",
			err:      context.Canceled,
			wantCode: codes.Canceled,
		},
		{
			name:     "os deadline exceeded",
			err:      os.ErrDeadlineExceeded,
			wantCode: codes.DeadlineExceeded,
		},
		{
			name:     "net timeout error",
			err:      mockNetTimeoutError{errors.New("timeout")},
			wantCode: codes.DeadlineExceeded,
		},
		{
			name:     "unknown error",
			err:      errors.New("unknown"),
			wantCode: codes.ResourceExhausted,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e := &ErrRetryBudgetExhausted{
				Attempts: 3,
				Err:      tc.err,
			}
			s := e.GRPCStatus()
			if s.Code() != tc.wantCode {
				t.Errorf("GRPCStatus().Code() = %v, want %v", s.Code(), tc.wantCode)
			}
		})
	}
}
