package retry

import (
	"context"
	"errors"
	"fmt"
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
