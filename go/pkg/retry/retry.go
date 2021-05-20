// Package retry implements retry logic helpers, which can be used to wrap operations that can
// intermittently fail, but can be retried at a higher level.
//
// Consider replacing with https://github.com/eapache/go-resiliency/tree/master/retrier. This would
// have slightly different values for backoff consts.
package retry

import (
	"context"
	stderrors "errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	log "github.com/golang/glog"
	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	backoffFactor = 1.3 // backoff increases by this factor on each retry
	backoffRange  = 0.4 // backoff is randomized downwards by this factor
)

// BackoffPolicy describes how to back off when retrying, and how many times to retry.
type BackoffPolicy struct {
	baseDelay, maxDelay time.Duration
	maxAttempts         Attempts // 0 means unlimited
}

// ExponentialBackoff returns an exponential backoff implementation.
//
// Starting from baseDelay, it will delay by an additional fixed multiple with each retry, never
// delaying by more than maxDelay.  For example, ExponentialBackoff(time.Second, time.Hour, 5) will
// produce delays of roughly: 1s, 1.5s, 2s, 3s, 4s.
//
// Note that delays are randomized, so the exact values are not guaranteed. attempts=0 means
// unlimited attempts. See UnlimitedAttempts.
func ExponentialBackoff(baseDelay, maxDelay time.Duration, attempts Attempts) BackoffPolicy {
	return BackoffPolicy{baseDelay, maxDelay, attempts}
}

// Immediately returns a retrier that retries right away.
func Immediately(attempts Attempts) BackoffPolicy {
	return BackoffPolicy{0, 0, attempts}
}

// Attempts is the number of times to attempt something before giving up. A value of 0 represents
// an effectively unlimited number of attempts, or you can use the equivalent
// retry.UnlimitedAttempts.
type Attempts uint

// UnlimitedAttempts is used to specify no limit to the number of attempts.
const UnlimitedAttempts = Attempts(0)

// ShouldRetry encapsulates the decision of whether an error is retry-able. If an error should not
// be retried, the function must return false.
type ShouldRetry func(error) bool

// Always always retries, regardless of error.
func Always(error) bool { return true }

// TransientOnly returns true if the error is transient.
// It implements ShouldRetry type.
func TransientOnly(err error) bool {
	// Retry RPC timeouts. Note that we do *not* retry context cancellations (context.Cancelled);
	// if the user wants to back out of the call we should let them.
	if stderrors.Is(err, context.DeadlineExceeded) {
		return true
	}
	s, ok := status.FromError(err)
	if !ok {
		return false
	}
	switch s.Code() {
	case codes.Canceled, codes.Unknown, codes.DeadlineExceeded, codes.Aborted,
		codes.Internal, codes.Unavailable, codes.ResourceExhausted:
		return true
	default:
		return false
	}
}

// WithPolicy retries f until either it succeeds, or shouldRetry returns false, or the number of
// retries is capped by the backoff policy. Returns the error returned by the final attempt. It
// annotates the error message in case the retry budget is exhausted.
func WithPolicy(ctx context.Context, shouldRetry ShouldRetry, bp BackoffPolicy, f func() error) error {
	timeAfter, ok := ctx.Value(TimeAfterContextKey).(func(time.Duration) <-chan time.Time)
	if !ok {
		timeAfter = time.After
	}

	for attempts := 0; ; attempts++ {
		err := f()
		if err == nil || !shouldRetry(err) {
			return err
		}

		if log.V(1) {
			// This log depth is custom-tailored to the SDK usage, which always calls the retrier from within client.CallWithTimeout.
			log.InfoDepth(3, fmt.Sprintf("call failed with err=%v, retrying.", err))
		}

		if attempts+1 == int(bp.maxAttempts) {
			// Annotates the error message to indicate the retry budget was exhausted.
			//
			// This is a little hacky, but generic status annotation preserving status code doesn't exist
			// in gRPC's status library yet, and it's overkill to implement it here for just this.
			if s, ok := status.FromError(err); ok {
				spb := s.Proto()
				spb.Message = fmt.Sprintf("retry budget exhausted (%d attempts): ", bp.maxAttempts) + spb.Message
				return status.ErrorProto(spb)
			}
			return errors.Wrapf(err, "retry budget exhausted (%d attempts)", bp.maxAttempts)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timeAfter(backoff(bp.baseDelay, bp.maxDelay, attempts)):

		}
	}
}

type timeAfterContextKey struct{}

// TimeAfterContextKey is to be used as a key in the context to provide a value that is compatible
// with time.After. The main purpose is to mock out time.After in the tests.
var TimeAfterContextKey = timeAfterContextKey{}

var (
	mu  sync.Mutex
	rng = rand.New(rand.NewSource(time.Now().UnixNano()))
)

// randFloat64 is equivalent to calling rng.Float64, but safe for concurrent use.
func randFloat64() float64 {
	mu.Lock()
	f := rng.Float64()
	mu.Unlock()
	return f
}

// backoff returns a random value in [0, maxDelay] that increases exponentially with value of
// retries, starting from baseDelay. Set retries to 0 for the first call and increment with each
// subsequent call.
func backoff(baseDelay, maxDelay time.Duration, retries int) time.Duration {
	backoff, max := float64(baseDelay), float64(maxDelay)
	for backoff < max && retries > 0 {
		backoff = backoff * backoffFactor
		retries--
	}
	if backoff > max {
		backoff = max
	}

	// Randomize backoff delays so that if a cluster of requests start at the same time, they won't
	// operate in lockstep. We just subtract up to 40% so that we obey maxDelay.
	backoff -= backoff * backoffRange * randFloat64()
	if backoff < 0 {
		return 0
	}
	return time.Duration(backoff)
}
