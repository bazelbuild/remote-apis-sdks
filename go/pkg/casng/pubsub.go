package casng

import (
	"strings"
	"sync"
	"time"

	log "github.com/golang/glog"
	"github.com/pborman/uuid"
)

// pubsub provides a simple pubsub implementation to route messages and wait for them.
type pubsub struct {
	subs map[string]chan any
	mu   sync.RWMutex
	// A signalling channel that gets a message everytime the broker hits 0 subscriptions.
	// Unlike sync.WaitGroup, this allows the broker to accept more subs while a client is waiting for signal.
	done chan struct{}
}

// sub returns a routing tag and a channel to the subscriber to read messages from.
//
// Only messages associated with the returned tag are sent on the returned channel.
// This allows the subscriber to send a tagged message (request) that propagates across the system and eventually
// receive related messages (responses) from publishers on the returned channel.
//
// The subscriber must continue draining the returned channel until it's closed.
// The returned channel is unbuffered and closed only when unsub is called with the returned tag.
//
// To properly terminate the subscription, the subscriber must wait until all expected responses are received
// on the returned channel before unsubscribing.
// Once unsubscribed, any tagged messages for this subscription are dropped.
func (ps *pubsub) sub() (string, <-chan any) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	tag := uuid.New()
	subscriber := make(chan any)
	ps.subs[tag] = subscriber

	log.V(2).Infof("[casng] pubsub.sub; tag=%s", tag)
	return tag, subscriber
}

// unsub schedules the subscription to be removed as soon as in-flight pubs are done.
// The subscriber must continue draining the channel until it's closed.
// It is an error to publish more messages for tag after this call.
func (ps *pubsub) unsub(tag string) {
	log.V(2).Infof("[casng] pubsub.unsub.sched; tag=%s", tag)
	// If unsub is called from the same goroutine that is listening on the subscription
	// channel, a deadlock might occur.
	// pub would be holding a read lock while this call wants to hold a write lock that must
	// wait for all reads to finish. However, that read will never finish because the corresponding goroutine
	// is blocked on this call.
	// Ideally, the user should call unsub after confirming all pub calls have returned. However, this
	// relieves the user from that burden with minimal overhead.
	go func() {
		ps.mu.Lock()
		defer ps.mu.Unlock()
		subscriber, ok := ps.subs[tag]
		if !ok {
			return
		}
		delete(ps.subs, tag)
		close(subscriber)
		if len(ps.subs) == 0 {
			close(ps.done)
			ps.done = make(chan struct{})
		}
		log.V(2).Infof("[casng] pubsub.unsub.done; tag=%s", tag)
	}()
}

// pub is a blocking call that fans-out a response to all specified (by tag) subscribers concurrently.
//
// Returns when all active subscribers have received their copies or timed out.
// Inactive subscribers (expired by cancelling their context) are skipped (their copies are dropped).
//
// A busy subscriber does not block others from receiving their copies. It is instead
// rescheduled for another attempt once all others get a chance to receive.
// To prevent a temporarily infinite round-robin loop from consuming too much CPU, each subscriber
// gets at most 10ms to receive before getting rescheduled.
// Blocking 10ms for every subscriber amortizes much better than blocking 10ms for every
// iteration on the subscribers, even though both have the same worst-case cost.
// For example, if out of 10 subscribers 5 were busy for 1ms, the attempt will cost ~5ms instead of 10ms.
func (ps *pubsub) pub(m any, tags ...string) {
	_ = ps.pubN(m, len(tags), tags...)
}

// mpub (multi-publish) delivers the "once" message to a single subscriber then delivers the "rest" message to the rest of the subscribers.
// It's useful for cases where the message holds shared information that should not be duplicated among subscribers, such as stats.
func (ps *pubsub) mpub(once any, rest any, tags ...string) {
	t := ps.pubOnce(once, tags...)
	_ = ps.pubN(rest, len(tags)-1, excludeTag(tags, t)...)
}

// pubOnce is like pub, but delivers the message to a single subscriber.
// The tag of the subscriber that got the message is returned.
func (ps *pubsub) pubOnce(m any, tags ...string) string {
	received := ps.pubN(m, 1, tags...)
	if len(received) == 0 {
		return ""
	}
	return received[0]
}

// pubN is like pub, but delivers the message to no more than n subscribers. The tags of the subscribers that got the message are returned.
func (ps *pubsub) pubN(m any, n int, tags ...string) []string {
	if log.V(3) {
		startTime := time.Now()
		defer func() {
			log.Infof("[casng] pubsub.duration; start=%d, end=%d", startTime.UnixNano(), time.Now().UnixNano())
		}()
	}
	if len(tags) == 0 {
		log.Warning("[casng] pubsub.pub: called without tags, dropping message")
		log.V(4).Infof("[casng] pubsub.pub: called without tags for msg=%v", m)
		return nil
	}
	if n <= 0 {
		log.Warningf("[casng] pubsub.pub: nothing published because n=%d", n)
		return nil
	}

	ps.mu.RLock()
	defer ps.mu.RUnlock()

	log.V(4).Infof("[casng] pubsub.pub.msg; type=%[1]T, value=%[1]v", m)

	var toRetry []string
	var received []string
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	retryCount := 0
	for {
		for _, t := range tags {
			subscriber, ok := ps.subs[t]
			if !ok {
				log.Warningf("[casng] pubsub.pub.drop: tag=%s", t)
				continue
			}
			// Send now or reschedule if the subscriber is not ready.
			select {
			case subscriber <- m:
				log.V(3).Infof("[casng] pubsub.pub.sent: tag=%s", t)
				received = append(received, t)
				if len(received) >= n {
					return received
				}
			case <-ticker.C:
				toRetry = append(toRetry, t)
			}
		}
		if len(toRetry) == 0 {
			break
		}
		retryCount++
		log.V(3).Infof("[casng] pubsub.pub.retry; retry=%d, tag=%s", retryCount, strings.Join(toRetry, "|"))

		// Reuse the underlying arrays by swapping slices and resetting one of them.
		tags, toRetry = toRetry, tags
		toRetry = toRetry[:0]
	}
	return received
}

// wait blocks until all existing subscribers unsubscribe.
// The signal is a snapshot. The broker my get more subscribers after returning from this call.
func (ps *pubsub) wait() {
	ps.mu.RLock()
	done := ps.done
	ps.mu.RUnlock()
	<-done
}

// len returns the number of active subscribers.
func (ps *pubsub) len() int {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	return len(ps.subs)
}

// newPubSub initializes a new instance where subscribers must receive messages within timeout.
func newPubSub() *pubsub {
	return &pubsub{
		subs: make(map[string]chan any),
		done: make(chan struct{}),
	}
}

// excludeTag is used by mpub to filter out the tag that received the "once" message.
func excludeTag(tags []string, et string) []string {
	if len(tags) == 0 {
		return []string{}
	}
	// Remove by swapping the item with the last one and then excluding the last index.
	// This approach avoids allocating a new underlying array without losing any items
	// in the original unsorted array.
	i := -1
	for index, t := range tags {
		if t == et {
			i = index
			break
		}
	}
	if i < 0 {
		return tags
	}
	j := len(tags) - 1
	tags[i], tags[j] = tags[j], tags[i]
	return tags[:j]
}
