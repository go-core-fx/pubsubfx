package pubsub_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/go-core-fx/pubsubfx/pubsub"
)

const (
	churnSubscribers = 4
	churnIterations  = 50
)

// TestMemoryConcurrentFanOut publishes from many goroutines while every
// subscriber drains concurrently, so the race detector watches the topic map,
// the per-subscriber channels and the close path at once.
func TestMemoryConcurrentFanOut(t *testing.T) {
	t.Parallel()

	const (
		subscribers    = 8
		publishers     = 8
		messagesPerPub = 50
	)

	ps := pubsub.NewMemory()
	t.Cleanup(func() {
		if err := ps.Close(); err != nil {
			t.Errorf("Close failed: %v", err)
		}
	})

	ctx := context.Background()
	totalPerSubscriber := int64(publishers * messagesPerPub)

	subs := make([]*pubsub.Subscription, 0, subscribers)
	counts := make([]atomic.Int64, subscribers)
	drains := sync.WaitGroup{}

	for i := range subscribers {
		sub, err := ps.Subscribe(ctx, "topic")
		if err != nil {
			t.Fatalf("Subscribe failed: %v", err)
		}

		subs = append(subs, sub)

		drains.Go(func() {
			for range sub.Receive() {
				counts[i].Add(1)
			}
		})
	}

	pubs := sync.WaitGroup{}
	for range publishers {
		pubs.Go(func() {
			for range messagesPerPub {
				if err := ps.Publish(ctx, "topic", []byte("x")); err != nil {
					t.Errorf("Publish failed: %v", err)

					return
				}
			}
		})
	}

	pubs.Wait()

	for _, sub := range subs {
		sub.Close()
	}

	drains.Wait()

	for i := range subscribers {
		if got := counts[i].Load(); got != totalPerSubscriber {
			t.Errorf("Subscriber %d: expected %d messages, got %d", i, totalPerSubscriber, got)
		}
	}
}

// TestMemorySubscribeCloseChurn hammers subscribe/close against concurrent
// publishes to exercise unsubscribe bookkeeping and channel closure.
func TestMemorySubscribeCloseChurn(t *testing.T) {
	t.Parallel()

	const (
		publishers = 4
		messages   = 100
	)

	ps := newMemory(t, pubsub.WithBufferSize(8))
	ctx := context.Background()

	cycles := sync.WaitGroup{}
	for range churnSubscribers {
		cycles.Go(func() {
			for range churnIterations {
				sub, err := ps.Subscribe(ctx, "topic")
				if err != nil {
					t.Errorf("Subscribe failed: %v", err)

					return
				}

				sub.Close()
			}
		})
	}

	pubs := sync.WaitGroup{}
	for range publishers {
		pubs.Go(func() {
			for range messages {
				if err := ps.Publish(ctx, "topic", []byte("x")); err != nil {
					t.Errorf("Publish failed: %v", err)

					return
				}
			}
		})
	}

	pubs.Wait()
	cycles.Wait()
}

// TestMemoryConcurrentClose closes the same instance from many goroutines at
// once: the shutdown transition must be serialized so closeCh is closed once
// and every caller waits for subscription cleanup.
func TestMemoryConcurrentClose(t *testing.T) {
	t.Parallel()

	const closers = 16

	ps := pubsub.NewMemory()

	if _, err := ps.Subscribe(context.Background(), "topic"); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	errs := make(chan error, closers)
	starts := sync.WaitGroup{}
	for range closers {
		starts.Go(func() {
			errs <- ps.Close()
		})
	}

	starts.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Errorf("Close failed: %v", err)
		}
	}
}

// TestMemorySubscribeCloseRace runs Subscribe against Close: admission must
// either succeed fully or be rejected with ErrPubSubClosed, and a successfully
// admitted subscription must be cleaned up by the time Close returns.
func TestMemorySubscribeCloseRace(t *testing.T) {
	t.Parallel()

	const cycles = 50

	for range cycles {
		ps := pubsub.NewMemory()
		ctx := context.Background()

		var (
			sub    *pubsub.Subscription
			subErr error
		)

		runs := sync.WaitGroup{}
		runs.Go(func() {
			sub, subErr = ps.Subscribe(ctx, "topic")
		})
		runs.Go(func() {
			if closeErr := ps.Close(); closeErr != nil {
				t.Errorf("Close failed: %v", closeErr)
			}
		})

		runs.Wait()

		if subErr != nil {
			if !errors.Is(subErr, pubsub.ErrPubSubClosed) {
				t.Errorf("Subscribe during Close: expected ErrPubSubClosed, got %v", subErr)
			}
		} else {
			select {
			case _, ok := <-sub.Receive():
				if ok {
					t.Error("Expected the admitted subscription channel to be closed after Close returned")
				}
			default:
				t.Error("Expected the admitted subscription channel to be closed after Close returned")
			}
		}

		if _, err := ps.Subscribe(ctx, "topic"); !errors.Is(err, pubsub.ErrPubSubClosed) {
			t.Errorf("Subscribe after Close: expected ErrPubSubClosed, got %v", err)
		}
	}
}
