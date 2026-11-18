package pubsub_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-core-fx/pubsubfx/pubsub"
)

const (
	waitTimeout   = 5 * time.Second
	noMessageWait = 150 * time.Millisecond
)

func newMemory(t *testing.T, opts ...pubsub.Option) *pubsub.MemoryPubSub {
	t.Helper()

	ps := pubsub.NewMemory(opts...)
	t.Cleanup(func() {
		if err := ps.Close(); err != nil {
			t.Errorf("Close failed: %v", err)
		}
	})

	return ps
}

// receiveOne returns the next message or fails the test if none arrives
// before waitTimeout.
func receiveOne(t *testing.T, sub *pubsub.Subscription) pubsub.Message {
	t.Helper()

	select {
	case msg, ok := <-sub.Receive():
		if !ok {
			t.Fatal("subscription channel closed before a message arrived")
		}

		return msg
	case <-time.After(waitTimeout):
		t.Fatal("timed out waiting for a message")

		return pubsub.Message{}
	}
}

// publishAsync starts a publish in the background and returns a channel that
// carries its result.
func publishAsync(ctx context.Context, ps pubsub.Publisher, topic string, data []byte) <-chan error {
	errCh := make(chan error, 1)

	go func() {
		errCh <- ps.Publish(ctx, topic, data)
	}()

	return errCh
}

// waitPublish waits for a background publish to finish and fails the test if
// it does not complete in time.
func waitPublish(t *testing.T, errCh <-chan error) {
	t.Helper()

	select {
	case err := <-errCh:
		if err != nil {
			t.Errorf("Publish failed: %v", err)
		}
	case <-time.After(waitTimeout):
		t.Fatal("timed out waiting for Publish to complete")
	}
}

func TestMemoryPublishReachesSubscriber(t *testing.T) {
	t.Parallel()

	ps := newMemory(t)
	ctx := context.Background()

	sub, err := ps.Subscribe(ctx, "topic")
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	errCh := publishAsync(ctx, ps, "topic", []byte("hello"))
	msg := receiveOne(t, sub)
	waitPublish(t, errCh)

	if msg.Topic != "topic" {
		t.Errorf("Expected topic %q, got %q", "topic", msg.Topic)
	}

	if string(msg.Data) != "hello" {
		t.Errorf("Expected data %q, got %q", "hello", msg.Data)
	}
}

func TestMemoryPublishFansOutToAllSubscribers(t *testing.T) {
	t.Parallel()

	ps := newMemory(t, pubsub.WithBufferSize(4))
	ctx := context.Background()

	subA, err := ps.Subscribe(ctx, "topic")
	if err != nil {
		t.Fatalf("Subscribe A failed: %v", err)
	}

	subB, err := ps.Subscribe(ctx, "topic")
	if err != nil {
		t.Fatalf("Subscribe B failed: %v", err)
	}

	other, err := ps.Subscribe(ctx, "other")
	if err != nil {
		t.Fatalf("Subscribe other failed: %v", err)
	}

	if pubErr := ps.Publish(ctx, "topic", []byte("payload")); pubErr != nil {
		t.Fatalf("Publish failed: %v", pubErr)
	}

	if msg := receiveOne(t, subA); string(msg.Data) != "payload" {
		t.Errorf("Subscriber A: expected %q, got %q", "payload", msg.Data)
	}

	if msg := receiveOne(t, subB); string(msg.Data) != "payload" {
		t.Errorf("Subscriber B: expected %q, got %q", "payload", msg.Data)
	}

	select {
	case msg := <-other.Receive():
		t.Errorf("Subscriber of %q must not receive, got %q", "other", msg.Topic)
	case <-time.After(noMessageWait):
	}
}

func TestMemoryPublishWithoutSubscribers(t *testing.T) {
	t.Parallel()

	ps := newMemory(t)

	if err := ps.Publish(context.Background(), "topic", []byte("x")); err != nil {
		t.Errorf("Publish without subscribers must succeed, got %v", err)
	}
}

func TestMemoryRejectsEmptyTopic(t *testing.T) {
	t.Parallel()

	ps := newMemory(t)
	ctx := context.Background()

	if err := ps.Publish(ctx, "", []byte("x")); !errors.Is(err, pubsub.ErrInvalidTopic) {
		t.Errorf("Publish empty topic: expected ErrInvalidTopic, got %v", err)
	}

	if _, err := ps.Subscribe(ctx, ""); !errors.Is(err, pubsub.ErrInvalidTopic) {
		t.Errorf("Subscribe empty topic: expected ErrInvalidTopic, got %v", err)
	}
}

func TestMemoryClosedInstanceRejectsOperations(t *testing.T) {
	t.Parallel()

	ps := pubsub.NewMemory()

	if closeErr := ps.Close(); closeErr != nil {
		t.Fatalf("Close failed: %v", closeErr)
	}

	ctx := context.Background()

	if err := ps.Publish(ctx, "topic", []byte("x")); !errors.Is(err, pubsub.ErrPubSubClosed) {
		t.Errorf("Publish on closed instance: expected ErrPubSubClosed, got %v", err)
	}

	if _, err := ps.Subscribe(ctx, "topic"); !errors.Is(err, pubsub.ErrPubSubClosed) {
		t.Errorf("Subscribe on closed instance: expected ErrPubSubClosed, got %v", err)
	}
}

func TestMemoryCloseIsIdempotent(t *testing.T) {
	t.Parallel()

	ps := pubsub.NewMemory()

	if err := ps.Close(); err != nil {
		t.Errorf("First Close failed: %v", err)
	}

	if err := ps.Close(); err != nil {
		t.Errorf("Second Close failed: %v", err)
	}
}

func TestMemoryCloseClosesSubscriptionChannels(t *testing.T) {
	t.Parallel()

	ps := newMemory(t)
	ctx := context.Background()

	sub, err := ps.Subscribe(ctx, "topic")
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	if closeErr := ps.Close(); closeErr != nil {
		t.Fatalf("Close failed: %v", closeErr)
	}

	select {
	case _, ok := <-sub.Receive():
		if ok {
			t.Error("Expected the subscription channel to close")
		}
	case <-time.After(waitTimeout):
		t.Fatal("Timed out waiting for the subscription channel to close")
	}
}

func TestMemorySubscriptionCloseEndsDelivery(t *testing.T) {
	t.Parallel()

	ps := newMemory(t, pubsub.WithBufferSize(4))
	ctx := context.Background()

	sub, err := ps.Subscribe(ctx, "topic")
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	sub.Close()

	select {
	case _, ok := <-sub.Receive():
		if ok {
			t.Error("Expected the subscription channel to close after Subscription.Close")
		}
	case <-time.After(waitTimeout):
		t.Fatal("Timed out waiting for the subscription channel to close")
	}

	if pubErr := ps.Publish(ctx, "topic", []byte("x")); pubErr != nil {
		t.Errorf("Publish after unsubscribe failed: %v", pubErr)
	}
}

func TestMemoryParentCancellationEndsSubscription(t *testing.T) {
	t.Parallel()

	ps := newMemory(t, pubsub.WithBufferSize(4))
	ctx, cancel := context.WithCancel(context.Background())

	sub, err := ps.Subscribe(ctx, "topic")
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	cancel()

	select {
	case _, ok := <-sub.Receive():
		if ok {
			t.Error("Expected the subscription channel to close after context cancellation")
		}
	case <-time.After(waitTimeout):
		t.Fatal("Timed out waiting for the subscription channel to close")
	}
}

func TestMemoryPublishWithBufferedChannelDoesNotBlock(t *testing.T) {
	t.Parallel()

	ps := newMemory(t, pubsub.WithBufferSize(4))
	ctx := context.Background()

	if _, err := ps.Subscribe(ctx, "topic"); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	// No reader is active: the publish must fit into the subscription buffer.
	waitPublish(t, publishAsync(ctx, ps, "topic", []byte("buffered")))
}

func TestMemoryPublishHonoursCancelledContext(t *testing.T) {
	t.Parallel()

	ps := newMemory(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := ps.Subscribe(context.Background(), "topic"); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	// The subscriber never reads, so only the cancelled context lets Publish
	// return instead of blocking on the unbuffered channel.
	err := ps.Publish(ctx, "topic", []byte("dropped"))
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Publish with cancelled context: expected context.Canceled, got %v", err)
	}
}

func TestMemoryPublishReturnsClosedOnShutdown(t *testing.T) {
	t.Parallel()

	ps := pubsub.NewMemory()
	ctx := context.Background()

	if _, err := ps.Subscribe(ctx, "topic"); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	errCh := publishAsync(ctx, ps, "topic", []byte("blocked"))

	if closeErr := ps.Close(); closeErr != nil {
		t.Fatalf("Close failed: %v", closeErr)
	}

	select {
	case err := <-errCh:
		if !errors.Is(err, pubsub.ErrPubSubClosed) {
			t.Errorf("Publish interrupted by Close: expected ErrPubSubClosed, got %v", err)
		}
	case <-time.After(waitTimeout):
		t.Fatal("timed out waiting for Publish interrupted by Close")
	}
}
