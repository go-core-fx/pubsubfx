package pubsubfx_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-core-fx/pubsubfx"
	"github.com/go-core-fx/pubsubfx/pubsub"
)

const waitTimeout = 5 * time.Second

func TestNewDefaultsToMemoryBackend(t *testing.T) {
	t.Parallel()

	ps, err := pubsubfx.New(pubsubfx.Config{})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	if _, ok := ps.(*pubsub.MemoryPubSub); !ok {
		t.Errorf("Expected *pubsub.MemoryPubSub, got %T", ps)
	}

	if closeErr := ps.Close(); closeErr != nil {
		t.Errorf("Close failed: %v", closeErr)
	}
}

func TestNewSelectsMemoryBackend(t *testing.T) {
	t.Parallel()

	ps, err := pubsubfx.New(pubsubfx.Config{URL: "memory://"})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	if _, ok := ps.(*pubsub.MemoryPubSub); !ok {
		t.Errorf("Expected *pubsub.MemoryPubSub, got %T", ps)
	}

	if closeErr := ps.Close(); closeErr != nil {
		t.Errorf("Close failed: %v", closeErr)
	}
}

func TestNewSelectsRedisBackendWithoutConnecting(t *testing.T) {
	t.Parallel()

	// New only parses the URL; nothing connects until an operation runs.
	ps, err := pubsubfx.New(pubsubfx.Config{URL: "redis://127.0.0.1:6379/0"})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	if _, ok := ps.(*pubsub.RedisPubSub); !ok {
		t.Errorf("Expected *pubsub.RedisPubSub, got %T", ps)
	}

	if closeErr := ps.Close(); closeErr != nil {
		t.Errorf("Close failed: %v", closeErr)
	}
}

func TestNewRejectsUnknownScheme(t *testing.T) {
	t.Parallel()

	_, err := pubsubfx.New(pubsubfx.Config{URL: "nats://broker"})
	if !errors.Is(err, pubsubfx.ErrInvalidConfig) {
		t.Errorf("Expected ErrInvalidConfig, got %v", err)
	}
}

func TestNewRejectsMalformedURL(t *testing.T) {
	t.Parallel()

	_, err := pubsubfx.New(pubsubfx.Config{URL: "://missing-scheme"})
	if !errors.Is(err, pubsubfx.ErrInvalidConfig) {
		t.Errorf("Expected ErrInvalidConfig, got %v", err)
	}
}

// TestNewRejectsBackendRejectedURL covers a URL the general parser accepts but
// the redis backend rejects, so the documented ErrInvalidConfig contract holds
// for backend rejections too, not just scheme and parse failures.
func TestNewRejectsBackendRejectedURL(t *testing.T) {
	t.Parallel()

	_, err := pubsubfx.New(pubsubfx.Config{URL: "redis://localhost:6379/not-a-db"})
	if !errors.Is(err, pubsubfx.ErrInvalidConfig) {
		t.Errorf("Expected ErrInvalidConfig, got %v", err)
	}
}

func TestNewAppliesBufferSize(t *testing.T) {
	t.Parallel()

	ps, err := pubsubfx.New(pubsubfx.Config{BufferSize: 4})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	t.Cleanup(func() {
		if closeErr := ps.Close(); closeErr != nil {
			t.Errorf("Close failed: %v", closeErr)
		}
	})

	ctx := context.Background()

	if _, subErr := ps.Subscribe(ctx, "topic"); subErr != nil {
		t.Fatalf("Subscribe failed: %v", subErr)
	}

	// No reader is active: the publish must fit into the subscription buffer.
	errCh := make(chan error, 1)
	go func() {
		errCh <- ps.Publish(ctx, "topic", []byte("buffered"))
	}()

	select {
	case pubErr := <-errCh:
		if pubErr != nil {
			t.Errorf("Publish failed: %v", pubErr)
		}
	case <-time.After(waitTimeout):
		t.Fatal("Publish blocked: BufferSize was not applied to the subscription")
	}
}
