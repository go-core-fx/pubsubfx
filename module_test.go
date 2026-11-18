package pubsubfx_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-core-fx/pubsubfx"
	"github.com/go-core-fx/pubsubfx/pubsub"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

// TestModuleProvidesPubSubAndClosesOnStop boots the module the way an app
// wires it, checks a publish/subscribe round trip resolves through the
// container, and verifies Stop runs the close hook.
func TestModuleProvidesPubSubAndClosesOnStop(t *testing.T) {
	t.Parallel()

	var ps pubsub.PubSub

	app := fx.New(
		fx.Supply(zap.NewNop()),
		fx.Supply(pubsubfx.Config{URL: "memory://", BufferSize: 4}),
		pubsubfx.Module(),
		fx.Invoke(func(p pubsub.PubSub) {
			ps = p
		}),
	)
	if app.Err() != nil {
		t.Fatalf("fx.New failed: %v", app.Err())
	}

	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()

	if err := app.Start(ctx); err != nil {
		t.Fatalf("start failed: %v", err)
	}

	sub, err := ps.Subscribe(ctx, "topic")
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	if pubErr := ps.Publish(ctx, "topic", []byte("hello")); pubErr != nil {
		t.Fatalf("Publish failed: %v", pubErr)
	}

	select {
	case msg, ok := <-sub.Receive():
		if !ok {
			t.Fatal("subscription channel closed before a message arrived")
		}

		if string(msg.Data) != "hello" {
			t.Errorf("Expected data %q, got %q", "hello", msg.Data)
		}
	case <-time.After(waitTimeout):
		t.Fatal("timed out waiting for a message")
	}

	if stopErr := app.Stop(ctx); stopErr != nil {
		t.Fatalf("stop failed: %v", stopErr)
	}

	// The OnStop hook must have closed the instance.
	if pubErr := ps.Publish(context.Background(), "topic", []byte("x")); !errors.Is(pubErr, pubsub.ErrPubSubClosed) {
		t.Errorf("Expected ErrPubSubClosed after Stop, got %v", pubErr)
	}
}

// TestModuleFailsFastOnInvalidScheme checks that an unusable Config fails at
// startup instead of at first publish.
func TestModuleFailsFastOnInvalidScheme(t *testing.T) {
	t.Parallel()

	app := fx.New(
		fx.Supply(zap.NewNop()),
		fx.Supply(pubsubfx.Config{URL: "nats://broker"}),
		pubsubfx.Module(),
	)

	if !errors.Is(app.Err(), pubsubfx.ErrInvalidConfig) {
		t.Fatalf("expected ErrInvalidConfig, got %v", app.Err())
	}
}
