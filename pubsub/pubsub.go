// Package pubsub provides a lightweight publish/subscribe abstraction with
// in-memory and Redis backends.
//
// The package offers a unified interface for fan-out messaging with support
// for:
//   - In-memory broadcasting (MemoryPubSub)
//   - Redis-backed pub/sub (RedisPubSub)
//
// Features:
//   - Context-aware publish and subscribe operations
//   - Fan-out delivery to every subscriber of a topic
//   - Per-subscription buffered channels
//   - Graceful shutdown that closes every subscription
//
// Basic Usage:
//
//	ps := pubsub.NewMemory()
//	defer ps.Close()
//
//	sub, err := ps.Subscribe(ctx, "events")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer sub.Close()
//
//	if err := ps.Publish(ctx, "events", []byte("hello")); err != nil {
//	    log.Fatal(err)
//	}
//
//	msg := <-sub.Receive()
package pubsub

import (
	"context"
)

// Message is a single published payload together with the topic it was
// published to.
type Message struct {
	Topic string
	Data  []byte
}

// Subscription is a single subscriber's view of a topic. Receive returns the
// channel messages arrive on; the channel is closed when the subscription or
// its context ends.
type Subscription struct {
	id     string
	ch     <-chan Message
	ctx    context.Context
	cancel context.CancelFunc
}

// Receive returns the channel messages are delivered on. The channel is closed
// when the subscription is closed, the parent context is cancelled, or the
// pubsub instance is closed.
func (s *Subscription) Receive() <-chan Message {
	return s.ch
}

// Close ends the subscription and releases its resources.
func (s *Subscription) Close() {
	s.cancel()
}

// Subscriber subscribes to topics and receives messages published to them.
type Subscriber interface {
	// Subscribe subscribes to a topic and returns a channel for receiving messages.
	// The channel will be closed when the context is cancelled.
	Subscribe(ctx context.Context, topic string) (*Subscription, error)
}

// Publisher publishes messages to topics.
type Publisher interface {
	// Publish publishes a message to a topic.
	// All subscribers to the topic will receive the message (fan-out).
	Publish(ctx context.Context, topic string, data []byte) error
}

// PubSub is the combined publish and subscribe interface implemented by the
// backends of this package.
type PubSub interface {
	Publisher
	Subscriber
	// Close closes the pubsub instance and releases all resources.
	Close() error
}
