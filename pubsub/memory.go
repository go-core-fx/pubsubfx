package pubsub

import (
	"context"
	"errors"
	"sync"

	"github.com/google/uuid"
)

type MemoryPubSub struct {
	bufferSize uint

	wg        sync.WaitGroup
	mu        sync.RWMutex
	topics    map[string]map[string]*subscriber
	closeCh   chan struct{}
	closeOnce sync.Once
}

type subscriber struct {
	ch  chan Message
	ctx context.Context

	mu     sync.RWMutex
	closed bool
}

func NewMemory(opts ...Option) *MemoryPubSub {
	o := options{
		bufferSize: 0,
	}
	o.apply(opts...)

	return &MemoryPubSub{
		bufferSize: o.bufferSize,

		wg:        sync.WaitGroup{},
		mu:        sync.RWMutex{},
		topics:    make(map[string]map[string]*subscriber),
		closeCh:   make(chan struct{}),
		closeOnce: sync.Once{},
	}
}

// Publish sends a message to all subscribers of the given topic.
// This method blocks until all subscribers have received the message
// or until ctx is cancelled or the pubsub instance is closed.
// It returns ctx.Err() when cancelled before delivery completes and
// ErrPubSubClosed when the instance is closed first.
func (m *MemoryPubSub) Publish(ctx context.Context, topic string, data []byte) error {
	select {
	case <-m.closeCh:
		return ErrPubSubClosed
	default:
	}

	if topic == "" {
		return ErrInvalidTopic
	}

	m.mu.RLock()
	subscribers, exists := m.topics[topic]
	if !exists || len(subscribers) == 0 {
		m.mu.RUnlock()

		return nil
	}

	subs := make([]*subscriber, 0, len(subscribers))
	for _, sub := range subscribers {
		subs = append(subs, sub)
	}
	m.mu.RUnlock()

	wg := &sync.WaitGroup{}
	msg := Message{Topic: topic, Data: data}
	errs := make([]error, len(subs))

	for i, sub := range subs {
		wg.Add(1)
		go func(i int, sub *subscriber) {
			defer wg.Done()

			sub.mu.RLock()
			defer sub.mu.RUnlock()

			if sub.closed {
				select {
				case <-m.closeCh:
					errs[i] = ErrPubSubClosed
				default:
				}

				return
			}

			select {
			case sub.ch <- msg:
			case <-ctx.Done():
				errs[i] = ctx.Err()
			case <-m.closeCh:
				errs[i] = ErrPubSubClosed
			case <-sub.ctx.Done():
				select {
				case <-m.closeCh:
					errs[i] = ErrPubSubClosed
				default:
				}
			}
		}(i, sub)
	}

	wg.Wait()

	return errors.Join(errs...)
}

func (m *MemoryPubSub) Subscribe(ctx context.Context, topic string) (*Subscription, error) {
	select {
	case <-m.closeCh:
		return nil, ErrPubSubClosed
	default:
	}

	if topic == "" {
		return nil, ErrInvalidTopic
	}

	id := uuid.NewString()
	subCtx, cancel := context.WithCancel(ctx)
	ch := make(chan Message, m.bufferSize)
	sub := &subscriber{ch: ch, ctx: subCtx, mu: sync.RWMutex{}, closed: false}

	if !m.subscribe(id, topic, sub) {
		cancel()

		return nil, ErrPubSubClosed
	}

	go func() {
		defer m.wg.Done()

		select {
		case <-subCtx.Done():
		case <-m.closeCh:
		}

		cancel()
		m.unsubscribe(id, topic)

		sub.mu.Lock()
		defer sub.mu.Unlock()

		sub.closed = true
		close(sub.ch)
	}()

	return &Subscription{id: id, ctx: subCtx, cancel: cancel, ch: ch}, nil
}

func (m *MemoryPubSub) subscribe(id, topic string, sub *subscriber) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	select {
	case <-m.closeCh:
		return false
	default:
	}

	subscriptions, ok := m.topics[topic]
	if !ok {
		subscriptions = make(map[string]*subscriber)
		m.topics[topic] = subscriptions
	}
	subscriptions[id] = sub

	m.wg.Add(1)

	return true
}

func (m *MemoryPubSub) unsubscribe(id, topic string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	subscriptions, ok := m.topics[topic]
	if !ok {
		return
	}
	delete(subscriptions, id)
	if len(subscriptions) == 0 {
		delete(m.topics, topic)
	}
}

func (m *MemoryPubSub) Close() error {
	m.closeOnce.Do(func() {
		m.mu.Lock()
		close(m.closeCh)
		m.mu.Unlock()

		m.wg.Wait()
	})

	return nil
}

var _ PubSub = (*MemoryPubSub)(nil)
