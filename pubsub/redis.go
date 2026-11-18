package pubsub

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// RedisConfig configures the Redis pubsub backend.
type RedisConfig struct {
	// Client is the Redis client to use.
	// If nil, a client is created from the URL.
	// If both Client and URL are provided, Client takes precedence.
	// Supplied clients must have ContextTimeoutEnabled set so command
	// I/O honours caller deadlines.
	Client *redis.Client

	// URL is the Redis URL to use.
	// If empty, the Redis client is not created.
	URL string

	// Prefix is the prefix to use for all topics.
	Prefix string
}

type RedisPubSub struct {
	prefix     string
	bufferSize uint

	client      *redis.Client
	ownedClient bool

	wg          sync.WaitGroup
	mu          sync.Mutex
	subscribers map[string]context.CancelFunc
	closeCh     chan struct{}
	closeOnce   sync.Once
	closeErr    error
}

func NewRedis(config RedisConfig, opts ...Option) (*RedisPubSub, error) {
	if config.Prefix != "" && !strings.HasSuffix(config.Prefix, ":") {
		config.Prefix += ":"
	}

	if config.Client == nil && config.URL == "" {
		return nil, fmt.Errorf("%w: no redis client or url provided", ErrInvalidConfig)
	}

	client := config.Client
	if client == nil {
		opt, err := redis.ParseURL(config.URL)
		if err != nil {
			return nil, fmt.Errorf("failed to parse redis url: %w", err)
		}

		opt.ContextTimeoutEnabled = true

		client = redis.NewClient(opt)
	}

	o := options{
		bufferSize: 0,
	}
	o.apply(opts...)

	return &RedisPubSub{
		prefix:     config.Prefix,
		bufferSize: o.bufferSize,

		client:      client,
		ownedClient: config.Client == nil,

		wg:          sync.WaitGroup{},
		mu:          sync.Mutex{},
		subscribers: make(map[string]context.CancelFunc),
		closeCh:     make(chan struct{}),
		closeOnce:   sync.Once{},
		closeErr:    nil,
	}, nil
}

func (r *RedisPubSub) Publish(ctx context.Context, topic string, data []byte) error {
	select {
	case <-r.closeCh:
		return ErrPubSubClosed
	default:
	}

	if topic == "" {
		return ErrInvalidTopic
	}

	if err := r.client.Publish(ctx, r.prefix+topic, data).Err(); err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	return nil
}

func (r *RedisPubSub) Subscribe(ctx context.Context, topic string) (*Subscription, error) {
	select {
	case <-r.closeCh:
		return nil, ErrPubSubClosed
	default:
	}

	if topic == "" {
		return nil, ErrInvalidTopic
	}

	// Track the in-flight setup under the lock Close latches shutdown on, so
	// Close waits for a pending handshake instead of returning while it is
	// still blocked. On the success path this accounting is handed over to
	// forward, which calls r.wg.Done during subscription cleanup.
	r.mu.Lock()
	select {
	case <-r.closeCh:
		r.mu.Unlock()
		return nil, ErrPubSubClosed
	default:
	}

	r.wg.Add(1)
	r.mu.Unlock()

	ps := r.client.Subscribe(ctx, r.prefix+topic)
	closePS, finishSetup := watchSetup(ctx, r.closeCh, ps)

	_, err := ps.Receive(ctx)
	finishSetup()

	if err != nil {
		closeErr := closePS()
		r.wg.Done()

		select {
		case <-r.closeCh:
			return nil, ErrPubSubClosed
		default:
		}

		return nil, errors.Join(fmt.Errorf("failed to subscribe: %w", err), closeErr)
	}

	id := uuid.NewString()
	subCtx, cancel := context.WithCancel(ctx)
	ch := make(chan Message, r.bufferSize)

	r.mu.Lock()
	select {
	case <-r.closeCh:
		r.mu.Unlock()
		cancel()
		_ = closePS()
		r.wg.Done()

		return nil, ErrPubSubClosed
	default:
	}

	r.subscribers[id] = cancel
	r.mu.Unlock()

	go r.forward(subCtx, ps, id, topic, ch, closePS)

	return &Subscription{id: id, ctx: subCtx, cancel: cancel, ch: ch}, nil
}

// watchSetup returns a closePS that closes ps exactly once and a finishSetup
// that marks setup complete. If ctx is cancelled or the instance is closed
// before finishSetup runs, watchSetup closes ps so a stalled subscription
// handshake unblocks.
func watchSetup(ctx context.Context, closeCh <-chan struct{}, ps *redis.PubSub) (func() error, func()) {
	var (
		mu        sync.Mutex
		setupDone bool
		closeErr  error
		closeOnce sync.Once
	)

	closePS := func() error {
		closeOnce.Do(func() {
			if err := ps.Close(); err != nil {
				closeErr = fmt.Errorf("failed to close subscription: %w", err)
			}
		})

		return closeErr
	}

	setupCh := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
		case <-closeCh:
		case <-setupCh:
			return
		}

		mu.Lock()
		defer mu.Unlock()

		if !setupDone {
			_ = closePS()
		}
	}()

	finishSetup := func() {
		mu.Lock()
		setupDone = true
		mu.Unlock()
		close(setupCh)
	}

	return closePS, finishSetup
}

func (r *RedisPubSub) forward(
	subCtx context.Context,
	ps *redis.PubSub,
	id, topic string,
	ch chan Message,
	closePS func() error,
) {
	defer func() {
		_ = closePS()
		close(ch)

		r.mu.Lock()
		delete(r.subscribers, id)
		r.mu.Unlock()

		r.wg.Done()
	}()

	for {
		select {
		case <-r.closeCh:
			return
		case <-subCtx.Done():
			return
		case msg, ok := <-ps.Channel():
			if !ok {
				return
			}
			if msg == nil {
				continue
			}

			select {
			case ch <- Message{
				Topic: topic,
				Data:  []byte(msg.Payload),
			}:
			case <-r.closeCh:
				return
			case <-subCtx.Done():
				return
			}
		}
	}
}

func (r *RedisPubSub) Close() error {
	r.closeOnce.Do(func() {
		r.mu.Lock()
		close(r.closeCh)
		r.mu.Unlock()

		r.wg.Wait()

		if r.ownedClient {
			if err := r.client.Close(); err != nil {
				r.closeErr = fmt.Errorf("failed to close redis client: %w", err)
			}
		}
	})

	return r.closeErr
}

var _ PubSub = (*RedisPubSub)(nil)
