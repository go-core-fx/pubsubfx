package pubsub_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-core-fx/pubsubfx/pubsub"
	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

var (
	redisOnce      sync.Once
	redisContainer testcontainers.Container
	redisURL       string
	errRedis       error
)

// TestMain terminates the shared Redis testcontainer after the test run, if one
// was started. Runs that never reach a Redis test leave no container behind.
func TestMain(m *testing.M) {
	code := m.Run()

	if redisContainer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if err := redisContainer.Terminate(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "failed to terminate redis testcontainer: %v\n", err)
		}
		cancel()
	}

	os.Exit(code)
}

// redisTestURL starts a shared Redis testcontainer on first use and returns its
// connection URL. It skips the calling test when Docker is unavailable, but fails
// it in CI so a missing container can never hide behind a green run.
func redisTestURL(t *testing.T) string {
	t.Helper()

	redisOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()

		container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Image:        "redis:8-alpine",
				ExposedPorts: []string{"6379/tcp"},
				WaitingFor:   wait.ForListeningPort("6379/tcp").WithStartupTimeout(30 * time.Second),
			},
			Started: true,
		})
		if err != nil {
			errRedis = fmt.Errorf("failed to start redis testcontainer: %w", err)
			return
		}

		redisContainer = container

		endpoint, err := container.Endpoint(ctx, "tcp")
		if err != nil {
			errRedis = fmt.Errorf("failed to get redis testcontainer endpoint: %w", err)
			return
		}

		redisURL = "redis://" + strings.TrimPrefix(endpoint, "tcp://")
	})

	if errRedis != nil {
		if os.Getenv("CI") != "" {
			t.Fatalf("redis testcontainer unavailable: %v", errRedis)
		}

		t.Skipf("redis testcontainer unavailable: %v", errRedis)
	}

	return redisURL
}

// newRedisPubSub builds a pubsub bound to the shared testcontainer, isolated per
// test through the test name as topic prefix, and closes it on cleanup.
func newRedisPubSub(t *testing.T) *pubsub.RedisPubSub {
	t.Helper()

	ps, err := pubsub.NewRedis(pubsub.RedisConfig{
		Client: nil,
		URL:    redisTestURL(t),
		Prefix: t.Name(),
	})
	if err != nil {
		t.Fatalf("NewRedis failed: %v", err)
	}

	t.Cleanup(func() {
		if closeErr := ps.Close(); closeErr != nil {
			t.Errorf("Close failed: %v", closeErr)
		}
	})

	return ps
}

func TestRedisPublishSubscribeRoundTrip(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ps := newRedisPubSub(t)

	sub, err := ps.Subscribe(ctx, "events")
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	if pubErr := ps.Publish(ctx, "events", []byte("hello")); pubErr != nil {
		t.Fatalf("Publish failed: %v", pubErr)
	}

	msg := receiveOne(t, sub)

	if msg.Topic != "events" {
		t.Errorf("Expected topic %q, got %q", "events", msg.Topic)
	}

	if string(msg.Data) != "hello" {
		t.Errorf("Expected data %q, got %q", "hello", msg.Data)
	}
}

func TestRedisPrefixIsolatesTopics(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	psA := newRedisPubSub(t)

	psB, err := pubsub.NewRedis(pubsub.RedisConfig{
		Client: nil,
		URL:    redisTestURL(t),
		Prefix: t.Name() + ":other",
	})
	if err != nil {
		t.Fatalf("NewRedis failed: %v", err)
	}

	t.Cleanup(func() {
		if closeErr := psB.Close(); closeErr != nil {
			t.Errorf("Close failed: %v", closeErr)
		}
	})

	sub, err := psA.Subscribe(ctx, "shared")
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	// A message published under the other prefix must not surface first; if
	// prefixes leaked, receiveOne would return it instead of our own payload.
	if pubErr := psB.Publish(ctx, "shared", []byte("foreign")); pubErr != nil {
		t.Fatalf("Publish failed: %v", pubErr)
	}

	select {
	case msg := <-sub.Receive():
		t.Fatalf("Foreign-prefixed message leaked, got %q", msg.Data)
	case <-time.After(noMessageWait):
	}

	if pubErr := psA.Publish(ctx, "shared", []byte("own")); pubErr != nil {
		t.Fatalf("Publish failed: %v", pubErr)
	}

	if msg := receiveOne(t, sub); string(msg.Data) != "own" {
		t.Errorf("Expected data %q, got %q", "own", msg.Data)
	}
}

func TestRedisClosedInstanceRejectsOperations(t *testing.T) {
	t.Parallel()

	ps := newRedisPubSub(t)

	if err := ps.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	ctx := context.Background()

	if err := ps.Publish(ctx, "topic", []byte("x")); !errors.Is(err, pubsub.ErrPubSubClosed) {
		t.Errorf("Publish on closed instance: expected ErrPubSubClosed, got %v", err)
	}

	if _, err := ps.Subscribe(ctx, "topic"); !errors.Is(err, pubsub.ErrPubSubClosed) {
		t.Errorf("Subscribe on closed instance: expected ErrPubSubClosed, got %v", err)
	}
}

func TestRedisSubscriptionCloseClosesChannel(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ps := newRedisPubSub(t)

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
}

func TestRedisRejectsEmptyTopic(t *testing.T) {
	t.Parallel()

	ps := newRedisPubSub(t)
	ctx := context.Background()

	if err := ps.Publish(ctx, "", []byte("x")); !errors.Is(err, pubsub.ErrInvalidTopic) {
		t.Errorf("Publish empty topic: expected ErrInvalidTopic, got %v", err)
	}

	if _, err := ps.Subscribe(ctx, ""); !errors.Is(err, pubsub.ErrInvalidTopic) {
		t.Errorf("Subscribe empty topic: expected ErrInvalidTopic, got %v", err)
	}
}

func TestRedisNewRequiresClientOrURL(t *testing.T) {
	t.Parallel()

	if _, err := pubsub.NewRedis(pubsub.RedisConfig{}); !errors.Is(err, pubsub.ErrInvalidConfig) {
		t.Errorf("Expected ErrInvalidConfig, got %v", err)
	}
}

func TestRedisNewRejectsUnknownURLScheme(t *testing.T) {
	t.Parallel()

	if _, err := pubsub.NewRedis(pubsub.RedisConfig{URL: "foo://localhost:6379"}); err == nil {
		t.Error("Expected an error for an unknown redis url scheme")
	}
}

// TestRedisConcurrentClose closes the same instance from many goroutines at
// once: the shutdown transition must be serialized so closeCh is closed once
// and every caller waits for subscription cleanup.
func TestRedisConcurrentClose(t *testing.T) {
	t.Parallel()

	const closers = 8

	ps := newRedisPubSub(t)

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

// TestRedisSubscribeCloseRace runs Subscribe against Close: admission must
// either succeed fully or be rejected with ErrPubSubClosed, and a successfully
// admitted subscription must be cleaned up by the time Close returns.
func TestRedisSubscribeCloseRace(t *testing.T) {
	t.Parallel()

	const cycles = 10

	for range cycles {
		ps, err := pubsub.NewRedis(pubsub.RedisConfig{
			Client: nil,
			URL:    redisTestURL(t),
			Prefix: t.Name(),
		})
		if err != nil {
			t.Fatalf("NewRedis failed: %v", err)
		}

		var (
			sub    *pubsub.Subscription
			subErr error
		)

		runs := sync.WaitGroup{}
		runs.Go(func() {
			sub, subErr = ps.Subscribe(context.Background(), "topic")
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

		_, err = ps.Subscribe(context.Background(), "topic")
		if !errors.Is(err, pubsub.ErrPubSubClosed) {
			t.Errorf("Subscribe after Close: expected ErrPubSubClosed, got %v", err)
		}
	}
}

// TestRedisCloseWaitsForPendingSetup stalls a subscription handshake against a
// TCP endpoint that accepts connections but never replies, then closes the
// instance. go-redis ignores cancellation and handle closes while the handshake
// holds its internal mutex, so the setup stays pending until its read deadline
// (ReadTimeout) unwinds it; Close must wait for that cleanup instead of
// returning with the subscription blocked and its connection open, and the
// supplied client must stay usable.
func TestRedisCloseWaitsForPendingSetup(t *testing.T) {
	t.Parallel()

	const (
		// pendingSetup bounds how long the test waits for a defecting Close
		// to return before concluding Close is waiting for the setup. It is
		// shorter than the client ReadTimeout below, so the handshake is
		// normally still pending when it elapses.
		pendingSetup = 250 * time.Millisecond
	)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to start listener: %v", err)
	}

	t.Cleanup(func() {
		if closeErr := ln.Close(); closeErr != nil {
			t.Errorf("failed to close listener: %v", closeErr)
		}
	})

	// Accept connections but never reply, so the subscription handshake blocks.
	go func() {
		for {
			if _, acceptErr := ln.Accept(); acceptErr != nil {
				return
			}
		}
	}()

	connected := make(chan struct{}, 1)
	client := redis.NewClient(&redis.Options{
		Addr: ln.Addr().String(),
		// A short read deadline bounds how long the stalled handshake stays
		// pending instead of go-redis's multi-second default.
		ReadTimeout: 500 * time.Millisecond,
		Dialer: func(ctx context.Context, network, addr string) (net.Conn, error) {
			dialer := net.Dialer{}
			conn, dialErr := dialer.DialContext(ctx, network, addr)
			if dialErr == nil {
				select {
				case connected <- struct{}{}:
				default:
				}
			}

			return conn, dialErr
		},
	})

	t.Cleanup(func() {
		if closeErr := client.Close(); closeErr != nil {
			t.Errorf("failed to close client: %v", closeErr)
		}
	})

	ps, err := pubsub.NewRedis(pubsub.RedisConfig{Client: client, Prefix: t.Name()})
	if err != nil {
		t.Fatalf("NewRedis failed: %v", err)
	}

	t.Cleanup(func() {
		if closeErr := ps.Close(); closeErr != nil {
			t.Errorf("Close failed: %v", closeErr)
		}
	})

	subErr := make(chan error, 1)
	go func() {
		_, subscribeErr := ps.Subscribe(context.Background(), "topic")
		subErr <- subscribeErr
	}()

	select {
	case <-connected:
	case <-time.After(waitTimeout):
		t.Fatal("Timed out waiting for the stalled handshake to connect")
	}

	closed := make(chan error, 1)
	go func() {
		closed <- ps.Close()
	}()

	// The setup reports itself when it unwinds, so a Close that returns early
	// is only a defect while that report is still missing: if the handshake
	// already timed out before Close ran, there was nothing left to wait for.
	select {
	case closeErr := <-closed:
		select {
		case subscribeErr := <-subErr:
			if !errors.Is(subscribeErr, pubsub.ErrPubSubClosed) {
				t.Errorf("Pending Subscribe during Close: expected ErrPubSubClosed, got %v", subscribeErr)
			}

			if closeErr != nil {
				t.Errorf("Close failed: %v", closeErr)
			}
		default:
			t.Fatalf("Close returned while the subscription setup was still pending: %v", closeErr)
		}
	case <-time.After(pendingSetup):
		select {
		case subscribeErr := <-subErr:
			if !errors.Is(subscribeErr, pubsub.ErrPubSubClosed) {
				t.Errorf("Pending Subscribe during Close: expected ErrPubSubClosed, got %v", subscribeErr)
			}
		case <-time.After(waitTimeout):
			t.Fatal("Subscribe never unwound after Close started waiting for the pending setup")
		}

		select {
		case closeErr := <-closed:
			if closeErr != nil {
				t.Errorf("Close failed: %v", closeErr)
			}
		case <-time.After(waitTimeout):
			t.Fatal("Close did not return after the pending setup was released")
		}
	}

	pingCtx, pingCancel := context.WithTimeout(context.Background(), time.Second)
	defer pingCancel()

	if pingErr := client.Ping(pingCtx).Err(); errors.Is(pingErr, redis.ErrClosed) {
		t.Errorf("Close must not close a supplied client: %v", pingErr)
	}
}
