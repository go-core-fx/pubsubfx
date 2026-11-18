package pubsubfx

import (
	"fmt"
	"net/url"

	"github.com/go-core-fx/pubsubfx/pubsub"
)

// New creates a PubSub for the backend selected by config.URL.
//
// The default and "memory://" select the in-process backend; "redis://"
// selects Redis, where Prefix namespaces the topics. Any other scheme, or a
// URL the backend rejects, fails with ErrInvalidConfig.
func New(config Config) (pubsub.PubSub, error) {
	if config.URL == "" {
		config.URL = "memory://"
	}

	u, err := url.Parse(config.URL)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to parse url: %w", ErrInvalidConfig, err)
	}

	opts := []pubsub.Option{pubsub.WithBufferSize(config.BufferSize)}

	switch u.Scheme {
	case "memory":
		return pubsub.NewMemory(opts...), nil
	case "redis":
		ps, redisErr := pubsub.NewRedis(pubsub.RedisConfig{
			Client: nil,
			URL:    config.URL,
			Prefix: config.Prefix,
		}, opts...)
		if redisErr != nil {
			return nil, fmt.Errorf("%w: failed to create redis pubsub: %w", ErrInvalidConfig, redisErr)
		}

		return ps, nil
	default:
		return nil, fmt.Errorf("%w: invalid scheme: %s", ErrInvalidConfig, u.Scheme)
	}
}
