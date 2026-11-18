package pubsubfx

// Config controls the pubsub backend via a URL (e.g., "memory://", "redis://...")
// and the subscription buffer.
type Config struct {
	// URL selects and configures the backend. It is passed to the backend
	// unchanged: "memory://" serves an in-process fan-out pubsub, and
	// "redis://" publishes through a Redis server. The backend parses the
	// URL, so a malformed one fails when the pubsub is created (typically at
	// app startup) instead of at first use.
	URL string

	// BufferSize is the capacity of the channel behind each subscription.
	// With the memory backend, a zero value makes Publish block until every
	// subscriber has accepted the message, and a positive value lets a
	// subscriber lag by up to BufferSize messages before Publish blocks. The
	// redis backend does not apply this backpressure: a successful Publish
	// means the Redis server accepted the message, not that subscribers have
	// received it.
	BufferSize uint

	// Prefix is prepended to every topic name, so independent applications
	// sharing one Redis server do not see each other's messages. An empty
	// value publishes to unprefixed topics. Ignored by the memory backend,
	// which scopes topics per instance.
	Prefix string
}
