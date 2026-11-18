package pubsub

import "errors"

var (
	// ErrInvalidConfig indicates an invalid configuration.
	ErrInvalidConfig = errors.New("invalid config")
	// ErrPubSubClosed indicates the operation was attempted on a closed
	// pubsub instance.
	ErrPubSubClosed = errors.New("pubsub is closed")
	// ErrInvalidTopic indicates an invalid topic name.
	ErrInvalidTopic = errors.New("invalid topic name")
)
