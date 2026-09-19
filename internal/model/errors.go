// Package model defines queue and message domain types.
package model

import "errors"

var (
	// ErrQueueManagerRequired indicates a missing queue manager.
	ErrQueueManagerRequired = errors.New("queue manager is required")
	// ErrQueueRequired indicates a missing queue.
	ErrQueueRequired = errors.New("queue is required")
	// ErrInvalidRequest indicates a malformed request.
	ErrInvalidRequest = errors.New("invalid_request")
	// ErrInvalidQueueName indicates an invalid queue name.
	ErrInvalidQueueName = errors.New("invalid_queue_name")
	// ErrQueueNotFound indicates that the requested queue does not exist.
	ErrQueueNotFound = errors.New("Queue Not Found")
	// ErrQueueEmpty indicates that no message is available.
	ErrQueueEmpty = errors.New("queue is empty")
	// ErrQueueFull indicates that a queue cannot accept another message.
	ErrQueueFull = errors.New("queue is full")
	// ErrInvalidQueueDepth indicates that a queue depth is outside its allowed range.
	ErrInvalidQueueDepth = errors.New("invalid_queue_depth")
	// ErrInvalidAttributes indicates invalid message attributes.
	ErrInvalidAttributes = errors.New("invalid_attributes")
	// ErrMessageTooLarge indicates that a message exceeds the configured size limit.
	ErrMessageTooLarge = errors.New("message_too_large")
)
