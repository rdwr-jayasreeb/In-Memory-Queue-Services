package model

import "errors"

var (
	ErrQueueManagerRequired = errors.New("queue manager is required")
	ErrQueueRequired        = errors.New("queue is required")
	ErrInvalidRequest       = errors.New("invalid_request")
	ErrInvalidQueueName     = errors.New("invalid_queue_name")
	ErrQueueNotFound        = errors.New("Queue Not Found")
	ErrQueueEmpty           = errors.New("queue is empty")
	ErrQueueFull            = errors.New("queue is full")
	ErrInvalidAttributes    = errors.New("invalid_attributes")
	ErrMessageTooLarge      = errors.New("message_too_large")
)
