package model

import "time"

// Message is an item stored in a queue.
type Message struct {
	// ID uniquely identifies the message.
	ID string `json:"id"`
	// Body contains the message payload.
	Body string `json:"body"`
	// Attributes contains optional message metadata.
	Attributes map[string]string `json:"attributes"`
	// EnqueuedAt records when the message entered the queue.
	EnqueuedAt time.Time `json:"enqueued_at"`
}
