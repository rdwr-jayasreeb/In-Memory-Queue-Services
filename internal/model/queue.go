package model

import "time"

// Queue stores messages and its queue configuration.
type Queue struct {
	// Name uniquely identifies the queue.
	Name string `json:"name"`
	// MaxDepth is the maximum number of messages allowed.
	MaxDepth int `json:"max_depth"`
	// CurrentMsgs is the current number of stored messages.
	CurrentMsgs int `json:"-"`
	// Messages contains the queued messages in FIFO order.
	Messages []Message `json:"-"`
	// CreatedAt records when the queue was created.
	CreatedAt time.Time `json:"created_at"`
}

// QueueDTO is the read-only queue metadata exposed by the service layer.
type QueueDTO struct {
	Name         string    `json:"name"`
	MaxDepth     int       `json:"max_depth"`
	MessageCount int       `json:"message_count"`
	CreatedAt    time.Time `json:"created_at"`
}
