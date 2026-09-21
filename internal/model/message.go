package model

import "time"

type Message struct {
	ID         string            `json:"id"`
	Body       string            `json:"body"`
	Attributes map[string]string `json:"attributes"`
	EnqueuedAt time.Time         `json:"enqueued_at"`
}
