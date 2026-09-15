package model

import "time"

type Queue struct {
	Name        string    `json:"name"`
	MaxDepth    int       `json:"max_depth"`
	CurrentMsgs int       `json:"-"`
	Messages    []Message `json:"-"`
	CreatedAt   time.Time `json:"created_at"`
}
