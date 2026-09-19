// Package service implements queue business operations.
package service

import (
	"context"

	"in-memory-queue/internal/model"
)

// QueueService provides validated queue and message operations.
type QueueService interface {
	CreateQueue(ctx context.Context, name string, maxDepth ...int) (*model.Queue, error)
	GetQueue(ctx context.Context, name string) (*model.Queue, error)
	ListQueues(ctx context.Context) ([]*model.Queue, error)
	DeleteQueue(ctx context.Context, name string) error
	Enqueue(ctx context.Context, name, body string, attributes map[string]string) (*model.Message, error)
	Dequeue(ctx context.Context, name string) (*model.Message, error)
	Peek(ctx context.Context, name string) (*model.Message, error)
}
