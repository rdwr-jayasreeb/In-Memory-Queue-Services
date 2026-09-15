package repository

import (
	"context"

	"in-memory-queue/internal/model"
)

type QueueRepository interface {
	Create(ctx context.Context, queue *model.Queue) error
	List(ctx context.Context) ([]*model.Queue, error)
	Get(ctx context.Context, name string) (*model.Queue, bool, error)
	Delete(ctx context.Context, name string) (bool, error)
}
