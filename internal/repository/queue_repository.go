package repository

import (
	"context"

	"in-memory-queue/internal/model"
)

// MemoryRepository stores queues in process memory.
type MemoryRepository struct {
	queues map[string]*model.Queue
}

// NewMemoryRepository creates an empty in-memory queue repository.
func NewMemoryRepository() *MemoryRepository {
	return &MemoryRepository{
		queues: make(map[string]*model.Queue)}
}

func checkContext(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	return ctx.Err()
}

// Create stores queue by its name.
func (r *MemoryRepository) Create(ctx context.Context, queue *model.Queue) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	r.queues[queue.Name] = queue
	return nil
}

// List returns all stored queues.
func (r *MemoryRepository) List(ctx context.Context) ([]*model.Queue, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	queues := make([]*model.Queue, 0, len(r.queues))
	for _, queue := range r.queues {
		queues = append(queues, queue)
	}
	return queues, nil
}

// Get returns the queue with name and whether it exists.
func (r *MemoryRepository) Get(ctx context.Context, name string) (*model.Queue, bool, error) {
	if err := checkContext(ctx); err != nil {
		return nil, false, err
	}
	queue, exists := r.queues[name]
	return queue, exists, nil
}

// Delete removes the queue with name and reports whether it existed.
func (r *MemoryRepository) Delete(ctx context.Context, name string) (bool, error) {
	if err := checkContext(ctx); err != nil {
		return false, err
	}
	if _, exists := r.queues[name]; !exists {
		return false, nil
	}
	delete(r.queues, name)
	return true, nil
}
