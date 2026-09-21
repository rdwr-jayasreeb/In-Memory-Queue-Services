package repository

import (
	"context"
	"sync"

	"in-memory-queue/internal/model"
)

// MemoryRepository stores queues in process memory.
type MemoryRepository struct {
	mu     sync.RWMutex
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
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.queues[queue.Name]; exists {
		return model.ErrQueueAlreadyExists
	}
	r.queues[queue.Name] = cloneQueue(queue)
	return nil
}

// List returns all stored queues.
func (r *MemoryRepository) List(ctx context.Context) ([]*model.Queue, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	queues := make([]*model.Queue, 0, len(r.queues))
	for _, queue := range r.queues {
		queues = append(queues, queueMetadata(queue))
	}
	return queues, nil
}

// Get returns the queue with name and whether it exists.
func (r *MemoryRepository) Get(ctx context.Context, name string) (*model.Queue, bool, error) {
	if err := checkContext(ctx); err != nil {
		return nil, false, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	queue, exists := r.queues[name]
	if !exists {
		return nil, false, nil
	}
	return queueMetadata(queue), true, nil
}

// Delete removes the queue with name and reports whether it existed.
func (r *MemoryRepository) Delete(ctx context.Context, name string) (bool, error) {
	if err := checkContext(ctx); err != nil {
		return false, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.queues[name]; !exists {
		return false, nil
	}
	delete(r.queues, name)
	return true, nil
}

// Enqueue appends a message while holding the repository write lock.
func (r *MemoryRepository) Enqueue(ctx context.Context, name string, message model.Message) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	queue, exists := r.queues[name]
	if !exists {
		return model.ErrQueueNotFound
	}
	if len(queue.Messages) >= queue.MaxDepth {
		return model.ErrQueueFull
	}
	queue.Messages = append(queue.Messages, cloneMessage(message))
	queue.CurrentMsgs++
	return nil
}

// Dequeue removes and returns the oldest message atomically.
func (r *MemoryRepository) Dequeue(ctx context.Context, name string) (*model.Message, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	queue, exists := r.queues[name]
	if !exists {
		return nil, model.ErrQueueNotFound
	}
	if len(queue.Messages) == 0 {
		return nil, model.ErrQueueEmpty
	}
	message := cloneMessage(queue.Messages[0])
	queue.Messages = queue.Messages[1:]
	queue.CurrentMsgs--
	return &message, nil
}

// Peek returns the oldest message without removing it.
func (r *MemoryRepository) Peek(ctx context.Context, name string) (*model.Message, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	queue, exists := r.queues[name]
	if !exists {
		return nil, model.ErrQueueNotFound
	}
	if len(queue.Messages) == 0 {
		return nil, model.ErrQueueEmpty
	}
	message := cloneMessage(queue.Messages[0])
	return &message, nil
}

func cloneQueue(queue *model.Queue) *model.Queue {
	clone := *queue
	clone.Messages = make([]model.Message, len(queue.Messages))
	for index, message := range queue.Messages {
		clone.Messages[index] = cloneMessage(message)
	}
	return &clone
}

func queueMetadata(queue *model.Queue) *model.Queue {
	return &model.Queue{
		Name:        queue.Name,
		MaxDepth:    queue.MaxDepth,
		CurrentMsgs: queue.CurrentMsgs,
		CreatedAt:   queue.CreatedAt,
	}
}

func cloneMessage(message model.Message) model.Message {
	clone := message
	clone.Attributes = make(map[string]string, len(message.Attributes))
	for key, value := range message.Attributes {
		clone.Attributes[key] = value
	}
	return clone
}
