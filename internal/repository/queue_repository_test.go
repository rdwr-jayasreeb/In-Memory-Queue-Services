package repository

import (
	"context"
	"sync"
	"testing"
	"time"

	"in-memory-queue/internal/model"
)

func testQueue(name string) *model.Queue {
	return &model.Queue{Name: name, MaxDepth: 10, Messages: []model.Message{}, CreatedAt: time.Now()}
}

func TestMemoryRepositoryCRUD(t *testing.T) {
	repo := NewMemoryRepository()
	ctx := context.Background()
	queue := testQueue("orders")
	if err := repo.Create(ctx, queue); err != nil {
		t.Fatal(err)
	}
	got, ok, err := repo.Get(ctx, "orders")
	if err != nil || !ok || got.Name != queue.Name || got.MaxDepth != queue.MaxDepth {
		t.Fatalf("get failed: got=%v ok=%v err=%v", got, ok, err)
	}
	items, err := repo.List(ctx)
	if err != nil || len(items) != 1 {
		t.Fatalf("list failed: %d %v", len(items), err)
	}
	deleted, err := repo.Delete(ctx, "orders")
	if err != nil || !deleted {
		t.Fatalf("delete failed: %v %v", deleted, err)
	}
	deleted, err = repo.Delete(ctx, "orders")
	if err != nil || deleted {
		t.Fatalf("expected missing delete, got %v %v", deleted, err)
	}
}

func TestMemoryRepositoryHonorsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	repo := NewMemoryRepository()
	if err := repo.Create(ctx, testQueue("orders")); err == nil {
		t.Fatal("expected context cancellation")
	}
	if _, err := repo.List(ctx); err == nil {
		t.Fatal("expected context cancellation")
	}
	if _, _, err := repo.Get(ctx, "orders"); err == nil {
		t.Fatal("expected context cancellation")
	}
	if _, err := repo.Delete(ctx, "orders"); err == nil {
		t.Fatal("expected context cancellation")
	}
}

func TestMemoryRepositorySupportsConcurrentAccess(t *testing.T) {
	repo := NewMemoryRepository()
	ctx := context.Background()

	var group sync.WaitGroup
	for index := 0; index < 100; index++ {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			name := "queue-" + string(rune('a'+index))
			if err := repo.Create(ctx, testQueue(name)); err != nil {
				t.Errorf("create queue: %v", err)
			}
			if _, _, err := repo.Get(ctx, name); err != nil {
				t.Errorf("get queue: %v", err)
			}
			if _, err := repo.List(ctx); err != nil {
				t.Errorf("list queues: %v", err)
			}
			if _, err := repo.Delete(ctx, name); err != nil {
				t.Errorf("delete queue: %v", err)
			}
		}(index)
	}
	group.Wait()

	queues, err := repo.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(queues) != 0 {
		t.Fatalf("expected repository to be empty, got %d queues", len(queues))
	}
}
