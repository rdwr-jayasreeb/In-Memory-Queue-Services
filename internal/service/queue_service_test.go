package service

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"sync"
	"testing"

	"in-memory-queue/internal/model"
	"in-memory-queue/internal/repository"
)

func TestEnqueueStoresAttributesAndEnqueuedAt(t *testing.T) {
	queueService := NewQueueService(repository.NewMemoryRepository(), 10)
	if _, err := queueService.CreateQueue(context.Background(), "orders"); err != nil {
		t.Fatalf("create queue: %v", err)
	}

	attributes := map[string]string{"source": "api"}
	if _, err := queueService.Enqueue(context.Background(), "orders", "order-1", attributes); err != nil {
		t.Fatalf("enqueue message: %v", err)
	}
	queue, err := queueService.GetQueue(context.Background(), "orders")
	if err != nil {
		t.Fatalf("get queue: %v", err)
	}
	if queue.MessageCount != 1 {
		t.Fatalf("expected current message count 1, got %d", queue.MessageCount)
	}

	message, err := queueService.Peek(context.Background(), "orders")
	if err != nil {
		t.Fatalf("peek message: %v", err)
	}
	if message.Attributes["source"] != "api" {
		t.Fatalf("expected attributes to be stored, got %#v", message.Attributes)
	}
	if message.EnqueuedAt.IsZero() {
		t.Fatal("expected EnqueuedAt to be set")
	}
	if message.ID == "" {
		t.Fatal("expected a generated message ID")
	}
	if !uuidV4Pattern.MatchString(message.ID) {
		t.Fatalf("expected UUIDv4 message ID, got %q", message.ID)
	}
}

var uuidV4Pattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func TestEnqueueGeneratesUniqueIDsAndDequeueIsFIFO(t *testing.T) {
	queueService := NewQueueService(repository.NewMemoryRepository(), 10)
	if _, err := queueService.CreateQueue(context.Background(), "orders"); err != nil {
		t.Fatalf("create queue: %v", err)
	}

	if _, err := queueService.Enqueue(context.Background(), "orders", "first", nil); err != nil {
		t.Fatalf("enqueue first message: %v", err)
	}
	first, err := queueService.Peek(context.Background(), "orders")
	if err != nil {
		t.Fatalf("peek first message: %v", err)
	}
	if _, err := queueService.Enqueue(context.Background(), "orders", "second", nil); err != nil {
		t.Fatalf("enqueue second message: %v", err)
	}
	second, err := queueService.Dequeue(context.Background(), "orders")
	if err != nil {
		t.Fatalf("dequeue first message: %v", err)
	}
	if second.Body != "first" {
		t.Fatalf("expected FIFO dequeue to return first, got %s", second.Body)
	}
	next, err := queueService.Peek(context.Background(), "orders")
	if err != nil {
		t.Fatalf("peek second message: %v", err)
	}
	if next.Body != "second" {
		t.Fatalf("expected second message to remain, got %s", next.Body)
	}
	if first.ID == next.ID {
		t.Fatal("expected message IDs to be unique")
	}
	queue, err := queueService.GetQueue(context.Background(), "orders")
	if err != nil {
		t.Fatalf("get queue after dequeue: %v", err)
	}
	if queue.MessageCount != 1 {
		t.Fatalf("expected current message count 1 after dequeue, got %d", queue.MessageCount)
	}
}

func TestDeleteQueueRemovesMessageState(t *testing.T) {
	queueService := NewQueueService(repository.NewMemoryRepository(), 10)
	if _, err := queueService.CreateQueue(context.Background(), "orders"); err != nil {
		t.Fatalf("create queue: %v", err)
	}
	if _, err := queueService.Enqueue(context.Background(), "orders", "order-1", nil); err != nil {
		t.Fatalf("enqueue message: %v", err)
	}
	if err := queueService.DeleteQueue(context.Background(), "orders"); err != nil {
		t.Fatalf("delete queue: %v", err)
	}
	if _, err := queueService.GetQueue(context.Background(), "orders"); !errors.Is(err, model.ErrQueueNotFound) {
		t.Fatalf("expected deleted queue to be unavailable, got %v", err)
	}
}

func TestConcurrentEnqueueAndDequeueMaintainQueueState(t *testing.T) {
	queueService := NewQueueService(repository.NewMemoryRepository(), 100)
	if _, err := queueService.CreateQueue(context.Background(), "orders"); err != nil {
		t.Fatalf("create queue: %v", err)
	}

	var enqueueGroup sync.WaitGroup
	enqueueErrors := make(chan error, 100)
	for index := 0; index < 100; index++ {
		enqueueGroup.Add(1)
		go func(index int) {
			defer enqueueGroup.Done()
			if _, err := queueService.Enqueue(context.Background(), "orders", strings.Repeat("x", index+1), nil); err != nil {
				enqueueErrors <- err
			}
		}(index)
	}
	enqueueGroup.Wait()
	close(enqueueErrors)
	for err := range enqueueErrors {
		t.Fatalf("concurrent enqueue failed: %v", err)
	}

	queue, err := queueService.GetQueue(context.Background(), "orders")
	if err != nil {
		t.Fatalf("get queue after enqueue: %v", err)
	}
	if queue.MessageCount != 100 {
		t.Fatalf("expected 100 messages after enqueue, got %d", queue.MessageCount)
	}

	var dequeueGroup sync.WaitGroup
	dequeueErrors := make(chan error, 100)
	for index := 0; index < 100; index++ {
		dequeueGroup.Add(1)
		go func() {
			defer dequeueGroup.Done()
			if _, err := queueService.Dequeue(context.Background(), "orders"); err != nil {
				dequeueErrors <- err
			}
		}()
	}
	dequeueGroup.Wait()
	close(dequeueErrors)
	for err := range dequeueErrors {
		t.Fatalf("concurrent dequeue failed: %v", err)
	}

	queue, err = queueService.GetQueue(context.Background(), "orders")
	if err != nil {
		t.Fatalf("get queue after dequeue: %v", err)
	}
	if queue.MessageCount != 0 {
		t.Fatalf("expected empty queue after dequeue, got count=%d", queue.MessageCount)
	}
}

func TestEnqueueRejectsMoreThanTenAttributes(t *testing.T) {
	queueService := NewQueueService(repository.NewMemoryRepository(), 10)
	if _, err := queueService.CreateQueue(context.Background(), "orders"); err != nil {
		t.Fatalf("create queue: %v", err)
	}

	attributes := make(map[string]string, 11)
	for index := 0; index < 11; index++ {
		attributes[strings.Repeat("a", index+1)] = "value"
	}

	_, err := queueService.Enqueue(context.Background(), "orders", "order-1", attributes)
	if !errors.Is(err, model.ErrInvalidAttributes) {
		t.Fatalf("expected invalid attributes error, got %v", err)
	}
}

func TestEnqueueRejectsInvalidAttributeSizes(t *testing.T) {
	queueService := NewQueueService(repository.NewMemoryRepository(), 10)
	if _, err := queueService.CreateQueue(context.Background(), "orders"); err != nil {
		t.Fatalf("create queue: %v", err)
	}

	attributes := map[string]string{strings.Repeat("a", maxAttributeKeyLength+1): "value"}
	if _, err := queueService.Enqueue(context.Background(), "orders", "order-1", attributes); !errors.Is(err, model.ErrInvalidAttributes) {
		t.Fatalf("expected invalid attribute key error, got %v", err)
	}

	attributes = map[string]string{"key": strings.Repeat("a", maxAttributeValueLength+1)}
	if _, err := queueService.Enqueue(context.Background(), "orders", "order-1", attributes); !errors.Is(err, model.ErrInvalidAttributes) {
		t.Fatalf("expected invalid attribute value error, got %v", err)
	}
}

func TestEnqueueRejectsBodyOver256KB(t *testing.T) {
	queueService := NewQueueService(repository.NewMemoryRepository(), 10)
	if _, err := queueService.CreateQueue(context.Background(), "orders"); err != nil {
		t.Fatalf("create queue: %v", err)
	}

	_, err := queueService.Enqueue(context.Background(), "orders", strings.Repeat("a", 256*1024+1), nil)
	if !errors.Is(err, model.ErrMessageTooLarge) {
		t.Fatalf("expected message too large error, got %v", err)
	}
}

func TestCreateQueueRejectsInvalidExplicitDepth(t *testing.T) {
	queueService := NewQueueService(repository.NewMemoryRepository(), 10)
	if _, err := queueService.CreateQueue(context.Background(), "zero-depth", 0); !errors.Is(err, model.ErrInvalidQueueDepth) {
		t.Fatalf("expected invalid zero depth error, got %v", err)
	}
	if _, err := queueService.CreateQueue(context.Background(), "large-depth", maxAllowedQueueDepth+1); !errors.Is(err, model.ErrInvalidQueueDepth) {
		t.Fatalf("expected invalid large depth error, got %v", err)
	}
}

func TestQueueCRUDAndValidation(t *testing.T) {
	ctx := context.Background()
	queueService := NewQueueService(repository.NewMemoryRepository(), 10)
	if _, err := queueService.CreateQueue(ctx, "bad name"); err == nil {
		t.Fatal("expected invalid queue name")
	}
	if _, err := queueService.CreateQueue(ctx, "orders", 5); err != nil {
		t.Fatal(err)
	}
	if _, err := queueService.CreateQueue(ctx, "orders"); err == nil {
		t.Fatal("expected duplicate queue error")
	}
	queues, err := queueService.ListQueues(ctx)
	if err != nil || len(queues) != 1 {
		t.Fatalf("expected one queue, got %d %v", len(queues), err)
	}
	if err := queueService.DeleteQueue(ctx, "missing"); !errors.Is(err, model.ErrQueueNotFound) {
		t.Fatalf("expected not found, got %v", err)
	}
	if err := queueService.DeleteQueue(ctx, "orders"); err != nil {
		t.Fatal(err)
	}
	if _, err := queueService.GetQueue(ctx, "bad name"); !errors.Is(err, model.ErrInvalidQueueName) {
		t.Fatalf("expected invalid name, got %v", err)
	}
}

func TestQueueCapacityAndEmptyBehavior(t *testing.T) {
	ctx := context.Background()
	queueService := NewQueueService(repository.NewMemoryRepository(), 1)
	if _, err := queueService.CreateQueue(ctx, "orders"); err != nil {
		t.Fatal(err)
	}
	if _, err := queueService.Enqueue(ctx, "orders", "one", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := queueService.Enqueue(ctx, "orders", "two", nil); !errors.Is(err, model.ErrQueueFull) {
		t.Fatalf("expected full, got %v", err)
	}
	if _, err := queueService.Dequeue(ctx, "orders"); err != nil {
		t.Fatal(err)
	}
	if _, err := queueService.Dequeue(ctx, "orders"); !errors.Is(err, model.ErrQueueEmpty) {
		t.Fatalf("expected empty, got %v", err)
	}
	if _, err := queueService.Peek(ctx, "orders"); !errors.Is(err, model.ErrQueueEmpty) {
		t.Fatalf("expected empty peek, got %v", err)
	}
}

func TestServiceHonorsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	queueService := NewQueueService(repository.NewMemoryRepository(), 10)
	if _, err := queueService.CreateQueue(ctx, "orders"); err == nil {
		t.Fatal("expected canceled create")
	}
	if _, err := queueService.ListQueues(ctx); err == nil {
		t.Fatal("expected canceled list")
	}
	if _, err := queueService.GetQueue(ctx, "orders"); err == nil {
		t.Fatal("expected canceled get")
	}
	if err := queueService.DeleteQueue(ctx, "orders"); err == nil {
		t.Fatal("expected canceled delete")
	}
	if _, err := queueService.Enqueue(ctx, "orders", "x", nil); err == nil {
		t.Fatal("expected canceled enqueue")
	}
	if _, err := queueService.Dequeue(ctx, "orders"); err == nil {
		t.Fatal("expected canceled dequeue")
	}
	if _, err := queueService.Peek(ctx, "orders"); err == nil {
		t.Fatal("expected canceled peek")
	}
}
