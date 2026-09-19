package service

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"in-memory-queue/internal/model"
	"in-memory-queue/internal/repository"
)

const (
	maxAllowedMessageSize   = 256 * 1024
	maxAllowedQueueDepth    = 1_000_000
	maxAttributeKeyLength   = 128
	maxAttributeValueLength = 1024
)

var queueMutex sync.RWMutex

type queueService struct {
	repository     repository.QueueRepository
	defaultDepth   int
	maxMessageSize int
	maxAttributes  int
}

// NewQueueService creates a queue service with the supplied defaults and limits.
func NewQueueService(repo repository.QueueRepository, defaultDepth int, maxMessageSizes ...int) QueueService {
	if repo == nil {
		repo = repository.NewMemoryRepository()
	}
	if defaultDepth <= 0 {
		defaultDepth = 10000
	}
	maxMessageSize := maxAllowedMessageSize
	if len(maxMessageSizes) > 0 && maxMessageSizes[0] > 0 {
		maxMessageSize = maxMessageSizes[0]
	}
	if maxMessageSize > maxAllowedMessageSize {
		maxMessageSize = maxAllowedMessageSize
	}
	maxAttributes := 10
	if len(maxMessageSizes) > 1 && maxMessageSizes[1] > 0 {
		maxAttributes = maxMessageSizes[1]
	}
	if maxAttributes > 10 {
		maxAttributes = 10
	}
	return &queueService{repository: repo, defaultDepth: defaultDepth, maxMessageSize: maxMessageSize, maxAttributes: maxAttributes}
}

func requestContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func (s *queueService) CreateQueue(ctx context.Context, name string, maxDepths ...int) (*model.Queue, error) {
	ctx = requestContext(ctx)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(name) < 1 || len(name) > 64 {
		log.Printf("[WARN] queue creation rejected: queue=%q reason=invalid_length", name)
		return nil, errors.New("queue name length must be between 1 and 64")
	}
	if !isValidQueueName(name) {
		log.Printf("[WARN] queue creation rejected: queue=%q reason=invalid_name", name)
		return nil, errors.New("queue name should contain only a-z, A-Z, 0-9, '-' or '_'")
	}
	queueMutex.Lock()
	defer queueMutex.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if _, exists, err := s.repository.Get(ctx, name); err != nil {
		return nil, err
	} else if exists {
		log.Printf("[WARN] queue creation rejected: queue=%s reason=already_exists", name)
		return nil, errors.New("queue already exists")
	}
	maxDepth := s.defaultDepth
	if len(maxDepths) > 0 {
		if maxDepths[0] < 1 || maxDepths[0] > maxAllowedQueueDepth {
			return nil, model.ErrInvalidQueueDepth
		}
		maxDepth = maxDepths[0]
	}
	queue := &model.Queue{Name: name, MaxDepth: maxDepth, Messages: []model.Message{}, CreatedAt: time.Now()}
	if err := s.repository.Create(ctx, queue); err != nil {
		log.Printf("[ERROR] queue creation failed: queue=%s error=%v", name, err)
		return nil, err
	}
	log.Printf("[INFO] queue created: queue=%s max_depth=%d", name, maxDepth)
	return cloneQueue(queue), nil
}

func (s *queueService) GetQueue(ctx context.Context, name string) (*model.Queue, error) {
	ctx = requestContext(ctx)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !isValidQueueName(name) {
		return nil, model.ErrInvalidQueueName
	}
	queueMutex.RLock()
	defer queueMutex.RUnlock()
	return s.getQueue(ctx, name)
}

func (s *queueService) ListQueues(ctx context.Context) ([]*model.Queue, error) {
	ctx = requestContext(ctx)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	queueMutex.RLock()
	defer queueMutex.RUnlock()
	queues, err := s.repository.List(ctx)
	if err != nil {
		return nil, err
	}
	snapshots := make([]*model.Queue, 0, len(queues))
	for _, queue := range queues {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		snapshots = append(snapshots, cloneQueue(queue))
	}
	return snapshots, nil
}

func (s *queueService) DeleteQueue(ctx context.Context, name string) error {
	ctx = requestContext(ctx)
	if err := ctx.Err(); err != nil {
		return err
	}
	if !isValidQueueName(name) {
		return model.ErrInvalidQueueName
	}
	queueMutex.Lock()
	defer queueMutex.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	deleted, err := s.repository.Delete(ctx, name)
	if err != nil {
		return err
	}
	if !deleted {
		log.Printf("[WARN] queue deletion failed: queue=%s reason=not_found", name)
		return model.ErrQueueNotFound
	}
	log.Printf("[INFO] queue deleted: queue=%s", name)
	return nil
}

func (s *queueService) Enqueue(ctx context.Context, name, body string, attributes map[string]string) (*model.Message, error) {
	ctx = requestContext(ctx)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !validAttributes(attributes, s.maxAttributes) {
		log.Printf("[WARN] message rejected: queue=%s reason=too_many_attributes count=%d", name, len(attributes))
		return nil, model.ErrInvalidAttributes
	}
	if len([]byte(body)) > s.maxMessageSize {
		log.Printf("[WARN] message rejected: queue=%s reason=message_too_large size=%d", name, len([]byte(body)))
		return nil, model.ErrMessageTooLarge
	}
	queueMutex.Lock()
	defer queueMutex.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	queue, err := s.getQueue(ctx, name)
	if err != nil {
		return nil, err
	}
	if len(queue.Messages) >= queue.MaxDepth {
		log.Printf("[WARN] enqueue rejected: queue=%s reason=queue_full depth=%d", name, queue.MaxDepth)
		return nil, model.ErrQueueFull
	}
	messageID, err := newMessageID()
	if err != nil {
		return nil, fmt.Errorf("generate message ID: %w", err)
	}
	message := model.Message{ID: messageID, Body: body, Attributes: attributes, EnqueuedAt: time.Now()}
	queue.Messages = append(queue.Messages, message)
	queue.CurrentMsgs++
	log.Printf("[INFO] message added: queue=%s message_id=%s", name, message.ID)
	return &message, nil
}

func validAttributes(attributes map[string]string, maxAttributes int) bool {
	if len(attributes) > maxAttributes {
		return false
	}
	for key, value := range attributes {
		if len(key) == 0 || len(key) > maxAttributeKeyLength || len(value) > maxAttributeValueLength {
			return false
		}
	}
	return true
}

func newMessageID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	value[6] = value[6]&0x0f | 0x40
	value[8] = value[8]&0x3f | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		value[0:4], value[4:6], value[6:8], value[8:10], value[10:16]), nil
}

func (s *queueService) Dequeue(ctx context.Context, name string) (*model.Message, error) {
	ctx = requestContext(ctx)
	queueMutex.Lock()
	defer queueMutex.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	queue, err := s.getQueue(ctx, name)
	if err != nil {
		return nil, err
	}
	if len(queue.Messages) == 0 {
		log.Printf("[WARN] dequeue failed: queue=%s reason=queue_empty", name)
		return nil, model.ErrQueueEmpty
	}
	message := queue.Messages[0]
	queue.Messages = queue.Messages[1:]
	queue.CurrentMsgs--
	log.Printf("[INFO] message removed: queue=%s message_id=%s", name, message.ID)
	return &message, nil
}

func (s *queueService) Peek(ctx context.Context, name string) (*model.Message, error) {
	ctx = requestContext(ctx)
	queueMutex.RLock()
	defer queueMutex.RUnlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	queue, err := s.getQueue(ctx, name)
	if err != nil {
		return nil, err
	}
	if len(queue.Messages) == 0 {
		log.Printf("[WARN] peek failed: queue=%s reason=queue_empty", name)
		return nil, model.ErrQueueEmpty
	}
	message := queue.Messages[0]
	log.Printf("[INFO] message viewed: queue=%s message_id=%s", name, message.ID)
	return &message, nil
}

func (s *queueService) getQueue(ctx context.Context, name string) (*model.Queue, error) {
	queue, exists, err := s.repository.Get(ctx, name)
	if err != nil {
		return nil, err
	}
	if !exists {
		log.Printf("[WARN] queue operation failed: queue=%s reason=not_found", name)
		return nil, model.ErrQueueNotFound
	}
	return queue, nil
}

func cloneQueue(queue *model.Queue) *model.Queue {
	clone := *queue
	clone.Messages = append([]model.Message(nil), queue.Messages...)
	return &clone
}

func isValidQueueName(name string) bool {
	if name == "" {
		return false
	}
	for _, character := range name {
		if (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') || (character >= '0' && character <= '9') || character == '-' || character == '_' {
			continue
		}
		return false
	}
	return true
}
