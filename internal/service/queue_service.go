package service

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log"
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

func (s *queueService) CreateQueue(ctx context.Context, name string, maxDepths ...int) (*model.QueueDTO, error) {
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
	maxDepth := s.defaultDepth
	if len(maxDepths) > 0 {
		if maxDepths[0] < 1 || maxDepths[0] > maxAllowedQueueDepth {
			return nil, model.ErrInvalidQueueDepth
		}
		maxDepth = maxDepths[0]
	}
	queue := &model.Queue{Name: name, MaxDepth: maxDepth, Messages: []model.Message{}, CreatedAt: time.Now()}
	if err := s.repository.Create(ctx, queue); err != nil {
		if errors.Is(err, model.ErrQueueAlreadyExists) {
			log.Printf("[WARN] queue creation rejected: queue=%s reason=already_exists", name)
		}
		log.Printf("[ERROR] queue creation failed: queue=%s error=%v", name, err)
		return nil, err
	}
	log.Printf("[INFO] queue created: queue=%s max_depth=%d", name, maxDepth)
	return toQueueDTO(queue), nil
}

func (s *queueService) GetQueue(ctx context.Context, name string) (*model.QueueDTO, error) {
	ctx = requestContext(ctx)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !isValidQueueName(name) {
		return nil, model.ErrInvalidQueueName
	}
	queue, exists, err := s.repository.Get(ctx, name)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, model.ErrQueueNotFound
	}
	return toQueueDTO(queue), nil
}

func (s *queueService) ListQueues(ctx context.Context) ([]*model.QueueDTO, error) {
	ctx = requestContext(ctx)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	queues, err := s.repository.List(ctx)
	if err != nil {
		return nil, err
	}
	items := make([]*model.QueueDTO, 0, len(queues))
	for _, queue := range queues {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		items = append(items, toQueueDTO(queue))
	}
	return items, nil
}

func (s *queueService) DeleteQueue(ctx context.Context, name string) error {
	ctx = requestContext(ctx)
	if err := ctx.Err(); err != nil {
		return err
	}
	if !isValidQueueName(name) {
		return model.ErrInvalidQueueName
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
	messageID, err := newMessageID()
	if err != nil {
		return nil, fmt.Errorf("generate message ID: %w", err)
	}
	message := model.Message{ID: messageID, Body: body, Attributes: attributes, EnqueuedAt: time.Now()}
	if err := s.repository.Enqueue(ctx, name, message); err != nil {
		if errors.Is(err, model.ErrQueueFull) {
			log.Printf("[WARN] enqueue rejected: queue=%s reason=queue_full", name)
		}
		return nil, err
	}
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
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	message, err := s.repository.Dequeue(ctx, name)
	if errors.Is(err, model.ErrQueueEmpty) {
		log.Printf("[WARN] dequeue failed: queue=%s reason=queue_empty", name)
	}
	if err != nil {
		return nil, err
	}
	log.Printf("[INFO] message removed: queue=%s message_id=%s", name, message.ID)
	return message, nil
}

func (s *queueService) Peek(ctx context.Context, name string) (*model.Message, error) {
	ctx = requestContext(ctx)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	message, err := s.repository.Peek(ctx, name)
	if errors.Is(err, model.ErrQueueEmpty) {
		log.Printf("[WARN] peek failed: queue=%s reason=queue_empty", name)
	}
	if err != nil {
		return nil, err
	}
	log.Printf("[INFO] message viewed: queue=%s message_id=%s", name, message.ID)
	return message, nil
}

func toQueueDTO(queue *model.Queue) *model.QueueDTO {
	return &model.QueueDTO{
		Name:         queue.Name,
		MaxDepth:     queue.MaxDepth,
		MessageCount: queue.CurrentMsgs,
		CreatedAt:    queue.CreatedAt,
	}
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
