// Package handler exposes HTTP endpoints for queue operations.
package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"in-memory-queue/internal/logging"
	"in-memory-queue/internal/model"
	"in-memory-queue/internal/service"
)

type queueRequest struct {
	Name     string `json:"name"`
	MaxDepth *int   `json:"max_depth"`
}

type messageRequest struct {
	Name       string            `json:"name"`
	Body       string            `json:"body"`
	Attributes map[string]string `json:"attributes"`
}

type queueResponse struct {
	Name         string `json:"name"`
	MaxDepth     int    `json:"max_depth"`
	MessageCount int    `json:"message_count"`
	CreatedAt    string `json:"created_at"`
}

// QueueHandler routes HTTP queue requests to a QueueService.
type QueueHandler struct {
	service service.QueueService
}

// NewQueueHandler creates a handler backed by queueService.
func NewQueueHandler(queueService service.QueueService) *QueueHandler {
	return &QueueHandler{service: queueService}
}

// Queues handles POST /queues and GET /queues.
func (h *QueueHandler) Queues(response http.ResponseWriter, request *http.Request) {
	switch request.Method {
	case http.MethodPost:
		h.createQueue(response, request)
	case http.MethodGet:
		h.listQueues(response, request)
	default:
		writeError(response, http.StatusMethodNotAllowed, "invalid_request", "Method not allowed")
	}
}

// QueueResource handles /queues/{name} and /queues/{name}/messages.
func (h *QueueHandler) QueueResource(response http.ResponseWriter, request *http.Request) {
	parts := strings.Split(strings.Trim(strings.TrimPrefix(request.URL.Path, "/queues/"), "/"), "/")
	if len(parts) == 1 && parts[0] != "" {
		h.queueDetails(response, request, parts[0])
		return
	}
	if len(parts) == 2 && parts[1] == "messages" && parts[0] != "" {
		h.messageResource(response, request, parts[0])
		return
	}
	writeError(response, http.StatusNotFound, "invalid_request", "Resource not found")
}

func (h *QueueHandler) createQueue(response http.ResponseWriter, request *http.Request) {
	var input queueRequest
	if err := decodeJSON(request.Body, &input); err != nil {
		logging.Warnf("invalid request: method=%s path=%s reason=invalid_json", request.Method, request.URL.Path)
		writeError(response, http.StatusBadRequest, "invalid_request", "Invalid request body")
		return
	}
	maxDepths := []int{}
	if input.MaxDepth != nil {
		maxDepths = append(maxDepths, *input.MaxDepth)
	}
	queue, err := h.service.CreateQueue(request.Context(), input.Name, maxDepths...)
	if err != nil {
		if errors.Is(err, model.ErrInvalidQueueDepth) {
			writeError(response, http.StatusBadRequest, "invalid_queue_depth", "Queue max depth must be between 1 and 1000000")
			return
		}
		if strings.Contains(err.Error(), "already exists") {
			writeError(response, http.StatusConflict, "queue_already_exists", "Queue '"+input.Name+"' already exists")
			return
		}
		writeError(response, http.StatusBadRequest, "invalid_queue_name", "Queue name must be 1-64 characters, alphanumeric, underscore, or hyphen")
		return
	}
	writeJSON(response, http.StatusCreated, toQueueResponse(queue))
}

func (h *QueueHandler) listQueues(response http.ResponseWriter, request *http.Request) {
	queues, err := h.service.ListQueues(request.Context())
	if err != nil {
		writeOperationError(response, err)
		return
	}
	items := make([]queueResponse, 0, len(queues))
	for _, queue := range queues {
		items = append(items, toQueueResponse(queue))
	}
	writeJSON(response, http.StatusOK, map[string][]queueResponse{"queues": items})
}

func (h *QueueHandler) queueDetails(response http.ResponseWriter, request *http.Request, name string) {
	queue, err := h.service.GetQueue(request.Context(), name)
	if err != nil {
		h.writeQueueLookupError(response, name, err)
		return
	}
	if request.Method == http.MethodGet {
		writeJSON(response, http.StatusOK, toQueueResponse(queue))
		return
	}
	if request.Method == http.MethodDelete {
		if err := h.service.DeleteQueue(request.Context(), name); err != nil {
			h.writeQueueLookupError(response, name, err)
			return
		}
		response.WriteHeader(http.StatusNoContent)
		return
	}
	writeError(response, http.StatusMethodNotAllowed, "invalid_request", "Method not allowed")
}

func (h *QueueHandler) messageResource(response http.ResponseWriter, request *http.Request, name string) {
	switch request.Method {
	case http.MethodPost:
		h.enqueueMessage(response, request, name)
	case http.MethodDelete:
		message, err := h.service.Dequeue(request.Context(), name)
		if err != nil {
			h.writeMessageOperationError(response, name, err)
			return
		}
		writeJSON(response, http.StatusOK, message)
	case http.MethodGet:
		message, err := h.service.Peek(request.Context(), name)
		if err != nil {
			h.writeMessageOperationError(response, name, err)
			return
		}
		writeJSON(response, http.StatusOK, message)
	default:
		writeError(response, http.StatusMethodNotAllowed, "invalid_request", "Method not allowed")
	}
}

func (h *QueueHandler) enqueueMessage(response http.ResponseWriter, request *http.Request, name string) {
	request.Body = http.MaxBytesReader(response, request.Body, maxRequestBodyBytes)
	var input messageRequest
	if err := json.NewDecoder(request.Body).Decode(&input); err != nil || input.Body == "" {
		logging.Warnf("invalid message request: method=%s path=%s", request.Method, request.URL.Path)
		writeError(response, http.StatusBadRequest, "invalid_message", "Message body is required")
		return
	}
	h.enqueueInput(response, request, name, input)
}

func (h *QueueHandler) enqueueInput(response http.ResponseWriter, request *http.Request, name string, input messageRequest) {
	message, err := h.service.Enqueue(request.Context(), name, input.Body, input.Attributes)
	if err != nil {
		switch {
		case errors.Is(err, model.ErrMessageTooLarge):
			writeError(response, http.StatusRequestEntityTooLarge, "message_too_large", "Message body exceeds 256 KB limit")
		case errors.Is(err, model.ErrInvalidAttributes):
			writeError(response, http.StatusBadRequest, "invalid_attributes", "Message cannot contain more than 10 attributes")
		case errors.Is(err, model.ErrQueueFull):
			queue, _ := h.service.GetQueue(request.Context(), name)
			maxDepth := 0
			if queue != nil {
				maxDepth = queue.MaxDepth
			}
			writeError(response, http.StatusConflict, "queue_full", "Queue '"+name+"' is at max depth ("+itoa(maxDepth)+" messages)")
		case errors.Is(err, model.ErrQueueNotFound):
			writeError(response, http.StatusNotFound, "queue_not_found", "Queue '"+name+"' does not exist")
		default:
			writeError(response, http.StatusBadRequest, "invalid_message", err.Error())
		}
		return
	}
	writeJSON(response, http.StatusCreated, message)
}

func (h *QueueHandler) writeQueueLookupError(response http.ResponseWriter, name string, err error) {
	if writeContextError(response, err) {
		return
	}
	if errors.Is(err, model.ErrQueueNotFound) {
		writeError(response, http.StatusNotFound, "queue_not_found", "Queue '"+name+"' does not exist")
		return
	}
	writeError(response, http.StatusBadRequest, "invalid_queue_name", "Queue name must be 1-64 characters, alphanumeric, underscore, or hyphen")
}

func (h *QueueHandler) writeMessageOperationError(response http.ResponseWriter, name string, err error) {
	if writeContextError(response, err) {
		return
	}
	if errors.Is(err, model.ErrQueueNotFound) {
		writeError(response, http.StatusNotFound, "queue_not_found", "Queue '"+name+"' does not exist")
		return
	}
	if errors.Is(err, model.ErrQueueEmpty) {
		writeError(response, http.StatusGone, "queue_empty", "Queue '"+name+"' is empty - no messages to dequeue or peek")
		return
	}
	if errors.Is(err, model.ErrInvalidQueueName) {
		writeError(response, http.StatusBadRequest, "invalid_queue_name", "Queue name must be 1-64 characters, alphanumeric, underscore, or hyphen")
		return
	}
	writeError(response, http.StatusBadRequest, "invalid_queue_name", err.Error())
}

const maxRequestBodyBytes int64 = 320 * 1024

func writeOperationError(response http.ResponseWriter, err error) {
	if writeContextError(response, err) {
		return
	}
	logging.Errorf("operation failed: %v", err)
	writeError(response, http.StatusInternalServerError, "internal_error", "Operation failed")
}

func writeContextError(response http.ResponseWriter, err error) bool {
	switch {
	case errors.Is(err, context.Canceled):
		writeError(response, 499, "request_canceled", "Request was canceled")
		return true
	case errors.Is(err, context.DeadlineExceeded):
		writeError(response, http.StatusGatewayTimeout, "request_timeout", "Request deadline exceeded")
		return true
	default:
		return false
	}
}

func toQueueResponse(queue *model.Queue) queueResponse {
	return queueResponse{
		Name:         queue.Name,
		MaxDepth:     queue.MaxDepth,
		MessageCount: queue.CurrentMsgs,
		CreatedAt:    queue.CreatedAt.UTC().Format("2006-01-02T15:04:05Z07:00"),
	}
}

func writeJSON(response http.ResponseWriter, status int, value any) {
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(value)
}

func writeError(response http.ResponseWriter, status int, code, message string) {
	writeJSON(response, status, map[string]string{"error": code, "message": message})
}

func decodeJSON(body io.Reader, target any) error {
	decoder := json.NewDecoder(body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return errors.New("request body must contain one JSON object")
	}
	return nil
}

func itoa(value int) string {
	return fmt.Sprintf("%d", value)
}

// The following methods preserve the earlier query-string routes.
// CreateQueue handles legacy queue creation requests.
func (h *QueueHandler) CreateQueue(response http.ResponseWriter, request *http.Request) {
	h.Queues(response, request)
}

// ListQueues handles legacy queue-list requests.
func (h *QueueHandler) ListQueues(response http.ResponseWriter, request *http.Request) {
	h.listQueues(response, request)
}

// DeleteQueue handles legacy queue deletion requests.
func (h *QueueHandler) DeleteQueue(response http.ResponseWriter, request *http.Request) {
	name := request.URL.Query().Get("name")
	if err := h.service.DeleteQueue(request.Context(), name); err != nil {
		h.writeQueueLookupError(response, name, err)
		return
	}
	response.WriteHeader(http.StatusNoContent)
}

// Enqueue handles legacy message enqueue requests.
func (h *QueueHandler) Enqueue(response http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		request.Body = http.MaxBytesReader(response, request.Body, maxRequestBodyBytes)
	}
	var input messageRequest
	if request.Method == http.MethodGet {
		input.Name = request.URL.Query().Get("name")
		input.Body = request.URL.Query().Get("body")
	} else if err := json.NewDecoder(request.Body).Decode(&input); err != nil {
		writeError(response, http.StatusBadRequest, "invalid_message", "Message body is required")
		return
	}
	if input.Name == "" {
		writeError(response, http.StatusBadRequest, "invalid_request", "Queue name is required")
		return
	}
	h.enqueueInput(response, request, input.Name, input)
}

// Dequeue handles legacy message dequeue requests.
func (h *QueueHandler) Dequeue(response http.ResponseWriter, request *http.Request) {
	h.legacyMessageOperation(response, request, false)
}

// Peek handles legacy message peek requests.
func (h *QueueHandler) Peek(response http.ResponseWriter, request *http.Request) {
	h.legacyMessageOperation(response, request, true)
}

func (h *QueueHandler) legacyMessageOperation(response http.ResponseWriter, request *http.Request, peek bool) {
	name := request.URL.Query().Get("name")
	if request.Method == http.MethodPost {
		var input queueRequest
		if err := json.NewDecoder(request.Body).Decode(&input); err != nil {
			writeError(response, http.StatusBadRequest, "invalid_request", "Queue name is required")
			return
		}
		name = input.Name
	}
	var message *model.Message
	var err error
	if peek {
		message, err = h.service.Peek(request.Context(), name)
	} else {
		message, err = h.service.Dequeue(request.Context(), name)
	}
	if err != nil {
		h.writeMessageOperationError(response, name, err)
		return
	}
	writeJSON(response, http.StatusOK, message)
}
