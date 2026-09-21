package handler

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"in-memory-queue/internal/repository"
	"in-memory-queue/internal/service"
)

func TestQueueAndMessageResourceContract(t *testing.T) {
	queueHandler := NewQueueHandler(service.NewQueueService(repository.NewMemoryRepository(), 10))

	createResponse := request(queueHandler.Queues, http.MethodPost, "/queues", `{"name":"orders","max_depth":10}`)
	if createResponse.Code != http.StatusCreated ||
		!bytes.Contains(createResponse.Body.Bytes(), []byte(`"status_code":201`)) ||
		!bytes.Contains(createResponse.Body.Bytes(), []byte(`"message":"Queue created successfully"`)) {
		t.Fatalf("expected create status 201, got %d", createResponse.Code)
	}

	listResponse := request(queueHandler.Queues, http.MethodGet, "/queues", "")
	if listResponse.Code != http.StatusOK || !bytes.Contains(listResponse.Body.Bytes(), []byte(`"queues"`)) {
		t.Fatalf("expected wrapped queue list, got %d %s", listResponse.Code, listResponse.Body.String())
	}

	enqueueResponse := request(queueHandler.QueueResource, http.MethodPost, "/queues/orders/messages", `{"body":"first","attributes":{"source":"api"}}`)
	if enqueueResponse.Code != http.StatusCreated {
		t.Fatalf("expected enqueue status 201, got %d: %s", enqueueResponse.Code, enqueueResponse.Body.String())
	}

	peekResponse := request(queueHandler.QueueResource, http.MethodGet, "/queues/orders/messages", "")
	if peekResponse.Code != http.StatusOK ||
		!bytes.Contains(peekResponse.Body.Bytes(), []byte(`"status_code":200`)) ||
		!bytes.Contains(peekResponse.Body.Bytes(), []byte(`"message":"Message retrieved successfully"`)) ||
		!bytes.Contains(peekResponse.Body.Bytes(), []byte(`"body":"first"`)) {
		t.Fatalf("expected peeked message, got %d %s", peekResponse.Code, peekResponse.Body.String())
	}

	dequeueResponse := request(queueHandler.QueueResource, http.MethodDelete, "/queues/orders/messages", "")
	if dequeueResponse.Code != http.StatusOK || !bytes.Contains(dequeueResponse.Body.Bytes(), []byte(`"body":"first"`)) {
		t.Fatalf("expected dequeued message, got %d %s", dequeueResponse.Code, dequeueResponse.Body.String())
	}

	emptyResponse := request(queueHandler.QueueResource, http.MethodGet, "/queues/orders/messages", "")
	assertError(t, emptyResponse, http.StatusGone, "queue_empty")

	missingResponse := request(queueHandler.QueueResource, http.MethodGet, "/queues/missing/messages", "")
	assertError(t, missingResponse, http.StatusNotFound, "queue_not_found")

	deleteResponse := request(queueHandler.QueueResource, http.MethodDelete, "/queues/orders", "")
	if deleteResponse.Code != http.StatusOK ||
		!bytes.Contains(deleteResponse.Body.Bytes(), []byte(`"status_code":200`)) ||
		!bytes.Contains(deleteResponse.Body.Bytes(), []byte(`"message":"Queue deleted successfully"`)) {
		t.Fatalf("expected successful delete response, got %d %s", deleteResponse.Code, deleteResponse.Body.String())
	}
}

func request(handler http.HandlerFunc, method, path, body string) *httptest.ResponseRecorder {
	recorder := httptest.NewRecorder()
	handler(recorder, httptest.NewRequest(method, path, bytes.NewBufferString(body)))
	return recorder
}

func assertError(t *testing.T, response *httptest.ResponseRecorder, status int, code string) {
	t.Helper()
	if response.Code != status {
		t.Fatalf("expected status %d, got %d: %s", status, response.Code, response.Body.String())
	}
	var payload map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if payload["error"] != code {
		t.Fatalf("expected error %q, got %q", code, payload["error"])
	}
}

func TestHandlerInvalidAndMethodErrors(t *testing.T) {
	h := NewQueueHandler(service.NewQueueService(repository.NewMemoryRepository(), 1))
	badJSON := request(h.Queues, http.MethodPost, "/queues", "{")
	if badJSON.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", badJSON.Code)
	}
	method := request(h.Queues, http.MethodDelete, "/queues", "")
	if method.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", method.Code)
	}
	missing := request(h.QueueResource, http.MethodGet, "/queues/missing", "")
	assertError(t, missing, http.StatusNotFound, "queue_not_found")
	invalid := request(h.QueueResource, http.MethodGet, "/queues/bad%20name", "")
	if invalid.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", invalid.Code)
	}
}

func TestHandlerMessageValidationAndCapacity(t *testing.T) {
	h := NewQueueHandler(service.NewQueueService(repository.NewMemoryRepository(), 1))
	request(h.Queues, http.MethodPost, "/queues", `{"name":"orders","max_depth":1}`)
	invalid := request(h.QueueResource, http.MethodPost, "/queues/orders/messages", `{"body":""}`)
	if invalid.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", invalid.Code)
	}
	request(h.QueueResource, http.MethodPost, "/queues/orders/messages", `{"body":"one"}`)
	full := request(h.QueueResource, http.MethodPost, "/queues/orders/messages", `{"body":"two"}`)
	assertError(t, full, http.StatusConflict, "queue_full")
}

func TestHandlerRejectsInvalidRequestFields(t *testing.T) {
	h := NewQueueHandler(service.NewQueueService(repository.NewMemoryRepository(), 10))

	invalidDepthType := request(h.Queues, http.MethodPost, "/queues", `{"name":"orders","max_depth":"ten"}`)
	assertError(t, invalidDepthType, http.StatusBadRequest, "invalid_request")

	invalidDepthRange := request(h.Queues, http.MethodPost, "/queues", `{"name":"orders","max_depth":0}`)
	assertError(t, invalidDepthRange, http.StatusBadRequest, "invalid_queue_depth")

	unknownField := request(h.Queues, http.MethodPost, "/queues", `{"name":"orders","unexpected":true}`)
	assertError(t, unknownField, http.StatusBadRequest, "invalid_request")
}
