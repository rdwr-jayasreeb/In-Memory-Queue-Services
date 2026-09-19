# In-Memory Queue

An in-memory HTTP queue service written in Go.

## Run

```bash
go run ./cmd/server
```

The server listens on `:8080` by default. Use `PORT`, `MAX_QUEUE_DEPTH`, `MAX_MESSAGE_BODY_SIZE`, and `MAX_ATTRIBUTES` to configure it.

Defaults:

- Port: `8080`
- Maximum queue depth: `10000`
- Maximum message body: `262144` bytes
- Maximum attributes per message: `10`

## Structure

- `cmd`: application entrypoint and configuration
- `internal/model`: queue and message domain types
- `internal/repository`: in-memory queue persistence
- `internal/service`: queue operations and validation
- `internal/handler`: HTTP request handlers