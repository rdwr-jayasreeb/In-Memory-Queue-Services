package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"in-memory-queue/internal/handler"
	"in-memory-queue/internal/logging"
	"in-memory-queue/internal/repository"
	"in-memory-queue/internal/service"
)

type config struct {
	listenAddress     string
	defaultQueueDepth int
	maxMessageSize    int
	maxAttributes     int
	logLevel          string
	readTimeout       time.Duration
	writeTimeout      time.Duration
	idleTimeout       time.Duration
	shutdownTimeout   time.Duration
}

func loadConfig() (config, error) {
	port, err := getEnvPort("PORT", 8080)
	if err != nil {
		return config{}, err
	}

	cfg := config{
		listenAddress:     getEnv("LISTEN_ADDRESS", fmt.Sprintf(":%d", port)),
		defaultQueueDepth: getEnvInt("MAX_QUEUE_DEPTH", 10000),
		maxMessageSize:    getEnvInt("MAX_MESSAGE_BODY_SIZE", 256*1024),
		maxAttributes:     getEnvInt("MAX_ATTRIBUTES", 10),
		logLevel:          getEnv("LOG_LEVEL", "INFO"),
		readTimeout:       getEnvDuration("READ_TIMEOUT", 15*time.Second),
		writeTimeout:      getEnvDuration("WRITE_TIMEOUT", 15*time.Second),
		idleTimeout:       getEnvDuration("IDLE_TIMEOUT", 60*time.Second),
		shutdownTimeout:   getEnvDuration("SHUTDOWN_TIMEOUT", 10*time.Second),
	}

	flag.StringVar(
		&cfg.listenAddress,
		"listen",
		cfg.listenAddress,
		"HTTP listen address",
	)

	flag.IntVar(
		&cfg.defaultQueueDepth,
		"queue-depth",
		cfg.defaultQueueDepth,
		"Default max queue depth",
	)

	flag.IntVar(
		&cfg.maxMessageSize,
		"max-msg-size",
		cfg.maxMessageSize,
		"Max message size",
	)

	flag.IntVar(
		&cfg.maxAttributes,
		"max-attributes",
		cfg.maxAttributes,
		"Maximum message attributes",
	)

	flag.StringVar(
		&cfg.logLevel,
		"log-level",
		cfg.logLevel,
		"Log level",
	)

	flag.DurationVar(
		&cfg.readTimeout,
		"read-timeout",
		cfg.readTimeout,
		"HTTP read timeout",
	)

	flag.DurationVar(
		&cfg.writeTimeout,
		"write-timeout",
		cfg.writeTimeout,
		"HTTP write timeout",
	)

	flag.DurationVar(
		&cfg.idleTimeout,
		"idle-timeout",
		cfg.idleTimeout,
		"HTTP idle timeout",
	)

	flag.DurationVar(
		&cfg.shutdownTimeout,
		"shutdown-timeout",
		cfg.shutdownTimeout,
		"Graceful shutdown timeout",
	)

	flag.Parse()

	// Validate all configuration values before starting the server.
	if err := validateConfig(cfg); err != nil {
		return config{}, err
	}

	return cfg, nil
}

func validateConfig(cfg config) error {
	// Validate listen address and port.
	if err := validateListenAddress(cfg.listenAddress); err != nil {
		return err
	}

	// Queue depth must be positive.
	if cfg.defaultQueueDepth <= 0 {
		return errors.New("queue-depth must be greater than 0")
	}

	// Message size must be positive.
	if cfg.maxMessageSize <= 0 {
		return errors.New("max-msg-size must be greater than 0")
	}

	// Project requirement: maximum message body size is 256 KB.
	if cfg.maxMessageSize > 256*1024 {
		return errors.New(
			"max-msg-size cannot exceed 262144 bytes (256 KB)",
		)
	}

	// Number of attributes must be positive.
	if cfg.maxAttributes <= 0 {
		return errors.New("max-attributes must be greater than 0")
	}

	// Project requirement: maximum 10 attributes.
	if cfg.maxAttributes > 10 {
		return errors.New("max-attributes cannot exceed 10")
	}

	// Validate log level.
	if err := logging.ValidateLevel(cfg.logLevel); err != nil {
		return err
	}

	// Validate HTTP timeouts.
	if cfg.readTimeout <= 0 {
		return errors.New("read-timeout must be greater than 0")
	}

	if cfg.writeTimeout <= 0 {
		return errors.New("write-timeout must be greater than 0")
	}

	if cfg.idleTimeout <= 0 {
		return errors.New("idle-timeout must be greater than 0")
	}

	if cfg.shutdownTimeout <= 0 {
		return errors.New("shutdown-timeout must be greater than 0")
	}

	return nil
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}

	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	value, err := strconv.Atoi(os.Getenv(key))

	if err != nil || value <= 0 {
		return defaultValue
	}

	return value
}

func getEnvPort(key string, defaultValue int) (int, error) {
	value := os.Getenv(key)

	if value == "" {
		return defaultValue, nil
	}

	port, err := strconv.Atoi(value)

	if err != nil || port < 1 || port > 65535 {
		return 0, fmt.Errorf(
			"%s must be a number between 1 and 65535",
			key,
		)
	}

	return port, nil
}

func validateListenAddress(address string) error {
	_, portValue, err := net.SplitHostPort(address)

	if err != nil {
		return fmt.Errorf(
			"listen address must be in host:port format: %w",
			err,
		)
	}

	port, err := strconv.Atoi(portValue)

	if err != nil || port < 1 || port > 65535 {
		return errors.New(
			"listen address port must be a number between 1 and 65535",
		)
	}

	return nil
}

func getEnvDuration(
	key string,
	defaultValue time.Duration,
) time.Duration {
	value, err := time.ParseDuration(os.Getenv(key))

	if err != nil || value <= 0 {
		return defaultValue
	}

	return value
}

func main() {
	// Load and validate configuration.
	cfg, err := loadConfig()
	if err != nil {
		log.Printf("[ERROR] invalid configuration: %v", err)
		return
	}

	// Configure logging.
	if err := logging.Configure(cfg.logLevel); err != nil {
		log.Printf("[ERROR] invalid log configuration: %v", err)
		return
	}

	logging.Infof(
		"log level configured: %s",
		cfg.logLevel,
	)

	// Create repository.
	repo := repository.NewMemoryRepository()

	// Create service.
	queueService := service.NewQueueService(
		repo,
		cfg.defaultQueueDepth,
		cfg.maxMessageSize,
		cfg.maxAttributes,
	)

	// Create handler.
	queueHandler := handler.NewQueueHandler(queueService)

	// Create HTTP router.
	mux := http.NewServeMux()

	mux.HandleFunc("/queues", queueHandler.Queues)

	mux.HandleFunc("/queues/", queueHandler.QueueResource)

	mux.HandleFunc("/queues/list", queueHandler.ListQueues)

	mux.HandleFunc("/queues/delete", queueHandler.DeleteQueue)

	mux.HandleFunc("/queues/enqueue", queueHandler.Enqueue)

	mux.HandleFunc("/queues/dequeue", queueHandler.Dequeue)

	mux.HandleFunc("/queues/peek", queueHandler.Peek)

	// Create HTTP server.
	server := &http.Server{
		Addr:           cfg.listenAddress,
		Handler:        mux,
		ReadTimeout:    cfg.readTimeout,
		WriteTimeout:   cfg.writeTimeout,
		IdleTimeout:    cfg.idleTimeout,
		MaxHeaderBytes: 1 << 20,
	}

	// Start listening.
	listener, err := net.Listen(
		"tcp",
		cfg.listenAddress,
	)

	if err != nil {
		log.Printf(
			"[ERROR] server startup failed: %v",
			err,
		)
		return
	}

	log.Printf(
		"[INFO] server started successfully on %s",
		listener.Addr(),
	)

	// Channel used to receive server errors.
	serverErr := make(chan error, 1)

	go func() {
		serverErr <- server.Serve(listener)
	}()

	// Listen for SIGINT and SIGTERM.
	signalCtx, stop := signal.NotifyContext(
		context.Background(),
		syscall.SIGINT,
		syscall.SIGTERM,
	)
	defer stop()

	select {
	case err := <-serverErr:
		if err != nil && err != http.ErrServerClosed {
			log.Printf(
				"[ERROR] server failed: %v",
				err,
			)
			return
		}

	case <-signalCtx.Done():
		log.Printf(
			"[INFO] server shutdown signal received: %v",
			signalCtx.Err(),
		)
	}

	// Graceful shutdown.
	shutdownCtx, cancel := context.WithTimeout(
		context.Background(),
		cfg.shutdownTimeout,
	)
	defer cancel()

	log.Printf("[INFO] server shutting down...")

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf(
			"[ERROR] server shutdown failed: %v",
			err,
		)
		return
	}

	log.Printf("[INFO] server shutdown completed")
}
