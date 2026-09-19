// Package logging provides level-based application logging.
package logging

import (
	"errors"
	"fmt"
	"log"
	"strings"
)

// Level controls which log messages are emitted.
type Level int

const (
	// Debug logs diagnostic messages.
	Debug Level = iota
	// Info logs informational messages.
	Info
	// Warn logs warning messages.
	Warn
	// Error logs error messages.
	Error
)

var configuredLevel = Info

// Configure sets the minimum log level from a DEBUG, INFO, WARN, or ERROR value.
func Configure(value string) error {
	level, ok := parseLevel(value)
	if !ok {
		return errors.New("log level must be DEBUG, INFO, WARN, or ERROR")
	}
	configuredLevel = level
	return nil
}

// ValidateLevel reports whether value is a supported log level.
func ValidateLevel(value string) error {
	if _, ok := parseLevel(value); !ok {
		return errors.New("log level must be DEBUG, INFO, WARN, or ERROR")
	}
	return nil
}

// Debugf logs a formatted diagnostic message.
func Debugf(format string, arguments ...any) {
	printf(Debug, format, arguments...)
}

// Infof logs a formatted informational message.
func Infof(format string, arguments ...any) {
	printf(Info, format, arguments...)
}

// Warnf logs a formatted warning message.
func Warnf(format string, arguments ...any) {
	printf(Warn, format, arguments...)
}

// Errorf logs a formatted error message.
func Errorf(format string, arguments ...any) {
	printf(Error, format, arguments...)
}

func printf(level Level, format string, arguments ...any) {
	if level < configuredLevel {
		return
	}
	log.Printf("level=%s msg=%s", levelName(level), fmt.Sprintf(format, arguments...))
}

func parseLevel(value string) (Level, bool) {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case "DEBUG":
		return Debug, true
	case "INFO":
		return Info, true
	case "WARN":
		return Warn, true
	case "ERROR":
		return Error, true
	default:
		return Info, false
	}
}

func levelName(level Level) string {
	switch level {
	case Debug:
		return "DEBUG"
	case Warn:
		return "WARN"
	case Error:
		return "ERROR"
	default:
		return "INFO"
	}
}
