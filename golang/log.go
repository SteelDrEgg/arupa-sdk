package arupa

import (
	"context"
	"fmt"
	"strings"
)

// LogLevel is a severity accepted by the Arupa host log API.
type LogLevel string

const (
	LogDebug LogLevel = "debug"
	LogInfo  LogLevel = "info"
	LogWarn  LogLevel = "warn"
	LogError LogLevel = "error"
)

// NormalizeLogLevel converts a user-supplied level to the canonical host
// spelling. "warning" is accepted as an alias for "warn".
func NormalizeLogLevel(level LogLevel) (LogLevel, error) {
	switch strings.ToLower(strings.TrimSpace(string(level))) {
	case "debug":
		return LogDebug, nil
	case "", "info":
		return LogInfo, nil
	case "warn", "warning":
		return LogWarn, nil
	case "error":
		return LogError, nil
	default:
		return "", fmt.Errorf("arupa: unsupported log level %q", level)
	}
}

// Logger sends service log records to the host. The host authenticates the
// caller and adds its registered service name to every record.
type Logger interface {
	Log(context.Context, LogLevel, string) error
}
