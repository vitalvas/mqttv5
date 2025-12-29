package mqttv5

import (
	"io"
	"log"
	"os"
)

// LogLevel represents the logging level.
type LogLevel int

const (
	// LogLevelDebug is the debug log level.
	LogLevelDebug LogLevel = iota
	// LogLevelInfo is the info log level.
	LogLevelInfo
	// LogLevelWarn is the warn log level.
	LogLevelWarn
	// LogLevelError is the error log level.
	LogLevelError
	// LogLevelNone disables all logging.
	LogLevelNone
)

// String returns the string representation of the log level.
func (l LogLevel) String() string {
	switch l {
	case LogLevelDebug:
		return "DEBUG"
	case LogLevelInfo:
		return "INFO"
	case LogLevelWarn:
		return "WARN"
	case LogLevelError:
		return "ERROR"
	case LogLevelNone:
		return "NONE"
	default:
		return "UNKNOWN"
	}
}

// LogFields represents key-value pairs for structured logging.
type LogFields map[string]any

// Logger defines the interface for logging.
type Logger interface {
	// Debug logs a debug message.
	Debug(msg string, fields LogFields)

	// Info logs an info message.
	Info(msg string, fields LogFields)

	// Warn logs a warning message.
	Warn(msg string, fields LogFields)

	// Error logs an error message.
	Error(msg string, fields LogFields)

	// WithFields returns a new logger with the given fields added.
	WithFields(fields LogFields) Logger

	// Level returns the current log level.
	Level() LogLevel

	// SetLevel sets the log level.
	SetLevel(level LogLevel)
}

// NoOpLogger is a logger that does nothing.
type NoOpLogger struct {
	level LogLevel
}

// NewNoOpLogger creates a new no-op logger.
func NewNoOpLogger() *NoOpLogger {
	return &NoOpLogger{level: LogLevelNone}
}

// Debug does nothing.
func (n *NoOpLogger) Debug(_ string, _ LogFields) {}

// Info does nothing.
func (n *NoOpLogger) Info(_ string, _ LogFields) {}

// Warn does nothing.
func (n *NoOpLogger) Warn(_ string, _ LogFields) {}

// Error does nothing.
func (n *NoOpLogger) Error(_ string, _ LogFields) {}

// WithFields returns the same logger.
func (n *NoOpLogger) WithFields(_ LogFields) Logger {
	return n
}

// Level returns the log level.
func (n *NoOpLogger) Level() LogLevel {
	return n.level
}

// SetLevel sets the log level.
func (n *NoOpLogger) SetLevel(level LogLevel) {
	n.level = level
}

// StdLogger is a simple logger using the standard library log package.
type StdLogger struct {
	logger *log.Logger
	level  LogLevel
	fields LogFields
}

// NewStdLogger creates a new standard library based logger.
func NewStdLogger(w io.Writer, level LogLevel) *StdLogger {
	if w == nil {
		w = os.Stderr
	}
	return &StdLogger{
		logger: log.New(w, "", log.LstdFlags),
		level:  level,
		fields: make(LogFields),
	}
}

// Debug logs a debug message.
func (s *StdLogger) Debug(msg string, fields LogFields) {
	if s.level <= LogLevelDebug {
		s.log("DEBUG", msg, fields)
	}
}

// Info logs an info message.
func (s *StdLogger) Info(msg string, fields LogFields) {
	if s.level <= LogLevelInfo {
		s.log("INFO", msg, fields)
	}
}

// Warn logs a warning message.
func (s *StdLogger) Warn(msg string, fields LogFields) {
	if s.level <= LogLevelWarn {
		s.log("WARN", msg, fields)
	}
}

// Error logs an error message.
func (s *StdLogger) Error(msg string, fields LogFields) {
	if s.level <= LogLevelError {
		s.log("ERROR", msg, fields)
	}
}

// WithFields returns a new logger with the given fields added.
func (s *StdLogger) WithFields(fields LogFields) Logger {
	newFields := make(LogFields, len(s.fields)+len(fields))
	for k, v := range s.fields {
		newFields[k] = v
	}
	for k, v := range fields {
		newFields[k] = v
	}

	return &StdLogger{
		logger: s.logger,
		level:  s.level,
		fields: newFields,
	}
}

// Level returns the current log level.
func (s *StdLogger) Level() LogLevel {
	return s.level
}

// SetLevel sets the log level.
func (s *StdLogger) SetLevel(level LogLevel) {
	s.level = level
}

func (s *StdLogger) log(level, msg string, fields LogFields) {
	allFields := make(LogFields, len(s.fields)+len(fields))
	for k, v := range s.fields {
		allFields[k] = v
	}
	for k, v := range fields {
		allFields[k] = v
	}

	if len(allFields) == 0 {
		s.logger.Printf("[%s] %s", level, msg)
		return
	}

	s.logger.Printf("[%s] %s %v", level, msg, allFields)
}

// Standard field names for MQTT logging.
const (
	// LogFieldClientID is the client ID field.
	LogFieldClientID = "client_id"

	// LogFieldTopic is the topic field.
	LogFieldTopic = "topic"

	// LogFieldPacketID is the packet ID field.
	LogFieldPacketID = "packet_id"

	// LogFieldPacketType is the packet type field.
	LogFieldPacketType = "packet_type"

	// LogFieldQoS is the QoS field.
	LogFieldQoS = "qos"

	// LogFieldReasonCode is the reason code field.
	LogFieldReasonCode = "reason_code"

	// LogFieldError is the error field.
	LogFieldError = "error"

	// LogFieldRemoteAddr is the remote address field.
	LogFieldRemoteAddr = "remote_addr"

	// LogFieldDuration is the duration field.
	LogFieldDuration = "duration"

	// LogFieldBytes is the bytes field.
	LogFieldBytes = "bytes"
)
