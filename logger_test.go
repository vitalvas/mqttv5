package mqttv5

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLogLevel(t *testing.T) {
	t.Run("string representation", func(t *testing.T) {
		assert.Equal(t, "DEBUG", LogLevelDebug.String())
		assert.Equal(t, "INFO", LogLevelInfo.String())
		assert.Equal(t, "WARN", LogLevelWarn.String())
		assert.Equal(t, "ERROR", LogLevelError.String())
		assert.Equal(t, "NONE", LogLevelNone.String())
		assert.Equal(t, "UNKNOWN", LogLevel(99).String())
	})

	t.Run("level ordering", func(t *testing.T) {
		assert.True(t, LogLevelDebug < LogLevelInfo)
		assert.True(t, LogLevelInfo < LogLevelWarn)
		assert.True(t, LogLevelWarn < LogLevelError)
		assert.True(t, LogLevelError < LogLevelNone)
	})
}

func TestNoOpLogger(t *testing.T) {
	logger := NewNoOpLogger()

	t.Run("all methods are no-ops", func(_ *testing.T) {
		logger.Debug("test", nil)
		logger.Info("test", nil)
		logger.Warn("test", nil)
		logger.Error("test", nil)
	})

	t.Run("with fields returns same logger", func(t *testing.T) {
		newLogger := logger.WithFields(LogFields{"key": "value"})
		assert.Equal(t, logger, newLogger)
	})

	t.Run("level operations", func(t *testing.T) {
		assert.Equal(t, LogLevelNone, logger.Level())

		logger.SetLevel(LogLevelDebug)
		assert.Equal(t, LogLevelDebug, logger.Level())
	})
}

func TestStdLogger(t *testing.T) {
	t.Run("debug level logs all", func(t *testing.T) {
		buf := &bytes.Buffer{}
		logger := NewStdLogger(buf, LogLevelDebug)

		logger.Debug("debug message", nil)
		logger.Info("info message", nil)
		logger.Warn("warn message", nil)
		logger.Error("error message", nil)

		output := buf.String()
		assert.Contains(t, output, "[DEBUG] debug message")
		assert.Contains(t, output, "[INFO] info message")
		assert.Contains(t, output, "[WARN] warn message")
		assert.Contains(t, output, "[ERROR] error message")
	})

	t.Run("info level skips debug", func(t *testing.T) {
		buf := &bytes.Buffer{}
		logger := NewStdLogger(buf, LogLevelInfo)

		logger.Debug("debug message", nil)
		logger.Info("info message", nil)

		output := buf.String()
		assert.NotContains(t, output, "debug message")
		assert.Contains(t, output, "info message")
	})

	t.Run("warn level skips debug and info", func(t *testing.T) {
		buf := &bytes.Buffer{}
		logger := NewStdLogger(buf, LogLevelWarn)

		logger.Debug("debug message", nil)
		logger.Info("info message", nil)
		logger.Warn("warn message", nil)

		output := buf.String()
		assert.NotContains(t, output, "debug message")
		assert.NotContains(t, output, "info message")
		assert.Contains(t, output, "warn message")
	})

	t.Run("error level only logs errors", func(t *testing.T) {
		buf := &bytes.Buffer{}
		logger := NewStdLogger(buf, LogLevelError)

		logger.Debug("debug message", nil)
		logger.Info("info message", nil)
		logger.Warn("warn message", nil)
		logger.Error("error message", nil)

		output := buf.String()
		assert.NotContains(t, output, "debug message")
		assert.NotContains(t, output, "info message")
		assert.NotContains(t, output, "warn message")
		assert.Contains(t, output, "error message")
	})

	t.Run("none level logs nothing", func(t *testing.T) {
		buf := &bytes.Buffer{}
		logger := NewStdLogger(buf, LogLevelNone)

		logger.Debug("debug message", nil)
		logger.Info("info message", nil)
		logger.Warn("warn message", nil)
		logger.Error("error message", nil)

		assert.Empty(t, buf.String())
	})

	t.Run("logs with fields", func(t *testing.T) {
		buf := &bytes.Buffer{}
		logger := NewStdLogger(buf, LogLevelDebug)

		logger.Info("message", LogFields{
			"key1": "value1",
			"key2": 42,
		})

		output := buf.String()
		assert.Contains(t, output, "message")
		assert.Contains(t, output, "key1")
		assert.Contains(t, output, "value1")
		assert.Contains(t, output, "key2")
	})

	t.Run("with fields creates new logger", func(t *testing.T) {
		buf := &bytes.Buffer{}
		logger := NewStdLogger(buf, LogLevelDebug)

		childLogger := logger.WithFields(LogFields{"client_id": "test-client"})

		childLogger.Info("child message", LogFields{"extra": "data"})

		output := buf.String()
		assert.Contains(t, output, "child message")
		assert.Contains(t, output, "client_id")
		assert.Contains(t, output, "test-client")
		assert.Contains(t, output, "extra")
	})

	t.Run("with fields preserves parent fields", func(t *testing.T) {
		buf := &bytes.Buffer{}
		logger := NewStdLogger(buf, LogLevelDebug)

		parent := logger.WithFields(LogFields{"parent": "field"})
		child := parent.WithFields(LogFields{"child": "field"})

		child.Info("message", nil)

		output := buf.String()
		assert.Contains(t, output, "parent")
		assert.Contains(t, output, "child")
	})

	t.Run("level operations", func(t *testing.T) {
		buf := &bytes.Buffer{}
		logger := NewStdLogger(buf, LogLevelInfo)

		assert.Equal(t, LogLevelInfo, logger.Level())

		logger.SetLevel(LogLevelDebug)
		assert.Equal(t, LogLevelDebug, logger.Level())
	})

	t.Run("nil writer defaults to stderr", func(t *testing.T) {
		logger := NewStdLogger(nil, LogLevelDebug)
		assert.NotNil(t, logger)
		assert.NotNil(t, logger.logger)
	})
}

func TestLogFieldConstants(t *testing.T) {
	t.Run("field names are defined", func(t *testing.T) {
		assert.Equal(t, "client_id", LogFieldClientID)
		assert.Equal(t, "topic", LogFieldTopic)
		assert.Equal(t, "packet_id", LogFieldPacketID)
		assert.Equal(t, "packet_type", LogFieldPacketType)
		assert.Equal(t, "qos", LogFieldQoS)
		assert.Equal(t, "reason_code", LogFieldReasonCode)
		assert.Equal(t, "error", LogFieldError)
		assert.Equal(t, "remote_addr", LogFieldRemoteAddr)
		assert.Equal(t, "duration", LogFieldDuration)
		assert.Equal(t, "bytes", LogFieldBytes)
	})
}

func TestLoggerInterface(t *testing.T) {
	t.Run("NoOpLogger implements Logger", func(_ *testing.T) {
		var _ Logger = NewNoOpLogger()
	})

	t.Run("StdLogger implements Logger", func(_ *testing.T) {
		var _ Logger = NewStdLogger(nil, LogLevelDebug)
	})
}

func BenchmarkNoOpLogger(b *testing.B) {
	logger := NewNoOpLogger()
	fields := LogFields{"key": "value"}

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		logger.Info("test message", fields)
	}
}

func BenchmarkStdLoggerNoFields(b *testing.B) {
	buf := &bytes.Buffer{}
	logger := NewStdLogger(buf, LogLevelDebug)

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		logger.Info("test message", nil)
	}
}

func BenchmarkStdLoggerWithFields(b *testing.B) {
	buf := &bytes.Buffer{}
	logger := NewStdLogger(buf, LogLevelDebug)
	fields := LogFields{"key": "value", "count": 42}

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		logger.Info("test message", fields)
	}
}

func BenchmarkStdLoggerFiltered(b *testing.B) {
	buf := &bytes.Buffer{}
	logger := NewStdLogger(buf, LogLevelError)
	fields := LogFields{"key": "value"}

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		logger.Debug("test message", fields)
	}
}

func BenchmarkStdLoggerWithFieldsChain(b *testing.B) {
	buf := &bytes.Buffer{}
	logger := NewStdLogger(buf, LogLevelDebug)

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		childLogger := logger.WithFields(LogFields{"client_id": "test"})
		childLogger.Info("message", nil)
	}
}

func TestLoggerRealWorldUsage(t *testing.T) {
	t.Run("connection lifecycle logging", func(t *testing.T) {
		buf := &bytes.Buffer{}
		logger := NewStdLogger(buf, LogLevelDebug)

		connLogger := logger.WithFields(LogFields{
			LogFieldClientID:   "client-123",
			LogFieldRemoteAddr: "192.168.1.100:54321",
		})

		connLogger.Info("client connected", nil)
		connLogger.Debug("processing subscribe", LogFields{LogFieldTopic: "sensors/#"})
		connLogger.Info("client disconnected", LogFields{LogFieldReasonCode: 0x00})

		output := buf.String()
		lines := strings.Split(strings.TrimSpace(output), "\n")
		assert.Len(t, lines, 3)

		assert.Contains(t, lines[0], "client connected")
		assert.Contains(t, lines[1], "processing subscribe")
		assert.Contains(t, lines[2], "client disconnected")
	})
}
