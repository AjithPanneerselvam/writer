package log

import (
	"bytes"
	"time"
)

type LogLevel string

const (
	LogLevelTrace LogLevel = "trace"
	LogLevelDebug LogLevel = "debug"
	LogLevelInfo  LogLevel = "info"
	LogLevelWarn  LogLevel = "warn"
	LogLevelError LogLevel = "error"
	LogLevelFatal LogLevel = "fatal"
)

type LogTimeFormat string

const (
	LogTimeFormatLocalTime LogTimeFormat = "[Jan 2 2006 15:04:05]"
	LogTimeFormatUTC       LogTimeFormat = "[2006-01-02T15:04:05Z]"
)

// Message is an abstraction of log message
type Log struct {
	TimeStamp       time.Time
	Level           LogLevel
	TimestampFormat LogTimeFormat
	Message         []byte
}

// New wraps the message with log formatted message
func New(msg []byte, logLevel LogLevel, timestampFormat LogTimeFormat) *Log {
	return &Log{
		TimeStamp:       time.Now(),
		Level:           logLevel,
		TimestampFormat: timestampFormat,
		Message:         msg,
	}
}

// Format returns in log format - "timestamp loglevel message"
func (l *Log) Format() []byte {
	logInBytes := new(bytes.Buffer)

	timestamp := []byte(l.TimeStamp.Format(string(l.TimestampFormat)))

	logInBytes.Write(timestamp)
	logInBytes.Write([]byte(string(' ')))

	logInBytes.Write([]byte(l.Level))
	logInBytes.Write([]byte(string(' ')))

	logInBytes.Write(l.Message)

	return logInBytes.Bytes()
}

// Size returns the size of the log
func (l *Log) Size() int {
	return len(l.Format())
}
