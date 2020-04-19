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

// Message is an abstraction of log message
type Log struct {
	TimeStamp time.Time
	LogLevel  LogLevel
	Message   []byte
}

// New wraps the message with log formatted message
func New(msg []byte, logLevel LogLevel) *Log {
	return &Log{
		TimeStamp: time.Now(),
		LogLevel:  logLevel,
		Message:   msg,
	}
}

// Format returns in log format - "timestamp loglevel message"
func (l *Log) Format() []byte {
	logInBytes := new(bytes.Buffer)

	timestamp := []byte(l.TimeStamp.Format("Jan 2 2006 15:04:05"))

	logInBytes.Write(timestamp)
	logInBytes.Write([]byte(string(' ')))

	logInBytes.Write([]byte(l.LogLevel))
	logInBytes.Write([]byte(string(' ')))

	logInBytes.Write(l.Message)

	return logInBytes.Bytes()
}

// Size returns the size of the log
func (l *Log) Size() int {
	return len(l.Format())
}
