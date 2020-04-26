package writer

import (
	"bytes"
	"errors"
	"fmt"
	"regexp"
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

const (
	NewLine byte = '\n'
)

// Message is an abstraction of log message
type Log struct {
	Timestamp       time.Time
	Level           LogLevel
	TimestampFormat LogTimeFormat
	Message         []byte
}

// NewLog wraps the message with log formatted message
func NewLog(msg []byte, logLevel LogLevel, timestampFormat LogTimeFormat) *Log {
	return &Log{
		Timestamp:       time.Now(),
		Level:           logLevel,
		TimestampFormat: timestampFormat,
		Message:         msg,
	}
}

// Format returns in log format - "timestamp loglevel message"
func (l *Log) Format() []byte {
	logInBytes := new(bytes.Buffer)

	timestamp := []byte(l.Timestamp.Format(string(l.TimestampFormat)))

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

// Unmarshal unmarshalls the byte slice into Log
func (l *Log) Unmarshal(b []byte) error {
	re := regexp.MustCompile(`(\[.+\])\s(.+)\s(.+)`)
	logLine := re.FindStringSubmatch(string(b))
	if len(logLine) != 4 {
		return errors.New("error matching log line")
	}

	l.TimestampFormat = LogTimeFormatLocalTime
	timestamp, err := time.Parse(string(LogTimeFormatLocalTime), logLine[1])
	if err != nil {
		timestamp, err = time.Parse(string(LogTimeFormatUTC), logLine[1])
		if err != nil {
			return err
		}

		l.TimestampFormat = LogTimeFormatUTC
	}

	l.Timestamp = timestamp

	l.Level = LogLevel(logLine[2])
	l.Message = []byte(logLine[3])

	return nil
}

func (l *Log) String() string {
	var timestamp string

	switch l.TimestampFormat {
	case LogTimeFormatLocalTime:
		timestamp = l.Timestamp.Format(string(LogTimeFormatLocalTime))

	case LogTimeFormatUTC:
		timestamp = l.Timestamp.Format(string(LogTimeFormatUTC))
	}

	log := fmt.Sprintf("%s %s %s", timestamp, l.Level, string(l.Message))

	return log
}
