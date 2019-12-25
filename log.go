package writer

import (
	"bytes"
	"time"
)

const (
	newLine = '\n'
)

// Message is an abstraction of log message
type Log struct {
	TimeStamp int64
	LogLevel  string
	Message   []byte
}

// NewLog wraps the message with log formatted message
func NewLog(msg []byte) *Log {
	msg = append(msg, byte(10))

	return &Log{
		TimeStamp: time.Now().Unix(),
		LogLevel:  "info",
		Message:   msg,
	}
}

// Bytes converts Message to byte slice
func (l *Log) Bytes() []byte {
	logInBytes := new(bytes.Buffer)

	timestamp := []byte(time.Unix(l.TimeStamp, 0).String())

	logInBytes.Write(timestamp)
	logInBytes.Write([]byte(string(' ')))
	logInBytes.Write(l.Message)

	return logInBytes.Bytes()
}

func (l *Log) Size() int {
	return len(l.Bytes())
}
