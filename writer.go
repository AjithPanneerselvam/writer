package writer

import (
	"io"
)

// Writer provides an interface to write logs to desired target store
type Writer interface {
	Write(data []byte) (n int, err error)

	Close() error

	Read()
}

// NewLogWriter returns a new file writer
func NewLogWriter(logDirectory string, out io.Writer) Writer {
	return &FileLog{
		logDirectory: logDirectory,
		out:          out,
		memTable:     NewMemTable(),
		segmentFiles: make(map[string]segmentFile, 0),
	}
}
