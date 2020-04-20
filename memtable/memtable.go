package memtable

import (
	"bytes"
	"io"
	"time"
)

type Memtable struct {
	Logs           bytes.Buffer
	StartTimeStamp int64
	OccupiedSize   int
	TotalSize      int
}

// New returns a new instance of Memtable
func New(totalSize int) Memtable {
	return Memtable{
		OccupiedSize: 0,
		TotalSize:    totalSize,
	}
}

// Append appends the log in the running memtable
func (m *Memtable) Append(log []byte, timestamp time.Time) error {
	if len(m.Logs.Bytes()) == 0 {
		// Using Nano() because when the log throughput is seemingly high
		// it will lead to overwriting segment file
		m.StartTimeStamp = timestamp.UnixNano()
	}

	m.Logs.Write(log)
	m.Logs.WriteByte('\n')

	// +1 for new line
	m.OccupiedSize += len(log) + 1

	return nil
}

// Flush flushes the logs from memtable to segmentfile
func (m *Memtable) Flush(w io.Writer) error {
	_, err := w.Write(m.Logs.Bytes())
	return err
}
