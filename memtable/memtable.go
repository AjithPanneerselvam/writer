package memtable

import (
	"bytes"
	"io"

	"github.com/AjithPanneerselvam/writer/log"
)

type Memtable struct {
	Logs           []log.Log
	StartTimeStamp int64
	OccupiedSize   int
	TotalSize      int
}

// New returns a new instance of Memtable
func New(totalSize int) Memtable {
	return Memtable{
		Logs:         make([]log.Log, 0),
		OccupiedSize: 0,
		TotalSize:    totalSize,
	}
}

// Append appends the log in the running memtable
func (m *Memtable) Append(log log.Log) error {
	if len(m.Logs) == 0 {
		// Using Nano() because when the log throughput is seemingly high
		// it will lead to overwriting segment file
		m.StartTimeStamp = log.TimeStamp.UnixNano()
	}

	m.Logs = append(m.Logs, log)
	// +1 for new line
	m.OccupiedSize += log.Size() + 1

	return nil
}

// Flush flushes the logs from memtable to segmentfile
func (m *Memtable) Flush(w io.Writer) error {
	var b = new(bytes.Buffer)

	for _, log := range m.Logs {
		_, err := b.Write(log.Format())
		if err != nil {
			return err
		}

		err = b.WriteByte('\n')
		if err != nil {
			return err
		}
	}

	_, err := w.Write(b.Bytes())
	return err
}
