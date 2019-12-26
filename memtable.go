package writer

import (
	"fmt"
	"io"
)

type memtable struct {
	logs           []Log
	startTimeStamp int64
	endTimeStamp   int64
	occupiedSize   int
	totalSize      int
}

// NewMemTable returns a new instance of memtable
func NewMemtable(totalSize int) memtable {
	return memtable{
		logs:         make([]Log, 0),
		occupiedSize: 0,
		totalSize:    totalSize,
	}
}

func (m *memtable) Append(log Log) error {
	if len(m.logs) == 0 {
		m.startTimeStamp = log.TimeStamp
		fmt.Println("memtable TimeStamp", log.TimeStamp)
	}

	m.logs = append(m.logs, log)
	m.occupiedSize += log.Size() + 1

	return nil
}

// flush flushes the logs to disk
func (m *memtable) flush(w io.Writer) error {
	for _, log := range m.logs {
		w.Write(log.Bytes())
	}

	return nil
}
