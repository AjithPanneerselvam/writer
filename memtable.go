package writer

import "os"

type memTable struct {
	logs           []Log
	startTimeStamp int64
	endTimeStamp   int64
	size           int
}

// NewMemTable returns a new instance of memTable
func NewMemTable() memTable {
	return memTable{
		logs: make([]Log, 0),
		size: 0,
	}
}

func (m *memTable) Append(log Log) error {
	if len(m.logs) == 0 {
		m.startTimeStamp = log.TimeStamp
	}

	m.logs = append(m.logs, log)
	m.size += log.Size() + 1

	return nil
}

// flush flushes the logs to disk
func (m *memTable) flush(w *os.File) error {
	for _, log := range m.logs {
		w.Write(log.Bytes())
	}

	return nil
}
