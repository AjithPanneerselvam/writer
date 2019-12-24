package writer

const (
	maxMemTableSize = 1024
)

type memTable struct {
	//log            *bytes.Buffer
	logs           []Message
	startTimeStamp int64
	endTimeStamp   int64
}

// NewMemTable returns a new instance of memTable
func NewMemTable() memTable {
	return memTable{
		//log: bytes.NewBuffer(make([]byte, maxMemTableSize)),
		logs: make([]Message, 0),
	}
}

func (m *memTable) Append(message Message) error {
	m.logs = append(m.logs, message)
	return nil
}

/*func (m *memTable) Write(data []byte) (n int, err error) {
	n, err = m.log.Write(data)
	if err != nil {
		return n, err
	}
	//fmt.Println("memTable msg len", len(data))

	//err = m.log.WriteByte('\n')
	//fmt.Println("error in writing a new line")

	if m.startTimeStamp == 0 {
		m.startTimeStamp = time.Now().UnixNano()
	}
	m.endTimeStamp = time.Now().UnixNano()

	return n, err
}*/
