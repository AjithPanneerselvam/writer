package writer

import (
	"fmt"
	"time"

	"github.com/vmihailenco/msgpack"
)

// Message is an abstraction of log message
type Message struct {
	TimeStamp int64
	LogLevel  string
	Message   []byte
}

// Marshal wraps the message with log formatted message
func Marshal(message []byte) *Message {
	return &Message{
		TimeStamp: time.Now().UnixNano(),
		// placeholder for now
		LogLevel: "info",
		Message:  message,
	}
}

// Bytes converts Message to byte slice
func (m *Message) Bytes() ([]byte, error) {
	bytesBuffer, err := msgpack.Marshal(m)
	if err != nil {
		return nil, err
	}
	//fmt.Println("msgpack len", len(bytesBuffer))
	fmt.Println("message", bytesBuffer, len(bytesBuffer))

	var decode Message
	err = msgpack.Unmarshal(bytesBuffer, &decode)
	if err != nil {
		fmt.Println("Unmarshal error", err.Error())
	}
	//fmt.Println("decode", decode)

	//err := json.Unmarshal(bytesBuffer, m)
	//err := binary.Write(&bytesBuffer, binary.BigEndian, *m)
	/*if err != nil {
		return nil, err
	}
	fmt.Println("message bytes buffer", bytesBuffer.Bytes())*/

	//return bytesBuffer.Bytes(), nil
	return bytesBuffer, nil
}
