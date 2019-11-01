package writers

import (
	"encoding/json"
	"fmt"
)

// Writer ...
type IPCWriter struct {
	topics map[string]Topic
}

// Reader ...
type IPCReader struct {
	topics map[string]Topic
}

// Write writes the datapack to the topic
func (i *IPCWriter) Write(p []byte) (int, error) {
	var msg Message

	err := json.Unmarshal(p, &msg)
	if err != nil {
		return 0, err
	}

	topic, ok := i.topics[msg.Topic]
	if !ok {
		return 0, fmt.Errorf("topic with name %s doesn't exist", msg.Topic)
	}

	return topic.Write(p)
}

func (i *IPCReader) Read(p []byte) (int, error) {
	var msg Message

	err := json.Unmarshal(p, &msg)
	if err != nil {
		return 0, err
	}

	topic, ok := i.topics[msg.Topic]
	if !ok {
		return 0, fmt.Errorf("topic with name %s doesn't exist", msg.Topic)
	}

	return topic.Read(p)
}
