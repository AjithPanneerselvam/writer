package writers

import (
	"fmt"
	"io"
)

type broker struct {
	topics map[string]Topic
	//producers map[string]Producer
	//consumers map[string]Consumer
}

// NewIPCWriter ...
func (b *broker) NewIPCWriter() io.Writer {
	return &IPCWriter{
		topics: b.topics,
	}
}

// NewIPCReader ...
func (b *broker) NewIPCReader() io.Reader {
	return &IPCReader{
		topics: b.topics,
	}
}

// CreateTopic creates a topic with given `name` and creates
// given number of `partitions`
func (b *broker) CreateTopic(name string, partitions int) error {
	if _, ok := b.topics[name]; ok {
		return fmt.Errorf("topic with name %s already exists", name)
	}

	var bus = make(map[PartitionNo]chan []byte)
	for i := 0; i < partitions; i++ {
		bus[PartitionNo(i)] = make(chan []byte)
	}

	topic := Topic{
		name:       name,
		partitions: partitions,
		bus:        bus,
	}

	b.topics[name] = topic

	return nil
}

// DeleteTopic deletes the topic and all of it's partitions
func (b *broker) DeleteTopic(name string) error {
	if _, ok := b.topics[name]; ok {
		delete(b.topics, name)
	}

	return nil
}
