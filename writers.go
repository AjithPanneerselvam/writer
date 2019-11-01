package writers

import "io"

// Broker ...
type Broker interface {
	CreateTopic(name string, partitions int) error
	NewIPCWriter() io.Writer
	NewIPCReader() io.Reader
}

// Producer ...
type Producer interface {
	Publish(msg Message) error
}

// Consumer ...
type Consumer interface {
}

// Writer ...
type Writer interface {
	Broker
	NewProducer(name string, publishTo io.Writer) Producer
	NewConsumer(name string, subscribeFrom io.Reader) Consumer
}

type writer struct {
	Broker
}

// New returns a new instance of current
func New() Writer {
	return &writer{
		newBroker(),
	}
}

func (w *writer) NewProducer(name string, publishTo io.Writer) Producer {
	return &producer{
		name:   name,
		writer: publishTo,
	}
}

func (w *writer) NewConsumer(groupID string, reader io.Reader) Consumer {
	return &consumer{
		groupID: groupID,
		reader:  reader,
	}
}

func newBroker() Broker {
	return &broker{
		topics: make(map[string]Topic),
		//producers: make(map[string]Producer),
		//consumers: make(map[string]Consumer),
	}
}

// Message is the abstract type which will flow through the bus
type Message struct {
	Topic string
	Data  []byte
}
