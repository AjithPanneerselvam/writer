package writers

import (
	"fmt"

	"github.com/labstack/gommon/log"
)

// PartitionNo ...
type PartitionNo int

// Topic is an abstraction of topic
type Topic struct {
	name       string
	partitions int
	bus        map[PartitionNo]chan []byte
}

func (t *Topic) Write(p []byte) (int, error) {
	if t.partitions < 1 {
		return 0, fmt.Errorf("error as number of partitions for the topic should be at least 1")
	}

	defaultPartitionChannel := t.bus[0]

	log.Infof("writing message to partition channel")
	defaultPartitionChannel <- p
	log.Infof("message written to partition channel")

	return len(p), nil
}

func (t *Topic) Read(p []byte) (int, error) {
	return 0, nil
}
