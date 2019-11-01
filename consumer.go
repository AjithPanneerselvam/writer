package writers

import "io"

type consumer struct {
	groupID string
	reader  io.Reader
	topics  []Topic
}

func (c *consumer) Subscribe(topic string) {

}
