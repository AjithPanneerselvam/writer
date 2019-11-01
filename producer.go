package writers

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/pkg/errors"
)

type producer struct {
	name   string
	writer io.Writer
}

func (p *producer) Publish(msg Message) error {
	var msgBytes []byte

	msgBytes, err := json.Marshal(msg)
	if err != nil {
		return errors.Wrap(err, "error encoding message")
	}

	n, err := p.writer.Write(msgBytes)
	if err != nil {
		return err
	}

	if n != len(msgBytes) {
		return fmt.Errorf("error failing to write message")
	}

	return nil
}
