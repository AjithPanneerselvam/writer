package main

import (
	writers "github.com/AjithPanneerselvam/writers"
)

func main() {
	writer := writers.New()

	err := writer.CreateTopic("foo", 1)
	if err != nil {
		panic(err)
	}

	ipcWriter := writer.NewIPCWriter()
	producer := writer.NewProducer("writer", ipcWriter)

	var msg = writers.Message{
		Topic: "foo",
		Data:  []byte{5, 4, 2, 3, 4, 1},
	}

	err = producer.Publish(msg)
	if err != nil {
		panic(err)
	}
}
