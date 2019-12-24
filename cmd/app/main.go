package main

import (
	"fmt"

	"github.com/AjithPanneerselvam/writer"
)

func main() {
	logs := []string{
		"application starting",
		"loading configs",
	}

	w := writer.NewLogWriter("/Users/ajith/go/src/github.com/AjithPanneerselvam/writer/cmd/app/logs", nil)
	for _, log := range logs {
		w.Write([]byte(log))
	}

	err := w.Close()
	fmt.Println(err)

	w.Read()
}
