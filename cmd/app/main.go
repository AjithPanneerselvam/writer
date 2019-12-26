package main

import (
	"strings"

	"github.com/AjithPanneerselvam/writer"
)

func main() {
	logs := []string{
		"starting service",
		"loading configs",
		"service running",
		"stopping service",
	}

	w := writer.New("/Users/ajith/go/src/github.com/AjithPanneerselvam/writer/cmd/app/logs", 1024, 2048)
	for i := 0; i < 50; i++ {
		for _, log := range logs {
			w.Write(strings.NewReader(log))
		}
	}

	err := w.Close()
	if err != nil {
		panic(err)
	}

	w.Replay()
}
