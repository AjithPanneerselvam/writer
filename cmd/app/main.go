package main

import (
	"strings"

	"github.com/AjithPanneerselvam/writer"
	"github.com/AjithPanneerselvam/writer/log"
)

func main() {
	logs := []string{
		"starting service",
		"loading configs",
		"running",
		"stopping service",
	}

	w := writer.New("/home/ajith/go/src/github.com/AjithPanneerselvam/writer/cmd/app/logs", 1024, 2048)

	for i := 0; i < 50; i++ {
		if i%2 == 1 {
			w.SetLogLevel(log.LogLevelDebug)
			w.SetLogTimeFormat(log.LogTimeFormatLocalTime)
		} else {
			w.SetLogLevel(log.LogLevelInfo)
			w.SetLogTimeFormat(log.LogTimeFormatUTC)
		}

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
