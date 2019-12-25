package writer

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strconv"
)

const (
	maxMemTableSize = 1024
)

// Writer provides an interface to write logs to desired target store
type Writer interface {
	// Write reads from r and writes it to out
	Write(r io.Reader) (n int, err error)

	Close() error

	Replay() error
}

// FileLog implements the log reader and writer interface
type logWriter struct {
	logDirectory        string
	out                 io.Writer
	fileIndex           fileIndex
	memTable            memTable
	flushMemTableOnSize int
	segmentFiles        map[string]segmentFile
}

// NewLogWriter returns a new file writer
func NewLogWriter(logDirectory string, out io.Writer) Writer {
	return &logWriter{
		logDirectory:        logDirectory,
		out:                 out,
		memTable:            NewMemTable(),
		flushMemTableOnSize: maxMemTableSize,
		segmentFiles:        make(map[string]segmentFile, 0),
	}
}

// Write writes the data to log file
func (f *logWriter) Write(r io.Reader) (int, error) {
	logMessage, err := ioutil.ReadAll(r)
	if err != nil {
		return 0, err
	}

	log := NewLog(logMessage)
	logLength := log.Size()

	if f.memTable.size+logLength > f.flushMemTableOnSize {
		go f.flushMemTable()
		f.memTable = NewMemTable()
	}

	f.memTable.Append(*log)

	return log.Size(), nil
}

// Close closes the file writer by flushing the uncommitted logs if any
func (f *logWriter) Close() error {
	return f.flushMemTable()
}

func (l *logWriter) flushMemTable() error {
	segmentFileName := strconv.FormatInt(l.memTable.startTimeStamp, 10)
	segmentFilePath := path.Join(l.logDirectory, segmentFileName)

	file, err := os.Create(segmentFilePath)
	if err != nil {
		return err
	}
	defer file.Close()

	err = l.memTable.flush(file)
	if err != nil {
		return err
	}

	segmentFile := segmentFile{
		name:           segmentFileName,
		startTimeStamp: l.memTable.startTimeStamp,
		endTimeStamp:   l.memTable.endTimeStamp,
		path:           segmentFilePath,
	}
	l.segmentFiles[segmentFileName] = segmentFile

	return nil
}

// Replay replays the logs from the given timestamp ...
func (f *logWriter) Replay() error {
	// TODO: Implementation incomplete
	for segmentFileName, segmentFile := range f.segmentFiles {
		fmt.Println("segmentFileName", segmentFileName)

		f, err := os.Open(path.Join(f.logDirectory, string(segmentFile.name)))
		if err != nil {
			return err
		}

		defer f.Close()

		fileScanner := bufio.NewScanner(f)
		fileScanner.Split(bufio.ScanLines)

		for fileScanner.Scan() {
			fmt.Println(fileScanner.Text())
		}
	}

	return nil
}
