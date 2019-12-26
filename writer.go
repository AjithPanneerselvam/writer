package writer

import (
	"bufio"
	"container/list"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sync"
)

// Writer provides an interface to write logs to desired target store
type Writer interface {
	// Write reads from r and writes it to out
	Write(r io.Reader) (n int, err error)

	Close() error

	Replay() error
}

// logWriter implements the log reader and writer interface
type logWriter struct {
	logDirectory     string
	fileIndex        fileIndex
	memtable         memtable
	memtableSize     int
	segmentFileSize  int
	segmentFiles     map[string]*segmentFile
	segmentFilesLock sync.RWMutex
	flushMemtableCh  chan<- memtable
	closeCh          chan struct{}
}

// New returns a new log writer
func New(logDirectory string, memtableSize int, segmentFileSize int) Writer {
	logWriter := &logWriter{
		logDirectory:    logDirectory,
		memtable:        NewMemtable(memtableSize),
		memtableSize:    memtableSize,
		segmentFileSize: segmentFileSize,
		segmentFiles:    make(map[string]*segmentFile, 0),
		closeCh:         make(chan struct{}),
	}

	logWriter.flushMemtableCh = logWriter.flushMemtables()

	return logWriter
}

// Write writes the data to memtable or segment file
func (l *logWriter) Write(r io.Reader) (int, error) {
	logMessage, err := ioutil.ReadAll(r)
	if err != nil {
		return 0, err
	}

	log := NewLog(logMessage)
	logLength := log.Size()

	if l.memtable.occupiedSize+logLength > l.memtableSize {
		l.flushMemtableCh <- l.memtable
		l.memtable = NewMemtable(l.memtableSize)
	}

	l.memtable.Append(*log)

	return log.Size(), nil
}

func (l *logWriter) flushMemtables() chan<- memtable {
	// TODO: Handle the error instead of panicking

	var flushMemtableCh = make(chan memtable)

	go func() {
		var flushQueue = list.New()
		var stop bool

	loop:
		for {
			select {
			case memtable := <-flushMemtableCh:
				flushQueue.PushBack(memtable)
			case <-l.closeCh:
				stop = true
			default:
				if flushQueue.Len() == 0 {
					if stop {
						break loop
					}
					continue
				}

				element := flushQueue.Front()
				memtable, ok := element.Value.(memtable)
				if !ok {
					panic("invalid value")
				}

				segmentFile, err := NewSegmentFile(l.logDirectory, l.segmentFileSize, memtable.startTimeStamp)
				if err != nil {
					panic(err)
				}

				l.segmentFilesLock.Lock()
				l.segmentFiles[segmentFile.name] = segmentFile
				l.segmentFilesLock.Unlock()

				segmentFileWriter := segmentFile.Writer()
				if err != nil {
					panic(err)
				}

				err = memtable.flush(segmentFileWriter)
				if err != nil {
					panic(err)
				}

				err = segmentFile.Close()
				if err != nil {
					panic(err)
				}

				flushQueue.Remove(flushQueue.Front())
			}
		}

		l.closeCh <- struct{}{}
	}()

	return flushMemtableCh
}

// Close closes the file writer by flushing the uncommitted logs if any
func (l *logWriter) Close() error {
	l.closeCh <- struct{}{}
	<-l.closeCh
	return nil
}

// Replay replays the logs from the given timestamp ...
func (l *logWriter) Replay() error {
	// TODO: Incomplete

	l.segmentFilesLock.RLock()
	for segmentFileName, _ := range l.segmentFiles {
		f, err := os.Open(path.Join(l.logDirectory, segmentFileName))
		if err != nil {
			return err
		}
		defer f.Close()
		fmt.Println("segmentFile", segmentFileName)

		fileScanner := bufio.NewScanner(f)
		fileScanner.Split(bufio.ScanLines)

		for fileScanner.Scan() {
			fmt.Println(fileScanner.Text())
		}
	}
	l.segmentFilesLock.RUnlock()

	return nil
}
