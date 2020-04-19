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

	"github.com/AjithPanneerselvam/writer/log"
	"github.com/AjithPanneerselvam/writer/memtable"
	"github.com/AjithPanneerselvam/writer/segmentfile"
	"github.com/pkg/errors"
)

// Writer provides an interface to write logs to desired target store
type Writer interface {
	// Write reads from r and writes it to out
	Write(r io.Reader) (n int, err error)

	//SetLogLevel sets the given logLevel
	SetLogLevel(logLevel log.LogLevel)

	Replay() error

	// Close closes flushes the running memtable
	// anc closes the writer
	Close() error
}

// logWriter implements the Writer interface
type logWriter struct {
	logDirectory string
	logLevel     log.LogLevel

	fileIndex fileIndex

	memtable     memtable.Memtable
	memtableSize int

	segmentFileSize int
	segmentFiles    map[string]*segmentfile.SegmentFile

	segmentFilesLock sync.RWMutex
	logLevelLock     sync.Mutex

	flushMemtableCh chan<- memtable.Memtable
	closeCh         chan struct{}
	closeAckCh      chan struct{}
}

// New returns a new instance of writer
func New(logDirectory string, memtableSize int, segmentFileSize int) Writer {
	logWriter := &logWriter{
		logDirectory:    logDirectory,
		logLevel:        log.LogLevelInfo,
		memtable:        memtable.New(memtableSize),
		memtableSize:    memtableSize,
		segmentFileSize: segmentFileSize,
		segmentFiles:    make(map[string]*segmentfile.SegmentFile, 0),
		closeCh:         make(chan struct{}),
		closeAckCh:      make(chan struct{}),
	}

	logWriter.flushMemtableCh = logWriter.flushMemtables()

	return logWriter
}

// Write writes the data to memtable.
func (l *logWriter) Write(r io.Reader) (int, error) {
	logMessage, err := ioutil.ReadAll(r)
	if err != nil {
		return 0, err
	}

	log := log.New(logMessage, l.logLevel)
	logLength := log.Size()

	if l.memtable.OccupiedSize+logLength > l.memtableSize {
		l.flushMemtableCh <- l.memtable
		l.memtable = memtable.New(l.memtableSize)
	}

	l.memtable.Append(*log)

	return log.Size(), nil
}

// SetLogLevel sets the log level for the writer
func (l *logWriter) SetLogLevel(logLevel log.LogLevel) {
	l.logLevelLock.Lock()
	l.logLevel = logLevel
	l.logLevelLock.Unlock()
}

func (l *logWriter) flushMemtables() chan<- memtable.Memtable {
	var flushMemtableCh = make(chan memtable.Memtable)

	go func() {
		var memtableQueue = list.New()

		for {
			select {
			case memtable := <-flushMemtableCh:
				memtableQueue.PushBack(memtable)

			case <-l.closeCh:
				memtableQueue.PushBack(l.memtable)
				err := l.flushMemtable(memtableQueue)
				if err != nil {
					panic(err)
				}

				l.closeAckCh <- struct{}{}
				return

			default:
				if memtableQueue.Len() == 0 {
					continue
				}

				err := l.flushMemtable(memtableQueue)
				if err != nil {
					panic(err)
				}
			}
		}

	}()

	return flushMemtableCh
}

func (l *logWriter) flushMemtable(memtableQueue *list.List) error {
	if memtableQueue.Len() == 0 {
		return nil
	}

	element := memtableQueue.Front()
	memtable, ok := element.Value.(memtable.Memtable)
	if !ok {
		return fmt.Errorf("invalid memtable value")
	}

	segmentFile, err := segmentfile.New(l.logDirectory, memtable.StartTimeStamp, l.segmentFileSize)
	if err != nil {
		return errors.Wrap(err, "error creating new segment file")
	}

	l.segmentFilesLock.Lock()
	l.segmentFiles[segmentFile.Name] = segmentFile
	l.segmentFilesLock.Unlock()

	segmentFileWriter := segmentFile.Writer()
	err = memtable.Flush(segmentFileWriter)
	if err != nil {
		return errors.Wrap(err, "error flushing memtable")
	}

	err = segmentFile.Close()
	if err != nil {
		return errors.Wrap(err, "error closing segment file")
	}

	memtableQueue.Remove(memtableQueue.Front())

	return nil
}

// Close closes the file writer by flushing the uncommitted logs if any
func (l *logWriter) Close() error {
	l.closeCh <- struct{}{}
	<-l.closeAckCh
	return nil
}

// Replay replays the logs from the given timestamp
func (l *logWriter) Replay() error {
	l.segmentFilesLock.RLock()
	for segmentFileName, _ := range l.segmentFiles {
		f, err := os.Open(path.Join(l.logDirectory, segmentFileName))
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
	l.segmentFilesLock.RUnlock()

	return nil
}
