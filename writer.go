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

	"github.com/AjithPanneerselvam/writer/compactor"
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

	//SetLogTimeFormat overrides the logTimeFormat
	// default logTimeFormat is Local Time
	SetLogTimeFormat(logTimeFormat log.LogTimeFormat)

	Replay() error

	// Close closes flushes the running memtable
	// anc closes the writer
	Close() error
}

// logWriter implements the Writer interface
type logWriter struct {
	logDirectory  string
	logLevel      log.LogLevel
	logTimeFormat log.LogTimeFormat

	memtable     memtable.Memtable
	memtableSize int

	segmentFileSize int
	segmentFiles    map[string]*segmentfile.SegmentFile

	segmentFilesLock sync.RWMutex
	exclusiveLock    sync.Mutex

	flushMemtableCh chan<- memtable.Memtable
	compactionCh    chan segmentfile.SegmentFile
	closeCh         chan struct{}
	closeAckCh      chan struct{}
}

// New returns a new instance of writer
func New(logDirectory string, memtableSize int, segmentFileSize int) Writer {
	logWriter := &logWriter{
		logDirectory:  logDirectory,
		logLevel:      log.LogLevelInfo,
		logTimeFormat: log.LogTimeFormatLocalTime,

		memtable:     memtable.New(memtableSize),
		memtableSize: memtableSize,

		segmentFileSize: segmentFileSize,
		segmentFiles:    make(map[string]*segmentfile.SegmentFile, 0),

		compactionCh: make(chan segmentfile.SegmentFile),
		closeCh:      make(chan struct{}),
		closeAckCh:   make(chan struct{}),
	}

	logWriter.flushMemtableCh = logWriter.flushMemtables()

	go compactor.New().Listen(logWriter.compactionCh)

	return logWriter
}

// Write writes the data to memtable.
func (l *logWriter) Write(r io.Reader) (int, error) {
	logMessage, err := ioutil.ReadAll(r)
	if err != nil {
		return 0, err
	}

	log := log.New(logMessage, l.logLevel, l.logTimeFormat)
	logInBytes := log.Format()
	logLength := len(logInBytes)

	if l.memtable.OccupiedSize+logLength > l.memtableSize {
		l.flushMemtableCh <- l.memtable
		l.memtable = memtable.New(l.memtableSize)
	}

	l.memtable.Append(logInBytes, log.Timestamp)

	return log.Size(), nil
}

// SetLogLevel sets the log level writer the writer
func (l *logWriter) SetLogLevel(logLevel log.LogLevel) {
	l.exclusiveLock.Lock()
	l.logLevel = logLevel
	l.exclusiveLock.Unlock()
}

// SetLogTimeFormat sets the logTimeFormat of the writer
func (l *logWriter) SetLogTimeFormat(logTimeFormat log.LogTimeFormat) {
	l.exclusiveLock.Lock()
	l.logTimeFormat = logTimeFormat
	l.exclusiveLock.Unlock()
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

	err = memtable.Flush(segmentFile.Writer())
	if err != nil {
		return errors.Wrap(err, "error flushing memtable")
	}

	l.compactionCh <- *segmentFile

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

		lineReader := bufio.NewReader(f)
		for {
			b, err := lineReader.ReadBytes('\n')
			if err != nil {
				if err == io.EOF {
					break
				}
			}

			var log = new(log.Log)
			err = log.UnmarshalLogLine(b)
			if err != nil {
				return errors.Wrap(err, "error unmarshalling log line")
			}

			fmt.Println(log.String())
		}
	}

	l.segmentFilesLock.RUnlock()

	return nil
}
