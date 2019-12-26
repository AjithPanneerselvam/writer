package writer

import (
	"io"
	"os"
	"path"
	"strconv"
)

type segmentFile struct {
	name           string
	startTimeStamp int64
	endTimeStamp   int64
	logDirectory   string
	size           int
	path           string
	out            io.ReadWriteCloser
}

func NewSegmentFile(logDirectory string, size int, startTimeStamp int64) (*segmentFile, error) {
	segmentFileName := strconv.FormatInt(startTimeStamp, 10)
	segmentFilePath := path.Join(logDirectory, segmentFileName)

	file, err := os.Create(segmentFilePath)
	if err != nil {
		return nil, err
	}

	return &segmentFile{
		name:           segmentFileName,
		logDirectory:   logDirectory,
		size:           size,
		startTimeStamp: startTimeStamp,
		out:            file,
	}, nil
}

func (s *segmentFile) Writer() io.Writer {
	return s.out
}

func (s *segmentFile) Reader() io.Reader {
	return s.out
}

func (s *segmentFile) Close() error {
	return s.out.Close()
}
