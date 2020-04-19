package segmentfile

import (
	"io"
	"os"
	"path"
	"strconv"
)

type SegmentFile struct {
	Name           string
	startTimeStamp int64
	endTimeStamp   int64
	logDirectory   string
	size           int
	path           string
	out            io.ReadWriteCloser
}

func New(logDirectory string, startTimeStamp int64, size int) (*SegmentFile, error) {
	segmentFileName := strconv.FormatInt(startTimeStamp, 10)
	segmentFilePath := path.Join(logDirectory, segmentFileName)

	file, err := os.Create(segmentFilePath)
	if err != nil {
		return nil, err
	}

	return &SegmentFile{
		Name:           segmentFileName,
		logDirectory:   logDirectory,
		size:           size,
		startTimeStamp: startTimeStamp,
		out:            file,
	}, nil
}

func (s *SegmentFile) Writer() io.Writer {
	return s.out
}

func (s *SegmentFile) Reader() io.Reader {
	return s.out
}

func (s *SegmentFile) Close() error {
	return s.out.Close()
}
