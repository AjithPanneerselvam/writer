package writer

import (
	"io"
	"os"
	"path"
	"strconv"
)

const (
	segmentFileExtension = ".segment"
)

type SegmentFile struct {
	Name           string
	StartTimeStamp int64
	LogDirectory   string
	OccupiedSize   int
	Size           int
	Path           string
	Out            io.ReadWriteCloser
}

func NewSegmentFile(logDirectory string, startTimeStamp int64, size int) (*SegmentFile, error) {
	segmentFileName := strconv.FormatInt(startTimeStamp, 10) + segmentFileExtension
	segmentFilePath := path.Join(logDirectory, segmentFileName)

	file, err := os.Create(segmentFilePath)
	if err != nil {
		return nil, err
	}

	return &SegmentFile{
		Name:           segmentFileName,
		LogDirectory:   logDirectory,
		Size:           size,
		StartTimeStamp: startTimeStamp,
		Out:            file,
	}, nil
}

func (s *SegmentFile) Writer() io.Writer {
	return s.Out
}

func (s *SegmentFile) Reader() io.Reader {
	return s.Out
}

func (s *SegmentFile) Close() error {
	return s.Out.Close()
}
