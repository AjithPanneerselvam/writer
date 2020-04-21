package compactor

import (
	"github.com/AjithPanneerselvam/writer/segmentfile"
)

type compactor struct {
	unprocessedSegments []segmentfile.SegmentFile
}

func New() *compactor {
	return &compactor{
		unprocessedSegments: make([]segmentfile.SegmentFile, 0),
	}
}

func (c *compactor) Listen(ch <-chan segmentfile.SegmentFile) {
	for {
		select {
		case segmentFile := <-ch:
			c.unprocessedSegments = append(c.unprocessedSegments, segmentFile)
		default:
		}
	}

}
