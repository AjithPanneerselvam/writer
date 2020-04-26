package writer

import (
	"fmt"
)

type Compactor interface {
	Listen(ch <-chan SegmentFile)
}

type compactor struct {
	unprocessedSegments []SegmentFile
	indexer             Indexer
}

func NewCompactor(indexer Indexer) Compactor {
	return &compactor{
		unprocessedSegments: make([]SegmentFile, 0),
		indexer:             indexer,
	}
}

func (c *compactor) Listen(ch <-chan SegmentFile) {
	for {
		select {
		case segmentFile := <-ch:
			c.unprocessedSegments = append(c.unprocessedSegments, segmentFile)

			err := c.indexer.WriteIndex(segmentFile.StartTimeStamp)
			if err != nil {
				panic(fmt.Sprintf("error writing index %v: %v", segmentFile.StartTimeStamp, err.Error()))
			}

		default:
		}
	}
}
