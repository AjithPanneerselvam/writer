package writer

import "github.com/AjithPanneerselvam/writer/segmentfile"

type fileIndex struct {
	fileMap map[string]segmentfile.SegmentFile
}
