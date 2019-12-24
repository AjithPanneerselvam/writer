package writer

import "os"

type fileName string

type segmentFile struct {
	name           fileName
	startTimeStamp int64
	endTimeStamp   int64
	pointer        *os.File
}
