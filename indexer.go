package writer

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"sync"
)

const (
	indexFileName = "index"
)

type Indexer interface {
	WriteIndex(index int64) error
	IndexLen() int
}

type indexer struct {
	indexFile *os.File
	indexes   []int64
	lock      sync.RWMutex
}

func NewIndexer(logDirectoryPath string) (Indexer, error) {
	var indexFile *os.File

	indexPath := path.Join(logDirectoryPath, indexFileName)
	_, err := os.Stat(indexPath)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}

		indexFile, err = os.Create(indexPath)
		if err != nil {
			return nil, err
		}
	}

	indexFile, err = os.OpenFile(indexPath, os.O_APPEND|os.O_RDWR, os.ModeAppend)
	if err != nil {
		return nil, err
	}

	indexes, err := loadIndex(indexFile)
	if err != nil {
		return nil, err
	}

	return &indexer{
		indexFile: indexFile,
		indexes:   indexes,
	}, nil
}

func loadIndex(indexFile *os.File) ([]int64, error) {
	var indexes = make([]int64, 0)

	lineReader := bufio.NewReader(indexFile)
	for {
		b, err := lineReader.ReadBytes(NewLine)
		if err != nil {
			if err == io.EOF {
				break
			}
		}

		index, err := strconv.ParseInt(string(b[:len(b)-1]), 10, 64)
		if err != nil {
			return nil, err
		}

		indexes = append(indexes, index)
	}
	fmt.Println(indexes)

	return indexes, nil
}

func (i *indexer) WriteIndex(index int64) error {
	i.lock.Lock()
	defer i.lock.Unlock()

	i.indexes = append(i.indexes, index)
	_, err := i.indexFile.WriteString(fmt.Sprintf("%d\n", index))

	return err
}

func (i *indexer) IndexLen() int {
	return len(i.indexes)
}
