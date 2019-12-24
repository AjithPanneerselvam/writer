package writer

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path"

	"github.com/vmihailenco/msgpack"
)

// FileLog implements the log reader and writer interface
type FileLog struct {
	logDirectory      string
	out               io.Writer
	fileIndex         fileIndex
	memTable          memTable
	memTableOccupancy int
	segmentFiles      map[string]segmentFile
}

// Write writes the data to log file
func (f *FileLog) Write(data []byte) (n int, err error) {
	//fmt.Println("message", Marshal(data))
	message := Marshal(data)

	messageBytes, err := message.Bytes()
	if err != nil {
		fmt.Println(err)
		return 0, err
	}
	n = len(messageBytes)
	fmt.Println("message len", n)

	if f.memTableOccupancy+n+1 > maxMemTableSize {
		go f.flushMemTable()
		f.memTable = NewMemTable()
	}

	f.memTable.Append(*message)
	return n, nil
}

// flushMemTable does batch write to disk
func (f *FileLog) flushMemTable() error {
	//fName := strconv.FormatInt(f.memTable.startTimeStamp, 10)
	fName := "sample"
	filePath := path.Join(f.logDirectory, fName)
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	w := bufio.NewWriter(file)
	for _, log := range f.memTable.logs {
		/*logBytes, err := log.Bytes()
		if err != nil {
			fmt.Println("error in encoding", err.Error())
			return err
		}*/
		//fmt.Println("logBytes", logBytes)

		fmt.Fprintln(w, log)
	}
	w.Flush()

	//_, err = f.memTable.log.WriteTo(file)
	/*n, err := io.Copy(file, f.memTable.log)
	if err != nil {
		return err
	}
	fmt.Printf("\ncopied %v bytes to log\n", n)*/

	segmentFile := segmentFile{
		name:           fileName(fName),
		startTimeStamp: f.memTable.startTimeStamp,
		endTimeStamp:   f.memTable.endTimeStamp,
		pointer:        file,
	}

	f.segmentFiles[fName] = segmentFile
	return nil
}

// Close closes the file writer by flushing the uncommitted logs if any
func (f *FileLog) Close() error {
	return f.flushMemTable()
}

// Read ...
func (f *FileLog) Read() {
	// TODO: Implementation incomplete
	for segmentFileName, segmentFile := range f.segmentFiles {
		fmt.Println("segmentFileName", segmentFileName)

		f, err := os.Open(path.Join(f.logDirectory, string(segmentFile.name)))
		defer f.Close()

		fmt.Println(err)

		fileScanner := bufio.NewScanner(f)
		fileScanner.Split(bufio.ScanLines)

		for fileScanner.Scan() {
			fmt.Println("message", fileScanner.Bytes(), len(fileScanner.Text()))

			var m Message
			b := fileScanner.Text()
			fmt.Println("b len", len(b))
			err := msgpack.Unmarshal([]byte(fileScanner.Text()), &m)
			if err != nil {
				fmt.Println("msgpack unmarshal error", err)
			}
		}
		/*scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			var m Message
			//fmt.Println("msg", scanner.Bytes())
			fmt.Println("msg text", scanner.Text())
			fmt.Println("msg length", len(scanner.Bytes()))
			err := msgpack.Unmarshal([]byte(scanner.Text()), &m)
			if err != nil {
				fmt.Println("msgpack unmarshal error", err)
			}
			//err := binary.Read(bytes.NewBuffer(scanner.Bytes()), binary.BigEndian, &m)
			fmt.Println("msgpack", m)
		}

		if err := scanner.Err(); err != nil {
			log.Fatal(err)
		}*/
	}
}
