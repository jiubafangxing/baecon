package store

import (
	"bytes"
	"fmt"
	"github.com/stretchr/testify/assert"
	"io/fs"
	"os"
	"testing"
	"time"
)

func TestWriteData(t *testing.T) {
	recordBatch := &RecordBatch{}
	recordBatch.BatchLength = 1
	recordBatch.Magic = 2
	recordBatch.PartitionLeaderEpoch = 1
	recordBatch.Attributes = 1
	recordBatch.BaseOffset = 2
	recordBatch.BaseSequence = 12213
	recordBatch.FirstTimestamp = 212122121
	recordBatch.MaxTimestamp = 21212121
	recordBatch.LastOffsetDelta = 2
	recordBatch.ProducerId = 2
	recordBatch.ProducerEpoch = 123
	header := Header{}
	header.HeaderKey = "hello header key"
	header.HeaderValue = bytes.NewBufferString("hello header value")
	headers := []Header{}
	headers = append(headers, header)
	bufferString := bytes.NewBufferString("hello key")
	record := Record{
		0,
		time.Time{}.UnixNano(),
		int32(0),
		*bufferString,
		*bytes.NewBufferString("hello value"),
		headers,
	}
	recordBatch.Records = append(recordBatch.Records, record)
	buf := &bytes.Buffer{}
	data, _ := recordBatch.WriteData(buf)
	assert.Greaterf(t, data, int64(0), "write bytes fail")

}

func TestReadData(t *testing.T) {
	recordBatch := &RecordBatch{}
	recordBatch.BatchLength = 1
	recordBatch.Magic = 2
	recordBatch.PartitionLeaderEpoch = 1
	recordBatch.Attributes = 1
	recordBatch.BaseOffset = 2
	recordBatch.BaseSequence = 12213
	recordBatch.FirstTimestamp = 212122121
	recordBatch.MaxTimestamp = 21212121
	recordBatch.LastOffsetDelta = 2
	recordBatch.ProducerId = 2
	recordBatch.ProducerEpoch = 123
	header := Header{}
	header.HeaderKey = "hello header key"
	header.HeaderValue = bytes.NewBufferString("hello header value")
	headers := []Header{}
	headers = append(headers, header)
	bufferString := bytes.NewBufferString("hello key")
	record := Record{
		0,
		time.Time{}.UnixNano(),
		int32(0),
		*bufferString,
		*bytes.NewBufferString("hello value"),
		headers,
	}
	recordBatch.Records = append(recordBatch.Records, record)
	buf := &bytes.Buffer{}
	recordBatch.WriteData(buf)
	readRecordBatch := &RecordBatch{}
	data, _ := readRecordBatch.ReadData(buf)

	batch := data.(*RecordBatch)
	assert.NotNil(t, batch.Records, "no records")
}

func TestRecordBatch_Write(t *testing.T) {
	fileName := "/tmp/baecon.log"
	openFile, err := os.OpenFile(fileName, os.O_RDWR, fs.ModeAppend)
	if nil == err {
		recordBatch := &RecordBatch{}
		recordBatch.BatchLength = 2
		recordBatch.Magic = 2
		recordBatch.PartitionLeaderEpoch = 2
		recordBatch.Attributes = 2
		recordBatch.BaseOffset = 2
		recordBatch.BaseSequence = 2
		recordBatch.FirstTimestamp = 2
		recordBatch.MaxTimestamp = 2
		recordBatch.LastOffsetDelta = 2
		recordBatch.ProducerId = 2
		recordBatch.ProducerEpoch = 2
		header := Header{}
		header.HeaderKey = "hello header key"
		header.HeaderValue = bytes.NewBufferString("hello header value")
		headers := []Header{}
		headers = append(headers, header)
		bufferString := bytes.NewBufferString("hello key")
		record := Record{
			0,
			time.Time{}.UnixNano(),
			int32(0),
			*bufferString,
			*bytes.NewBufferString("hello value"),
			headers,
		}
		recordBatch.Records = append(recordBatch.Records, record)
		recordBatch.Write(openFile)
		defer openFile.Close()
	}
}

func TestRecordBatch_Read(t *testing.T) {
	fileName := "/tmp/baecon.log"
	openFile, err := os.OpenFile(fileName, os.O_RDWR, fs.ModeAppend)
	stat, err := openFile.Stat()
	if err != nil {
		return
	}
	size := stat.Size()
	batch := RecordBatch{}
	read, _ := batch.Read(openFile, size)
	fmt.Println("--")
	fmt.Println(read.Records[0].Key)
	s := string(read.Records[0].Key.Bytes())
	fmt.Println(s)
}
