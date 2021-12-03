package store

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestWriteData(t *testing.T) {
	recordBatch := &RecordBatch{}
	recordBatch.BatchLength=1
	recordBatch.Magic=2
	recordBatch.PartitionLeaderEpoch=1
	recordBatch.Attributes=1
	recordBatch.BaseOffset=2
	recordBatch.BaseSequence=12213
	recordBatch.FirstTimestamp=212122121
	recordBatch.MaxTimestamp=21212121
	recordBatch.LastOffsetDelta=2
	recordBatch.ProducerId=2
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
	assert.Greaterf(t, data,int64(0),"write bytes fail")

}

func TestReadData(t *testing.T) {
	recordBatch := &RecordBatch{}
	recordBatch.BatchLength=1
	recordBatch.Magic=2
	recordBatch.PartitionLeaderEpoch=1
	recordBatch.Attributes=1
	recordBatch.BaseOffset=2
	recordBatch.BaseSequence=12213
	recordBatch.FirstTimestamp=212122121
	recordBatch.MaxTimestamp=21212121
	recordBatch.LastOffsetDelta=2
	recordBatch.ProducerId=2
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
	assert.NotNil(t, batch.Records,"no records")
}
