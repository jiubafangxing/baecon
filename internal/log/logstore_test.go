package log

import (
	"bytes"
	"fmt"
	"github.com/jiubafangxing/baecon/internal/store"
	"testing"
	"time"
)

func TestBuildLogSegment(t *testing.T) {
	recordBatch := store.RecordBatch{}
	recordBatch.BatchLength = 1
	recordBatch.Magic = 2
	recordBatch.PartitionLeaderEpoch = 1
	recordBatch.Attributes = 1
	recordBatch.BaseOffset = 0
	recordBatch.BaseSequence = 12213
	recordBatch.FirstTimestamp = 212122121
	recordBatch.MaxTimestamp = 21212121
	recordBatch.LastOffsetDelta = 2
	recordBatch.ProducerId = 2
	recordBatch.ProducerEpoch = 123
	header := store.Header{}
	header.HeaderKey = "hello header key"
	header.HeaderValue = bytes.NewBufferString("hello header value")
	headers := []store.Header{}
	headers = append(headers, header)
	bufferString := bytes.NewBufferString("hello key")
	record := store.Record{
		0,
		time.Time{}.UnixNano(),
		int32(0),
		*bufferString,
		*bytes.NewBufferString("hello value"),
		headers,
	}
	recordBatch.Records = append(recordBatch.Records, record)


	_, segment := BuildLogSegment("isotopic",true,0)
	segment.write(&recordBatch)
	recordBatch.BaseOffset = 1
	segment.write(&recordBatch)
	recordBatch.BaseOffset = 2
	segment.write(&recordBatch)
	batch := segment.readRecords(int64(3))

	fmt.Println(string(batch.Records[0].Headers[0].HeaderValue.Bytes()))
}
