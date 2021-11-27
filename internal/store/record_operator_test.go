package store

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestRecord_WriteData(t *testing.T) {

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
	writeBuffer := bytes.Buffer{}
	record.WriteData(&writeBuffer)
	
}

func TestRecord_ReadData(t *testing.T) {
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
	writeBuffer := bytes.Buffer{}
	record.WriteData(&writeBuffer)

	readRecord := &Record{}
	data, err := readRecord.ReadData(&writeBuffer)
	assert.Nilf(t, err,"read data fail")
	assert.IsType(t, data,readRecord,"type error ")
	readRecord  = (data).(*Record)
	assert.Equalf(t, string(readRecord.Key.Bytes()),"hello key"," read key success")
	assert.Equalf(t, string(readRecord.Value.Bytes()),"hello value"," read key success")
	assert.Greaterf(t, len(record.Headers),0,"readHeader")
	h := record.Headers[0]
	assert.Equalf(t, h.HeaderKey, "hello header key", "read key is error ")
}
