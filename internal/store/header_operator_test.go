package store

import (
	"bytes"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestWriteHeader(t *testing.T) {
	var buffer *bytes.Buffer = nil
	header := Header{}
	_, err := header.WriteHeader(buffer)
	assert.NotNilf(t, err, "set buffer is null want err fail")

	header.HeaderKey = "hello header key"
	header.HeaderValue = bytes.NewBufferString("hello header value")
	buffer = new(bytes.Buffer)
	writeHeaderSize, err := header.WriteHeader(buffer)
	assert.Nil(t, err, "set buffer not null want err nil but failed")
	assert.Greaterf(t, writeHeaderSize, 0, "has not be writed into buffer ")
	assert.Greaterf(t, buffer.Len(), 0, "write not success")

}

func TestReadHeader(t *testing.T) {
	var buffer *bytes.Buffer = nil
	header := Header{}
	header.HeaderKey = "hello header key"
	header.HeaderValue = bytes.NewBufferString("hello header value")
	buffer = new(bytes.Buffer)
	writeHeaderSize, err := header.WriteHeader(buffer)
	assert.Nil(t, err, "set buffer not null want err nil but failed")
	assert.Greaterf(t, writeHeaderSize, 0, "has not be writed into buffer ")
	assert.Greaterf(t, buffer.Len(), 0, "write not success")

	readerHeader := Header{}
	readerHeader, _ = readerHeader.ReadHeader(buffer)
	assert.NotNilf(t, readerHeader.HeaderKey, "read key is null failed")
	assert.Equalf(t, readerHeader.HeaderKey, "hello header key", "read key is error ")
	assert.NotNilf(t, readerHeader.HeaderValue, "value should not nil")
	assert.Equalf(t, readerHeader.HeaderValue.Len(), header.HeaderValue.Len(), "read value failed")
	fmt.Println(readerHeader.HeaderValue)
	return
}
