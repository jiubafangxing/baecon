package store

import (
	"bytes"
	"os"
)

//type TRecordBatch interface {
//	type FileRecordBatch
//}

type RecordBatchReader interface {
	nextBatch() (*FileRecordBatch, error)
}

type RecordBatch struct {
	BaseOffset           int64
	BatchLength          int32
	PartitionLeaderEpoch int32
	Magic                int8
	Crc                  int32
	Attributes           int16
	LastOffsetDelta      int32
	FirstTimestamp       int64
	MaxTimestamp         int64
	ProducerId           int64
	ProducerEpoch        int16
	BaseSequence         int32
	Records              []Record
}

type FileRecordBatch struct {
	LogFile *os.File
	Length int32
	StartPosition int64
	Offset int64
}



type Record struct {
	Attributes     int8
	TimestampDelta int64
	OffsetDelta    int32
	Key            bytes.Buffer
	Value          bytes.Buffer
	Headers        []Header
}

type Header struct {
	HeaderKey   string
	HeaderValue *bytes.Buffer
}

type FileRecordBatchReader struct {
	File *os.File
	Position int64
	End int64
}

