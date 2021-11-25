package store

import "bytes"

type store_operator interface {

	WriteData(buf *bytes.Buffer) (int64, error)

    ReadData(buf *bytes.Buffer) (interface{}, error)
}
