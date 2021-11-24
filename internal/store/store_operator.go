package store

import "bytes"

type store_operator interface {
	WriteData(buf *bytes.Buffer) (int, error)

    ReadData(buf *bytes.Buffer) (interface{}, error)
}
