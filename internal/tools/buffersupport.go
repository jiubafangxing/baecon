package tools

import (
	"bytes"
	"encoding/binary"
)

func AppendElement(buf *bytes.Buffer, headerKeyLen int) int {
	return	AppendElement64(buf, int64(headerKeyLen))
}

func AppendElement64(buf *bytes.Buffer, headerKeyLen int64) int {
	var keyArray [binary.MaxVarintLen64]byte
	varintSize := binary.PutVarint(keyArray[:], int64(headerKeyLen))
	buf.Write(keyArray[:varintSize])
	return varintSize
	
}