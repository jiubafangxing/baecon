package store

import (
	"bytes"
	"github.com/jiubafangxing/baecon/internal/tools"
)


func (record *Record) WriteData(buf *bytes.Buffer) (int, error) {
//	#@int sizeInBytes = sizeOfBodyInBytes(offsetDelta, timestampDelta, key, value, headers);
//	ByteUtils.writeVarint(sizeInBytes, out);
//
//	byte attributes = 0; // there are no used record attributes at the moment
//	out.write(attributes);

	keyLen := record.Key.Len()
	tools.AppendElement(buf,keyLen)
	valueLen := record.Value.Len()
	tools.AppendElement(buf,valueLen)
	return 0,nil
}

func (record  *Record) ReadData(buf *bytes.Buffer) (interface{}, error) {
    //TODO
	return nil,nil
}



