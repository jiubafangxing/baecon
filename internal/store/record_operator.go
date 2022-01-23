package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"github.com/jiubafangxing/baecon/internal/tools"
	"unsafe"
)


func (record *Record) WriteData(buf *bytes.Buffer) (int64, error) {

	baseBuf := &bytes.Buffer{}
    allLength  := int64(0)
	//attribute length
    record.Attributes =  int8(0)
    attributeBuf := &bytes.Buffer{}
    attributeBuf.WriteByte(byte(record.Attributes))
	allLength += int64(unsafe.Sizeof(record.Attributes))

	//time
	timeKeyLenBuf := &bytes.Buffer{}
	timeSize := tools.AppendElement64(timeKeyLenBuf, record.TimestampDelta)
	allLength += int64(timeSize)

	//time
	offsetBuf := &bytes.Buffer{}
	offsetSize := tools.AppendElement64(offsetBuf, record.TimestampDelta)
	allLength += int64(offsetSize)

	//key length 2 varint
	keyLenBuf := &bytes.Buffer{}
	keyLen := record.Key.Len()
	allLength += int64(keyLen)

	//keyLen_len
	keyLen_len := tools.AppendElement(keyLenBuf, keyLen)
	allLength += int64(keyLen_len)

	//value length 2 varint
	valueLenBuf := &bytes.Buffer{}
	valueLen := record.Value.Len()
	allLength += int64(valueLen)

	//valueLen_len
	valueLen_len := tools.AppendElement(valueLenBuf, valueLen)
	allLength += int64(valueLen_len)

	//headerLen
	headerBuffer := &bytes.Buffer{}
    for i := 0; i< len(record.Headers); i++{
		header := record.Headers[i]
		writeHeaderSize, err := header.writeHeader(headerBuffer)
		if err != nil {
			print("write header err")
			return 0 ,err
		}
		allLength += int64(writeHeaderSize)
	}

	allLengthBuf := &bytes.Buffer{}
	tools.AppendElement64(allLengthBuf, allLength)


	contentArray := [...] bytes.Buffer {*allLengthBuf,*attributeBuf,*timeKeyLenBuf, *offsetBuf, *keyLenBuf, record.Key,  *valueLenBuf, record.Value, *headerBuffer}
	buildRecordBuffer(baseBuf, contentArray)
	buf.Write(baseBuf.Bytes())
	return int64(len(baseBuf.Bytes())),nil
}

func buildRecordBuffer(buf *bytes.Buffer, bufferArray [9]bytes.Buffer) {
	for _, buffer := range bufferArray {
		buf.Write(buffer.Bytes())
	}
 }

func (record  *Record) ReadData(buf *bytes.Buffer) (interface{}, error) {
	varint, err := binary.ReadVarint(buf)
	if(err != nil){
		print("write header err")
		return 0 ,err
	}
	print("the record length is ", varint)
	 //*allLengthBuf, *headerBuffer
	attribute, _ := binary.ReadVarint(buf)
	record.Attributes = int8(attribute)

	timeDelta, _ := binary.ReadVarint(buf)
	record.TimestampDelta = timeDelta

	offsetDelata, _ := binary.ReadVarint(buf)
	record.OffsetDelta = int32(offsetDelata)

	keyLen, _ := binary.ReadVarint(buf)
	var writeBytes []byte
	var index int64 = 0
	for index < keyLen {
		readByte, _ := buf.ReadByte()
		writeBytes = append(writeBytes, readByte)
		index++
	}
	record.Key = *bytes.NewBuffer(writeBytes)

	valueLen, _ := binary.ReadVarint(buf)
	var valueBytes []byte
	var valueIndex int64 = 0
	for valueIndex < valueLen {
		readByte, _ := buf.ReadByte()
		valueBytes = append(valueBytes, readByte)
		valueIndex++
	}
	record.Value = *bytes.NewBuffer(valueBytes)
	headerBuf := bytes.NewBuffer(buf.Bytes())
	for  {
		headerBuf = bytes.NewBuffer(headerBuf.Bytes())
		readerHeader := Header{}
		if(headerBuf.Len() == 0){
			break
		}
		headerRes,err := readerHeader.readHeader(headerBuf)
		if(errors.Is(NoHeader,err)) {
			break
		}
		record.Headers = append(record.Headers, headerRes)

	}

	return record,nil
}



