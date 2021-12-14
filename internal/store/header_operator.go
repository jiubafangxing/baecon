package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"github.com/jiubafangxing/baecon/internal/tools"
	"log"
)


func (header *Header)WriteData(buf *bytes.Buffer) (int, error){
	return header.writeHeader(buf)
}

func (header *Header)ReadData(buf *bytes.Buffer) (interface{}, error){
	return header.readHeader(buf)
}

func (header *Header) writeHeader(buf *bytes.Buffer) (int, error) {
	//check the buf
	log.SetPrefix("header")
	if nil == buf {
		log.Println("buffer is nil ,can't write ")
		return 0, errors.New("buffer is nil ")
	}

	var writeSize int = 0

	//write key len
	buf.Reset()
	headerKeyLen := len(header.HeaderKey)
	writeSize += tools.AppendElement(buf, headerKeyLen)

	//write key
	if headerKeyLen != 0 {
		write, _ := buf.Write([]byte(header.HeaderKey))
		writeSize += write
	}

	//write value len
	valueLen := header.HeaderValue.Len()
	writeSize += tools.AppendElement(buf, valueLen)

	//write value
	if valueLen != 0 {
		write, _ := buf.Write(header.HeaderValue.Bytes())
		writeSize += write
	}

	return writeSize, nil
}

func (header *Header) readHeader(buf *bytes.Buffer) (Header, error) {

	var defaultHeader Header
	if(len(buf.Bytes())==0 ){
		return defaultHeader ,NoHeader
	}

	keySize, _ := binary.ReadVarint(buf)
	var index int64 = 0

	var writeBytes []byte
	for index < keySize {
		readByte, _ := buf.ReadByte()
		writeBytes = append(writeBytes, readByte)
		index++
	}
	//writeBuf.ReadRune()
	key := string(writeBytes)

	valSize, _ := binary.ReadVarint(buf)
	var valIndex int64 = 0

	valWriteBuf := new(bytes.Buffer)
	for valIndex < valSize {
		readByte, _ := buf.ReadByte()
		valWriteBuf.WriteByte(readByte)
		valIndex++
	}

	head := Header{
		key,
		valWriteBuf,
	}

	return head, nil

}




