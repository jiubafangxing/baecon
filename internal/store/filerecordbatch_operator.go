package store

import (
	"encoding/binary"
	"errors"
)

const (
	MAGIC_OFFSET int64= 16
	MAGIC_LENGTH int64= 1
	HEADER_SIZE_UP_TO_MAGIC int64= MAGIC_OFFSET + MAGIC_LENGTH
)



func (this *FileRecordBatchReader) nextBatch() (interface{},error){
	lackSize := this.End - this.Position
	if(lackSize < HEADER_SIZE_UP_TO_MAGIC){
		return nil, errors.New("no enough message")
	}
	readBytes := make([]byte,HEADER_SIZE_UP_TO_MAGIC )
	_, err := this.File.Read(readBytes)
	if(nil != err){
		return nil, err
	}
	offset := binary.BigEndian.Uint64(readBytes[0:8])
	size := binary.BigEndian.Uint32(readBytes[8:12])
	fileRecortBatch := &FileRecordBatch{this.File, int32(size), this.Position, int64(offset)}
	return  fileRecortBatch , nil
}

func (this *FileRecordBatch) write(recordBatch RecordBatch){
	stat, err := this.LogFile.Stat()
	if err != nil {
		return
	}
	size:= stat.Size()
	//write to the end of file
	this.LogFile.WriteAt(recordBatch.toBuffer().Bytes(), size)
}

func (this *FileRecordBatch) Close() {
	this.LogFile.Sync()
	this.LogFile.Close()
}
