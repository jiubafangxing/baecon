package common

import (
	"encoding/binary"
	"errors"
)

type MmapOperator struct {
	Mmap []byte
	writePosition int64
	validBytes int64
}

func (this *MmapOperator)putInt64(writePosition int64) error{
	if this.writePosition+8 > int64(len(this.Mmap)){
		return errors.New("no space to write")
	}
	writeBytes := this.Mmap[this.writePosition: this.writePosition+8]
	binary.PutVarint(writeBytes, writePosition)
	this.writePosition +=8
	return nil
}

// set sparse index
func (this *MmapOperator) PutIndex(offset int64, writePosition int64) error {
	if(this.validBytes - this.writePosition > INDEX_INTERVAL_BYTES){
		//writeIndex
		this.putInt64(offset)
		this.putInt64(writePosition)
	}
	this.validBytes += 16
	return nil
}




