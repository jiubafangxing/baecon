package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"reflect"
)

func (recordBatch *RecordBatch) WriteData(oribuf *bytes.Buffer) (int64, error) {

	tmpRecordBuf := &bytes.Buffer{}
	length := int64(0)
	for i, record := range recordBatch.Records {
		print("\r\n","write record for ", i)
		writeData, _ := record.WriteData(tmpRecordBuf)
		length += writeData
	}

	buf := &bytes.Buffer{}

	length = length+ 61
	//BaseOffset
	data := int64(recordBatch.BaseOffset)
	print("\r\n","baseoffset ", recordBatch.BaseOffset)
	binary.Write(buf, binary.BigEndian, data)

	//BatchLength
	BatchLengthData := int32(length)
	print("\r\n","length ", BatchLengthData)
	binary.Write(buf, binary.BigEndian, BatchLengthData)
	//PartitionLeaderEpoch
	PartitionLeaderEpochData := int32(recordBatch.PartitionLeaderEpoch)
	print("\r\n","PartitionLeaderEpochData ", PartitionLeaderEpochData)
	binary.Write(buf, binary.BigEndian, PartitionLeaderEpochData)
	//Magic
	MagicData := byte(recordBatch.Magic)
	print("\r\n","MagicData ", MagicData)
	binary.Write(buf, binary.BigEndian, MagicData)
	//Crc
	CrcData := int32(recordBatch.Crc)
	print("\r\n","CrcData ", CrcData)
	binary.Write(buf, binary.BigEndian, CrcData)
	//Attributes
	AttributesData := int16(recordBatch.Attributes)
	print("\r\n","AttributesData ", AttributesData)
	binary.Write(buf, binary.BigEndian, AttributesData)
	//LastOffsetDelta
	LastOffsetDeltaData := int32(recordBatch.LastOffsetDelta)
	print("\r\n","LastOffsetDeltaData ", LastOffsetDeltaData)
	binary.Write(buf, binary.BigEndian, LastOffsetDeltaData)
	//FirstTimestamp
	FirstTimestampData := int64(recordBatch.FirstTimestamp)
	print("\r\n","FirstTimestampData ", FirstTimestampData)
	binary.Write(buf, binary.BigEndian, FirstTimestampData)
	//MaxTimestamp
	MaxTimestampData := int64(recordBatch.MaxTimestamp)
	print("\r\n","MaxTimestampData ", MaxTimestampData)
	binary.Write(buf, binary.BigEndian, MaxTimestampData)
	//ProducerId
	ProducerIdData := int64(recordBatch.ProducerId)
	print("\r\n","ProducerIdData ", ProducerIdData)
	binary.Write(buf, binary.BigEndian, ProducerIdData)
	//ProducerEpochcerId
	ProducerEpochData := int16(recordBatch.ProducerEpoch)
	print("\r\n","ProducerEpochData ", ProducerEpochData)
	binary.Write(buf, binary.BigEndian, ProducerEpochData)
	//BaseSequenceData
	BaseSequenceData := int32(recordBatch.BaseSequence)
	print("\r\n","BaseSequenceData ", BaseSequenceData)
	binary.Write(buf, binary.BigEndian, BaseSequenceData)
	//records count
	recordLen := int32(	len(recordBatch.Records))
	print("\r\n","Records len ", recordLen)
	binary.Write(buf, binary.BigEndian, recordLen)
	resultBytes := buf.Bytes()

	buf.Write(tmpRecordBuf.Bytes())

	oribuf.Write(buf.Bytes())
	return int64(len(resultBytes)),nil
}


func (recordBatch *RecordBatch)ReadData(buf *bytes.Buffer) (interface{}, error){
	array2 := make([]byte, 12)
	buf.Read(array2)
	var length int32
	binary.Read(bytes.NewBuffer(array2[8:12]) ,binary.BigEndian, &length)




	var leftOverLen = length - 12
	array := make([]byte, leftOverLen)
	binary.Read(buf,binary.BigEndian,array)

	head := 0
	last := 4
	binary.Read(bytes.NewBuffer(array[head:last]),binary.BigEndian,&recordBatch.PartitionLeaderEpoch)

	head = last
	last += int(reflect.TypeOf(recordBatch.Magic).Size())
	print("size is ",last-head)
	binary.Read(bytes.NewBuffer(array[head:last]),binary.BigEndian,&recordBatch.Magic)

	head = last
	last += int(reflect.TypeOf(recordBatch.Crc).Size())
	print("size is ",last-head)
	binary.Read(bytes.NewBuffer(array[head:last]),binary.BigEndian,&recordBatch.Crc)

	head = last
	last += int(reflect.TypeOf(recordBatch.Attributes).Size())
	print("size is ",last-head)
	binary.Read(bytes.NewBuffer(array[head:last]),binary.BigEndian,&recordBatch.Attributes)

	head = last
	last += int(reflect.TypeOf(recordBatch.LastOffsetDelta).Size())
	print("size is ",last-head)
	binary.Read(bytes.NewBuffer(array[head:last]),binary.BigEndian,&recordBatch.LastOffsetDelta)

	head = last
	last += int(reflect.TypeOf(recordBatch.FirstTimestamp).Size())
	print("size is ",last-head)
	binary.Read(bytes.NewBuffer(array[head:last]),binary.BigEndian,&recordBatch.FirstTimestamp)

	head = last
	last += int(reflect.TypeOf(recordBatch.MaxTimestamp).Size())
	print("size is ",last-head)
	binary.Read(bytes.NewBuffer(array[head:last]),binary.BigEndian,&recordBatch.MaxTimestamp)

	head = last
	last += int(reflect.TypeOf(recordBatch.ProducerId).Size())
	print("size is ",last-head)
	binary.Read(bytes.NewBuffer(array[head:last]),binary.BigEndian,&recordBatch.ProducerId)

	head = last
	last += int(reflect.TypeOf(recordBatch.ProducerEpoch).Size())
	print("size is ",last-head)
	binary.Read(bytes.NewBuffer(array[head:last]),binary.BigEndian,&recordBatch.ProducerEpoch)

	head = last
	last += int(reflect.TypeOf(recordBatch.BaseSequence).Size())
	print("size is ",last-head)
	binary.Read(bytes.NewBuffer(array[head:last]),binary.BigEndian,&recordBatch.BaseSequence)

	recordSize := 0
	head = last
	last += 4
	print("size is ",last-head)
	binary.Read(bytes.NewBuffer(array[head:last]),binary.BigEndian,recordSize)

	head = last
	recordBuf :=bytes.NewBuffer(array[head:])
	for  {
		record := Record{}
		lackLen := recordBuf.Len()
		if(lackLen ==0){
			break
		}
		recordRes,err := record.ReadData(recordBuf)
		if(errors.Is(NoHeader,err)){
			break
		}else{
			recordTmp :=recordRes.(*Record)
			recordBatch.Records = append(recordBatch.Records, *recordTmp)
		}
	}
	return recordBatch, nil
}

