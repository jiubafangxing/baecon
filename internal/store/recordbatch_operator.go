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
		print("write record for ", i)
		writeData, _ := record.WriteData(tmpRecordBuf)
		length += writeData
	}

	buf := &bytes.Buffer{}

	length = length+ 61
	//BaseOffset
	data := int64(recordBatch.BaseOffset)
	binary.Write(buf, binary.BigEndian, data)

	//BatchLength
	BatchLengthData := int32(length)
	binary.Write(buf, binary.BigEndian, BatchLengthData)
	//PartitionLeaderEpoch
	PartitionLeaderEpochData := int32(recordBatch.PartitionLeaderEpoch)
	binary.Write(buf, binary.BigEndian, PartitionLeaderEpochData)
	//Magic
	MagicData := byte(recordBatch.Magic)
	binary.Write(buf, binary.BigEndian, MagicData)
	//Crc
	CrcData := int32(recordBatch.Crc)
	binary.Write(buf, binary.BigEndian, CrcData)
	//Attributes
	AttributesData := int16(recordBatch.Attributes)
	binary.Write(buf, binary.BigEndian, AttributesData)
	//LastOffsetDelta
	LastOffsetDeltaData := int32(recordBatch.LastOffsetDelta)
	binary.Write(buf, binary.BigEndian, LastOffsetDeltaData)
	//FirstTimestamp
	FirstTimestampData := int64(recordBatch.FirstTimestamp)
	binary.Write(buf, binary.BigEndian, FirstTimestampData)
	//MaxTimestamp
	MaxTimestampData := int64(recordBatch.MaxTimestamp)
	binary.Write(buf, binary.BigEndian, MaxTimestampData)
	//ProducerId
	ProducerIdData := int64(recordBatch.ProducerId)
	binary.Write(buf, binary.BigEndian, ProducerIdData)
	//ProducerEpochcerId
	ProducerEpochData := int16(recordBatch.ProducerEpoch)
	binary.Write(buf, binary.BigEndian, ProducerEpochData)
	//BaseSequenceData
	BaseSequenceData := int32(recordBatch.BaseSequence)
	binary.Write(buf, binary.BigEndian, BaseSequenceData)
	//records count
	recordLen := int32(	len(recordBatch.Records))
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
	last += reflect.TypeOf(recordBatch.Magic).Len()
	binary.Read(bytes.NewBuffer(array[head:last]),binary.BigEndian,&recordBatch.Magic)

	head = last
	last += reflect.TypeOf(recordBatch.Crc).Len()
	binary.Read(bytes.NewBuffer(array[head:last]),binary.BigEndian,&recordBatch.Crc)

	head = last
	last += reflect.TypeOf(recordBatch.Attributes).Len()
	binary.Read(bytes.NewBuffer(array[head:last]),binary.BigEndian,&recordBatch.Attributes)

	head = last
	last += reflect.TypeOf(recordBatch.LastOffsetDelta).Len()
	binary.Read(bytes.NewBuffer(array[head:last]),binary.BigEndian,&recordBatch.LastOffsetDelta)

	head = last
	last += reflect.TypeOf(recordBatch.FirstTimestamp).Len()
	binary.Read(bytes.NewBuffer(array[head:last]),binary.BigEndian,&recordBatch.FirstTimestamp)

	head = last
	last += reflect.TypeOf(recordBatch.MaxTimestamp).Len()
	binary.Read(bytes.NewBuffer(array[head:last]),binary.BigEndian,&recordBatch.MaxTimestamp)

	head = last
	last += reflect.TypeOf(recordBatch.ProducerId).Len()
	binary.Read(bytes.NewBuffer(array[head:last]),binary.BigEndian,&recordBatch.ProducerId)

	head = last
	last += reflect.TypeOf(recordBatch.ProducerEpoch).Len()
	binary.Read(bytes.NewBuffer(array[head:last]),binary.BigEndian,&recordBatch.ProducerEpoch)

	head = last
	last += reflect.TypeOf(recordBatch.BaseSequence).Len()
	binary.Read(bytes.NewBuffer(array[head:last]),binary.BigEndian,&recordBatch.BaseSequence)

	head = last
	recordBuf :=bytes.NewBuffer(array[head:])
	for  {
		record := Record{}
		recordRes,err := record.ReadData(recordBuf)
		if(errors.Is(NoHeader,err)){
			break
		}else{
			recordTmp :=recordRes.(Record)
			recordBatch.Records = append(recordBatch.Records, recordTmp)
		}
	}
	return recordBatch, nil
}

