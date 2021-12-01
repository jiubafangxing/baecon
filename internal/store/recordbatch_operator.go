package store

import (
	"bytes"
	"encoding/binary"
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
	data := int64(recordBatch.BatchLength)
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

	oribuf.Write(buf.Bytes())
	return int64(len(buf.Bytes())),nil
}


func (recordBatch *RecordBatch)ReadData(buf *bytes.Buffer) (interface{}, error){
	//baseOffsetBuf := []byte{}
	//_, err := buf.Read(baseOffsetBuf)
	//if(nil != err){
	//	return nil, err
	//}
	//reader := bytes.NewReader(buf.Bytes())


	return nil, nil
}

