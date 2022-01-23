package log

import (
	"encoding/binary"
	"fmt"
	"github.com/edsrzf/mmap-go"
	"github.com/jiubafangxing/baecon/internal/common"
	"github.com/jiubafangxing/baecon/internal/store"
	"io/ioutil"
	"os"
	"strings"
)

const (
	OFFSET_INDEX_SIZE = 16
	LOG_SUFFIX        = "log"
	INDEX_SUFFIX      = "index"
	DOT               = "."
)

type LogSegment struct {
	LogFile *LogFile
	//Sparse index based on relative offset and physical position
	OffsetIndex *OffsetIndex

	SegmentName string

	LastEndOffset int64
}

func (this *LogSegment) readRecords(offset int64)(*store.RecordBatch) {
	batch, err := this.targetBatch(uint64(offset), 0)
	if(nil != err){
		fmt.Println(err)
	}
	return batch
}

func BuildLogSegment(segmentPath string, active bool, logEndOffset int64) (error, *LogSegment) {
	exists, _ := common.PathExists(common.BASE_DIR + segmentPath)
	if !exists {
		createrr := os.MkdirAll(segmentPath, 0766)
		if createrr != nil {
			print(createrr)
		}
	}
	if _, err := os.Stat(common.BASE_DIR); os.IsNotExist(err) {
		os.MkdirAll(common.BASE_DIR, 0700)
	}
	fileList, _ := ioutil.ReadDir(common.BASE_DIR + segmentPath)
	var files []*os.File
	var mmapBytes []byte
	var indexFile *os.File
	var logFile *os.File
	if len(fileList) >= 0 {
		var indexFileNames []string = []string{segmentPath, DOT, INDEX_SUFFIX}
		var logFileNames []string = []string{segmentPath, DOT, LOG_SUFFIX}
		indexFileName := strings.Join(indexFileNames, "")
		logFileName := strings.Join(logFileNames, "")
		tmpIndexFile, err :=os.OpenFile(common.BASE_DIR + indexFileName, os.O_RDWR|os.O_CREATE, 0666)
		if nil != err {
			print("create index fail")
			fmt.Println(err)
			return nil, nil
		}
		tmpLogFile, err :=os.OpenFile(common.BASE_DIR + logFileName, os.O_RDWR|os.O_CREATE, 0666)
		if nil != err {
			print("create log fail")
			fmt.Println(err)
			return nil, nil
		}
		//indexFileInfo, _ := indexFile.Stat()
		//logFileInfo, _ := logFile.Stat()
		files = append(files, tmpLogFile)
		files = append(files, tmpIndexFile)
	}
	for _, fileItem := range files {
		//offset index
		if strings.HasSuffix(fileItem.Name(), "index") {
			stat, err := fileItem.Stat()
			if err != nil {
				return err, nil
			}
			if(stat.Size() == 0 ){
				fileItem.Truncate(common.INDEX_INTERVAL_BYTES)
			}
			mmap, err := mmap.Map(fileItem, mmap.RDWR, 0)
			if nil != err {
				return err, nil
			}
			mmapBytes = mmap
			indexFile = fileItem
		}
		//logfile
		if strings.HasSuffix(fileItem.Name(), "log") {
			logFile = fileItem
		}
	}
	m := common.MmapOperator{}
	m.Mmap = mmapBytes
	logFileItem := &LogFile{logFile}
	index := &OffsetIndex{
		indexFile,
		m,
		0,
	}
	// if the log segment is active , we need to load last index offset to the end of the logfile ,
	// then we can get the lastEndOffset
	logOffset := &LogSegment{
		logFileItem,
		index,
		segmentPath,
		logEndOffset,
	}
	if active {
		offset, err := logOffset.LastIndexOffset()
		if err != nil {
			return err, nil
		}
		logOffset.LastEndOffset = offset

	}
	return nil, logOffset
}

type LogFile struct {
	storeFile *os.File
}

type OffsetIndex struct {
	storeFile *os.File
	//mmapBytes *[]byte
	Operator common.MmapOperator

	endOffset int
}

//Entries 识别该index中存在多少个索引
func (this *OffsetIndex) Entries() int {
	if(this.endOffset != 0){
		return this.endOffset
	}
	tmpEndOffset := 0
	for i := 0; i <  len(this.Operator.Mmap) / OFFSET_INDEX_SIZE; i++ {
		positionBytes := (this.Operator.Mmap)[i*OFFSET_INDEX_SIZE : (i+1)*OFFSET_INDEX_SIZE]
		indexKey := binary.BigEndian.Uint64(positionBytes[0:8])
		indexValue := binary.BigEndian.Uint64(positionBytes[8:16])
		if indexKey  != 0 ||  indexValue!=0{
			tmpEndOffset = int(indexKey)
		}
	}
	this.endOffset = tmpEndOffset
	return this.endOffset
}

func (this *OffsetIndex) binarySearch(start int, end int, targetOffset uint64) (int, int) {
	startIndex := this.loadIndex(start)
	endIndex := this.loadIndex(end)
	if (end-start) == 1 && startIndex.indexKey < targetOffset && endIndex.indexKey < targetOffset {
		return start, end
	}
	if( end == 0){
		return  start,end
	}
	mid := (start + end) >> 1
	midIndex := this.loadIndex(mid)
	if midIndex.indexKey > targetOffset {
		return this.binarySearch(start, mid, targetOffset)
	} else if midIndex.indexKey < targetOffset {
		return this.binarySearch(mid, end, targetOffset)
	} else {
		return mid, mid
	}
}

func (this *LogSegment) targetBatch(targetOffset uint64, size int32) (*store.RecordBatch, error) {
	end := this.OffsetIndex.Entries()
	startIndex, endIndex := this.OffsetIndex.binarySearch(0, end, targetOffset)
	result := &store.RecordBatch{}
	if startIndex == endIndex {
		offsetIndex := this.OffsetIndex.loadIndex(startIndex)
		absPosition := offsetIndex.indexValue
		//store.RecordBatch.Read(this.LogFile.storeFile,)
		_, err := this.LogFile.storeFile.Seek(int64(absPosition), 0)
		tmpRead := make([]byte,12)
		read, err := this.LogFile.storeFile.Read(tmpRead)
		fmt.Println(read)
		fmt.Println(err)

		batchLength := binary.BigEndian.Uint32(tmpRead[8:12])
		batchLengthInt64 := int64(batchLength)
		_, err = this.LogFile.storeFile.Seek(int64(absPosition), 0)
		if nil != err {
			return nil, err
		}
		batch := store.RecordBatch{}
		result, err = batch.Read(this.LogFile.storeFile, batchLengthInt64)
		if nil != err {
			return nil, err
		}
	}
	return result, nil
}

func (this *LogSegment) rangeBatch(targetOffsetPosition uint64, size int) ([]*store.RecordBatch, error) {
	stat, err := this.LogFile.storeFile.Stat()
	if nil != err {
		return nil, err
	}
	logSize := stat.Size()
	fileRecordsReader := &store.FileRecordBatchReader{
		this.LogFile.storeFile,
		int64(targetOffsetPosition),
		logSize,
	}
	var batches []*store.RecordBatch
	for fileRecordsReader.HasNext() {
		batch, err := fileRecordsReader.NextBatch()
		if err != nil {
			return nil, err
		}
		batches = append(batches, batch.(*store.RecordBatch))
		if len(batches) >= size {
			break
		}
	}
	return batches, nil
}

func (this *LogSegment) write(batch *store.RecordBatch) error {
	stat, err := this.LogFile.storeFile.Stat()
	if nil != err {
		return err
	}
	position := stat.Size()
	batch.Write(this.LogFile.storeFile)
	//store index
	this.OffsetIndex.Operator.PutIndex(batch.BaseOffset, position)
	return nil
}

func (this *OffsetIndex) loadIndex(start int) *OffsetPosition {
	positionBytes := (this.Operator.Mmap)[start*OFFSET_INDEX_SIZE : (start+1)*OFFSET_INDEX_SIZE]
	indexKey := binary.BigEndian.Uint64(positionBytes[0:8])
	indexValue := binary.BigEndian.Uint64(positionBytes[8:16])
 	return &OffsetPosition{
		indexKey,
		indexValue,
	}
}

func (this *LogSegment) LastIndexOffset() (int64, error) {
	position := this.OffsetIndex.Operator.WritePosition
	if(position == 0){
		return 0, nil
	}
	//last index in the index file
	index := this.OffsetIndex.loadIndex(int(position))
	stat, err := this.LogFile.storeFile.Stat()
	if nil != err {
		return -1, err
	}
	batch, _ := this.rangeBatch(index.indexValue, int(stat.Size()))
	last := len(batch)
	recordBatch := batch[last-1]
	return recordBatch.BaseOffset + int64(recordBatch.LastOffsetDelta), nil
}

type OffsetPosition struct {
	indexKey   uint64
	indexValue uint64
}
