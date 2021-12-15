package log

import (
	"encoding/binary"
	"github.com/edsrzf/mmap-go"
	"github.com/jiubafangxing/baecon/internal/common"
	"github.com/jiubafangxing/baecon/internal/store"
	"io/ioutil"
	"os"
	"strings"
)

const (
	OFFSET_INDEX_SIZE = 16
	LOG_SUFFIX = "log"
	INDEX_SUFFIX = "index"
	DOT = "."
)

type  LogSegment struct {
	LogFile *LogFile
	//Sparse index based on relative offset and physical position
	OffsetIndex *OffsetIndex

	SegmentName string


}

func (this * LogSegment) readRecords(offset int64)  {


}

func  BuildLogSegment(segmentPath string)(error, *LogSegment) {
	exists, _ := common.PathExists(common.BASE_DIR + segmentPath)
	if(!exists){
		createrr := os.MkdirAll(segmentPath, 0766)
		if createrr != nil {
			print(createrr)
		}
	}
	fileList, _ := ioutil.ReadDir(common.BASE_DIR + segmentPath)
	var mmapBytes []byte
	var indexFile *os.File
	var logFile *os.File
	if len(fileList) >= 0 {
		var indexFileNames []string=  []string{segmentPath,DOT, INDEX_SUFFIX}
		var logFileNames []string=  []string{segmentPath,DOT, LOG_SUFFIX}
		indexFileName := strings.Join(indexFileNames,"")
		logFileName := strings.Join(logFileNames,"")
		indexFile, err := os.Create(common.BASE_DIR + indexFileName)
		if nil != err {
			print("create index fail")
			return nil,nil
		}
		logFile, err := os.Create(common.BASE_DIR + logFileName)
		if nil != err {
			print("create log fail")
			return nil,nil
		}
		indexFileInfo, _ := indexFile.Stat()
		logFileInfo, _ := logFile.Stat()
		fileList = append(fileList, indexFileInfo)
		fileList = append(fileList, logFileInfo)
	}
		for _, fileItem := range fileList {
			//offset index
			if(strings.HasSuffix(fileItem.Name(),"index")){
				indexFile, _ := os.Open(fileItem.Name())
				defer indexFile.Close()
				mmap, err := mmap.Map(indexFile, mmap.RDWR, 0)
				if(nil != err){
					return err,nil
				}
				mmapBytes = mmap
			}
			//logfile
			if(strings.HasSuffix(fileItem.Name(),"log")) {
				logFile, err := os.Open(fileItem.Name())
				if err != nil {
					return err,nil
				}
				logFile = logFile
			}
		}

	logFileItem := &LogFile{logFile}
	index := &OffsetIndex{
		indexFile,
		mmapBytes,
		}
	return  nil, &LogSegment{
		logFileItem,
		index,
		segmentPath,

	}
}


type LogFile struct {
	storeFile *os.File
}

type OffsetIndex struct {
	storeFile *os.File
	mmapBytes []byte
}

func (this * OffsetIndex) Entries() int{
	return len(this.mmapBytes) / OFFSET_INDEX_SIZE
}

func (this * OffsetIndex)binarySearch(start int , end int, targetOffset uint64) (int, int) {
	startIndex := this.loadIndex(start)
	endIndex := this.loadIndex(end)
	if((end -start) ==1 && startIndex.indexKey < targetOffset && endIndex.indexKey < targetOffset){
		return start, end
	}
	mid := (start + end) >> 1
	midIndex := this.loadIndex(mid)
	if midIndex.indexKey > targetOffset{
		return this.binarySearch(start, mid, targetOffset)
	} else if midIndex.indexKey < targetOffset{
		return this.binarySearch(mid, end, targetOffset)
	} else{
		return mid,mid
	}
}

func (this * LogSegment) targetBatch(targetOffset uint64) (*store.RecordBatch,error) {
	end := this.OffsetIndex.Entries()
	startIndex, endIndex := this.OffsetIndex.binarySearch(0, end, targetOffset)
	if(startIndex == endIndex){
		offsetIndex := this.OffsetIndex.loadIndex(startIndex)
		absPosition := offsetIndex.indexValue
		_, err := this.LogFile.storeFile.Seek(int64(absPosition), 0)
		if nil != err {
			return nil, err
		}
	}
	return nil,nil
}

func (this * OffsetIndex) loadIndex(start int) *OffsetPosition {
	positionBytes := this.mmapBytes[start*OFFSET_INDEX_SIZE : (start+1)*OFFSET_INDEX_SIZE]
	indexKey := binary.BigEndian.Uint64(positionBytes[0:8])
	indexValue := binary.BigEndian.Uint64(positionBytes[8:16])
	return &OffsetPosition{
		indexKey,
		indexValue,
	}
}

type  OffsetPosition struct {
	indexKey uint64
	indexValue uint64
}


