package WAL

import (
	"bufio"
	"io/ioutil"
	"os"
	"strconv"
)

const DIRECTORY = "Data/WAL"

type WriteAheadLog struct {
	segmentSize	uint32
	recordNum	int
	lowMark     uint32
	filePath	string
}

func NewWAL(segmentSize uint32, lwm uint32) *WriteAheadLog{
	wal := WriteAheadLog{}
	wal.segmentSize = segmentSize
	wal.lowMark = lwm

	files, _ := ioutil.ReadDir(DIRECTORY)
	if len(files) == 0{
		os.Create(DIRECTORY + "/wal_1.bin")
		wal.filePath = DIRECTORY+"/wal_1.bin"
		wal.recordNum = 0
	}else{
		wal.filePath = DIRECTORY + "/wal_" + strconv.Itoa(len(files))
	}
	wal.SetRecordNumber()
	return &wal
}

func (wal *WriteAheadLog) SetRecordNumber(){
	file, _ := os.OpenFile(wal.filePath, os.O_RDWR, 0666)
	defer file.Close()
	reader := bufio.NewReader(file)
	recordNum:= 0
	node := WALNode{}
	for true {
		if !node.Decode(reader){
			break
		}
		recordNum++
	}
	wal.recordNum = recordNum
}


