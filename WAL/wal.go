package WAL

import (
	"io/ioutil"
	"os"
	"strconv"
)

const DIRECTORY = "Data/WAL"

type WriteAheadLog struct {
	segmentSize uint32
	recordNum  uint32
	lowMark     uint32
	file        *os.File
}

func NewWAL(segmentSize uint32, lwm uint32) *WriteAheadLog{
	wal := WriteAheadLog{}
	wal.segmentSize = segmentSize
	wal.lowMark = lwm

	files, _ := ioutil.ReadDir(DIRECTORY)
	if len(files) == 0{
		wal.file, _ = os.Create(DIRECTORY + "/wal_1.bin")
		wal.recordNum = 0
	}else{
		wal.file, _ = os.OpenFile(DIRECTORY + "/wal_" +strconv.Itoa(len(files)), os.O_RDWR, 0666)
	}
	//TODO recordNum
	return &wal
}
