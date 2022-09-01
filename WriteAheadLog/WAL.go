package WriteAheadLog

import (
	"encoding/binary"
	"io"
	"io/ioutil"
	"os"
	"strconv"
)

type WriteAheadLog struct {
	WALNodes       []*WALNode
	segmentSize    uint32
	recordNum      uint32
	lowMark        uint32
	currentFile    uint32
	currentFileNum uint32
	path           string
}

func NewWAL(segmentSize uint32) *WriteAheadLog {

	fileList, _ := ioutil.ReadDir("./Data/WAL")
	currentFile := len(fileList)

	return &WriteAheadLog{
		WALNodes:       make([]*WALNode, 0),
		segmentSize:    segmentSize,
		currentFile:    uint32(currentFile),
		recordNum:      0,
		currentFileNum: 0,
		lowMark:        0,
		path:           resolvePath(),
	}

}

func resolvePath() string {
	files, _ := os.ReadDir("./Data/WAL")
	var name string
	if len(files) == 0 {
		name = "./Data/WAL/wal.db"
	} else {
		name = "./Data/WAL/wal" + strconv.Itoa(len(files)) + ".db"
	}
	return name
}

func (wal *WriteAheadLog) Put(key string, data []byte, delete bool) {
	if wal.checkSegmentLimit() {
		wal.writeAndClear()
	}
	newWalNode := NewNode(key, data)
	if delete {
		newWalNode.tombstone = 1
	}
	wal.WALNodes = append(wal.WALNodes, newWalNode)
}

func (wal *WriteAheadLog) checkSegmentLimit() bool {
	return uint32(len(wal.WALNodes)) >= wal.segmentSize
}

//func refactorFileNames(path string){
//	fileList, _ := ioutil.ReadDir("DIR")
//
//	i := 0
//	for _, file := range fileList {
//		os.Rename("DIR"+file.Name(), path+strconv.Itoa(i)+".gob")
//		i++
//	}
//}
//
//func (wal *WriteAheadLog) deleteSegmentsAndRearrangeFile() {
//	fileList, _ := ioutil.ReadDir("DIR")
//	fileCount := uint32(len(fileList))
//
//	if fileCount > wal.lowMark {
//		for _, file := range fileList {
//			file.Name()
//			os.Remove("ovde ime fajla za brisanje")
//			fileCount--
//			if fileCount == wal.lowMark {
//				break
//			}
//		}
//		refactorFileNames(wal.path)
//	}
//}

func (wal *WriteAheadLog) writeAndClear() bool {
	appendFile, _ := os.Create(wal.path)
	defer appendFile.Close()
	for i := 0; i < len(wal.WALNodes); i++ {
		appendFile.Seek(0, 2)
		appendFile.Write(wal.WALNodes[i].dataForWrite())
		wal.currentFileNum++
		if wal.currentFileNum >= wal.recordNum {
			wal.currentFile++
			wal.path = resolvePath()

			//appendFile, _ = os.OpenFile()
		}
	}
	wal.empty()
	return true
}

func (wal *WriteAheadLog) empty() {
	wal.currentFileNum = 0
	wal.WALNodes = make([]*WALNode, 0)
}

func recoveryCreateNodes(data []byte, currentFileIndex int, source *os.File) {
	crc := binary.LittleEndian.Uint32(data[:4])
	timestamp := binary.LittleEndian.Uint64(data[4:19])
	tombstone := data[19:20]
	keySize := binary.LittleEndian.Uint64(data[20:28])
	valueSize := binary.LittleEndian.Uint64(data[28:36])

	data = make([]byte, currentFileIndex+int(keySize)-currentFileIndex)
	_, err := io.ReadFull(source, data)
	if err != nil {
		return
	}
	key := data
	currentFileIndex += int(valueSize)

	data = make([]byte, currentFileIndex+int(keySize)-currentFileIndex)
	_, err = io.ReadFull(source, data)
	if err != nil {
		return
	}
	value := data
	currentFileIndex += int(valueSize)

	node := WALNode{
		CRC:       crc,
		timeStamp: timestamp,
		tombstone: tombstone[0],
		keySize:   keySize,
		valueSize: valueSize,
		key:       key,
		value:     value,
	}
	node.checkSum()

}
func (wal *WriteAheadLog) recoverySystem() {

	fileList, _ := ioutil.ReadDir("wal/") //ovde da se ostavi sa kog direktorijuma da povuce fajlove
	fileLen := len(fileList)

	for i := 0; i < fileLen; i++ {
		fileName := ""

		source, _ := os.OpenFile(fileName, os.O_RDONLY, 0777)
		fileInfo, err := source.Stat()
		if err != nil {
			break
		}

		fileSize := fileInfo.Size()
		if fileSize == 0 {
			break
		}

		currentFileIndex := 0
		for j := 0; true; j++ {
			data := make([]byte, currentFileIndex+36-currentFileIndex)
			_, err = io.ReadFull(source, data)
			currentFileIndex += 36

			if err == io.EOF {
				source.Close()
				break
			}
			recoveryCreateNodes(data, currentFileIndex, source)

		}
	}
}
