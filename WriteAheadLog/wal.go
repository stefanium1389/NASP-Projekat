package WAL

import (
	"encoding/binary"
	"io"
	"io/ioutil"
	"os"
	"strconv"
)

//Treba ubaciti dir i filenameove koje ce da trazi plus readfile na dva mesta

type WriteAheadLog struct {
	WALNodes    []*WALNode
	segmentSize uint32
	recordNum   uint32
	lowMark     uint32
	currentFile uint32
	currentFileNum uint32
	path        string
}

func NewWAL(segmentSize uint32) *WriteAheadLog{
	path := "" // ovde fali konstanta koja ce da vodi na odredjeni path


	fileList, _ := ioutil.ReadDir("DIR") // fali dir
	currentFile := len(fileList)

	return &WriteAheadLog{
		WALNodes:        make([]*WALNode, 0),
		segmentSize:     segmentSize,
		currentFile:     uint32(currentFile),
		recordNum: 		 0,
		currentFileNum:  0,
		lowMark:   		 0,
		path:            path,
	}


}


func (wal *WriteAheadLog) put(key string, data []byte) {
	if wal.checkSegmentLimit() {
		wal.writeAndClear()
	}
	newWalNode := NewNode(key,data)
	wal.WALNodes = append(wal.WALNodes, newWalNode)
}

func (wal *WriteAheadLog) delete(key string) {
	newWalNode := NewNode(key,nil)
	newWalNode.tombstone = 1
	wal.WALNodes = append(wal.WALNodes, newWalNode)
}



func (wal *WriteAheadLog) checkSegmentLimit() bool {
	return uint32(len(wal.WALNodes)) >= wal.segmentSize
}
func refactorFileNames(path string){
	fileList, _ := ioutil.ReadDir("DIR")

	i := 0
	for _, file := range fileList {
		os.Rename("DIR"+file.Name(), path+strconv.Itoa(i)+".gob")
		i++
	}
}

func (wal *WriteAheadLog) deleteSegmentsAndRearrangeFile() {
	fileList, _ := ioutil.ReadDir("DIR")
	fileCount := uint32(len(fileList))

	if fileCount > wal.lowMark {
		for _, file := range fileList {
			file.Name()
			os.Remove("ovde ime fajla za brisanje")
			fileCount--
			if fileCount == wal.lowMark {
				break
			}
		}
		refactorFileNames(wal.path)
	}
}





func (wal *WriteAheadLog) writeAndClear() bool {
//	appendFile, _ := os.OpenFile()
//
//	for i := 0; i < len(wal.WALNodes); i++ {
//		err := appendToFile(appendFile, wal.WALNodes[i].dataForWrite())
//		if err != nil {
//			return false
//		}
//		wal.currentFileNum++
//		if wal.currentFileNum >= wal.recordNum {
//			wal.currentFile++
//			appendFile.Close()
//			//appendFile, _ = os.OpenFile()
//		}
//	}
//	wal.empty()
//	appendFile.Close()
	return true
}

func appendToFile(file *os.File, data []byte) error {

	file.Seek(0, 2)
	file.Write(data)

	return nil
}

func (wal *WriteAheadLog) empty(){
	wal.currentFileNum = 0
	wal.WALNodes = make([]*WALNode, 0)
}

func recoveryCreateNodes(data []byte,currentFileIndex int ,source *os.File){
	crc 	  := binary.LittleEndian.Uint32(data[:4])
	timestamp := binary.LittleEndian.Uint64(data[4:19])
	tombstone := data[19:20]
	keySize   := binary.LittleEndian.Uint64(data[20:28])
	valueSize := binary.LittleEndian.Uint64(data[28:36])

	data = make([]byte, currentFileIndex+int(keySize)-currentFileIndex)
	_, err := io.ReadFull(source, data)
	if err!=nil {
		return
	}
	key := data
	currentFileIndex += int(valueSize)

	data = make([]byte, currentFileIndex+int(keySize)-currentFileIndex)
	_, err = io.ReadFull(source, data)
	if err!=nil {
		return
	}
	value := data
	currentFileIndex += int(valueSize)

	node := WALNode{
		CRC		 : crc,
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
			recoveryCreateNodes(data,currentFileIndex,source)

		}
	}
}










