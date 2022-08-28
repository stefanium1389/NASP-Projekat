package SSTable

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io/fs"
	"main/BloomFilter"
	"main/Memtable"
	"main/SkipList"
	"os"
	"strconv"
	"strings"
)

func Panic(err error) {
	if err != nil {
		panic(err.Error())
	}
}

func DataSegToBin(node *SkipList.Skipnode) []byte {
	tomb := []byte{0}
	if node.Tombstone {
		tomb[0] = 1
	}
	key := []byte(node.Key)
	keySize := make([]byte, 8)
	binary.LittleEndian.PutUint64(keySize, uint64(len(key)))

	valSize := make([]byte, 8)
	binary.LittleEndian.PutUint64(valSize, uint64(len(node.Value)))

	crc := make([]byte, 4)
	tmp := crc32.ChecksumIEEE(node.Value)
	binary.LittleEndian.PutUint32(crc, tmp)

	size := binary.LittleEndian.Uint64(keySize) + binary.LittleEndian.Uint64(valSize) + 37
	elem := make([]byte, 0, size)
	elem = append(elem, crc...)
	elem = append(elem, tomb...)
	elem = append(elem, keySize...)
	elem = append(elem, valSize...)
	elem = append(elem, key...)
	elem = append(elem, node.Value...)

	return elem
}

func ResolveFileName(fileName fs.DirEntry) string {
	name := fileName.Name()
	num := strings.Split(name, "e")
	number, err := strconv.Atoi(num[1])
	Panic(err)

	return "SSTable" + strconv.Itoa(number+1)
}

func CreateSSTable(level int) string {
	files, err := os.ReadDir("./Data/SSTable/Level" + strconv.Itoa(level))
	Panic(err)

	newFileName := ResolveFileName(files[len(files)-1])
	err = os.Mkdir("./Data/SSTable/Level"+strconv.Itoa(level)+"/"+newFileName, 0755)
	Panic(err)

	return newFileName
}

func FilesOfSSTable(FileName string, level int) (*os.File, *os.File, *os.File, *os.File, *os.File, *os.File) {
	/* Each SSTable folder will contain the next files:
	usertable-1-Data.db; usertable-1-Index.db; usertable-1-TOC.db; usertable-1-Filter.db; usertable-1-Metadata.db
	*/
	prefix := "./Data/SSTable/Level" + strconv.Itoa(level) + "/" + FileName + "/usertable-" + strconv.Itoa(level)

	data, err := os.Create(prefix + "-Data.db")
	Panic(err)
	index, err := os.Create(prefix + "-Index.db")
	Panic(err)
	TOC, err := os.Create(prefix + "-TOC.db")
	Panic(err)
	filter, err := os.Create(prefix + "-Filter.db")
	Panic(err)
	metaData, err := os.Create(prefix + "-Metadata.txt")
	Panic(err)
	summary, err := os.Create(prefix + "-Summary.db")
	Panic(err)
	return data, index, TOC, filter, metaData, summary
}

func CreateTOC(level int, file *os.File) {
	suffix := [6]string{"-Data.db", "-Index.db", "-TOC.db", "-Filter.db", "-Metadata.db", "-Summary.db"}
	for _, fn := range suffix {
		_, err := file.WriteString("usertable-" + strconv.Itoa(level) + fn + "\n")
		Panic(err)
	}
}

func Flush(mt *Memtable.Memtable) {
	//newFileName := CreateSSTable(1)
	data, index, toc, fltr, mtData, summ := FilesOfSSTable(CreateSSTable(1), 1)
	defer data.Close()
	defer index.Close()
	defer toc.Close()
	defer fltr.Close()
	defer mtData.Close()
	defer summ.Close()

	//Toc
	CreateTOC(1, toc)

	//Bloom Filter
	bloom := BloomFilter.CreateBloom(mt.GetThreshold(), 0.99)

	dataOffset := 0
	indexOffset := 0

	//Summary
	summary := Summary{}
	summary.Elements = make(map[string]int)

	node := mt.Skiplist.Header.Forward[0]
	summary.FirstKey = mt.Skiplist.Header.Key

	for node != nil {
		binData := DataSegToBin(node)
		_, err := data.Write(binData)
		Panic(err)

		bloom.Add(node.Key)
		binIndx := IndexSegToBin(node.Key, dataOffset)
		_, err = index.Write(binIndx)
		Panic(err)

		dataOffset += len(binData)

		summary.Elements[node.Key] = indexOffset

		indexOffset += len(binIndx)

		nextNode := node.Forward[0]
		if nextNode == nil {
			summary.LastKey = node.Key
		}
		node = nextNode
	}
	//Dodati deo sa Merkle Stablom
	//	WriteMerkle()
	bloom.WriteBloomFilter(fltr)
	WriteSummary(&summary, summ)
}

func ReadData(path string, key string, offset int64) ([]byte, []byte, []byte, []byte, []byte, []byte) {
	//+---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
	//|    CRC (4B)   | Timestamp (16B) | Tombstone(1B) | Key Size (8B) | Value Size (8B) | Key | Value |
	//+---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+

	file, err := os.OpenFile(path, os.O_RDONLY, 0700)
	Panic(err)
	defer file.Close()
	_, err = file.Seek(offset, 0)
	Panic(err)
	br := bufio.NewReader(file)

	crc := make([]byte, 4)
	_, err = br.Read(crc)
	Panic(err)
	timeStamp := make([]byte, 16)
	_, err = br.Read(timeStamp)
	Panic(err)
	tombStone := make([]byte, 1)
	_, err = br.Read(tombStone)
	Panic(err)
	keySize := make([]byte, 8)
	_, err = br.Read(keySize)
	Panic(err)
	valueSize := make([]byte, 8)
	_, err = br.Read(valueSize)
	Panic(err)

	currentKey := make([]byte, binary.LittleEndian.Uint64(keySize))
	_, err = br.Read(currentKey)
	Panic(err)
	// If the key is not where we expected it to be an error is raised
	if key == string(currentKey) {
		value := make([]byte, binary.LittleEndian.Uint64(valueSize))
		_, err = br.Read(value)
		return crc, tombStone, keySize, valueSize, currentKey, value
	} else {
		panic("Error: Key not found in estimated position")
	}
}

// PrintData : Used for debugging, prints the contents of the Data file
func PrintData(path string) {

	file, err := os.OpenFile(path, os.O_RDONLY, 0700)
	Panic(err)
	defer file.Close()
	_, err = file.Seek(0, 0)
	Panic(err)
	br := bufio.NewReader(file)

	i := 1
	for err == nil {
		crc := make([]byte, 4)
		_, err = br.Read(crc)
		if err != nil {
			break
		}
		timeStamp := make([]byte, 16)
		_, err = br.Read(timeStamp)
		if err != nil {
			break
		}
		tombStone := make([]byte, 1)
		_, err = br.Read(tombStone)
		if err != nil {
			break
		}
		keySize := make([]byte, 8)
		_, err = br.Read(keySize)
		if err != nil {
			break
		}
		valueSize := make([]byte, 8)
		_, err = br.Read(valueSize)
		if err != nil {
			break
		}
		currentKey := make([]byte, binary.LittleEndian.Uint64(keySize))
		_, err = br.Read(currentKey)
		if err != nil {
			break
		}
		value := make([]byte, binary.LittleEndian.Uint64(valueSize))
		_, err = br.Read(value)

		var ts string
		if tombStone[0] == 1 {
			ts = "True"
		} else {
			ts = "False"
		}

		fmt.Println(i, ". CRC: ", binary.LittleEndian.Uint32(crc),
			"; Timestamp: ", binary.LittleEndian.Uint32(timeStamp),
			"; Tombstone: ", ts,
			"; Key size: ", binary.LittleEndian.Uint64(keySize),
			"; Value Size: ", binary.LittleEndian.Uint64(valueSize),
			"; Key: ", string(currentKey),
			"; Value:", string(value))
		i++
	}
}
