package SSTable

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io/fs"
	"main/BloomFilter"
	"main/Memtable"
	"main/MerkleTree"
	"main/SkipList"
	"os"
	"strconv"
	"strings"
)

type Element struct {
	CRC       uint32
	Timestamp uint64
	Tombstone bool
	KeySize   uint64
	ValueSize uint64
	Key       string
	Value     []byte
}

func Panic(err error) {
	if err != nil {
		panic(err.Error())
	}
}

func DataSegToBin(node *SkipList.Skipnode) []byte {
	tombStone := []byte{0}
	if node.Tombstone {
		tombStone[0] = 1
	}
	key := []byte(node.Key)

	keySize := make([]byte, 8)
	binary.LittleEndian.PutUint64(keySize, uint64(len(key)))

	valueSize := make([]byte, 8)
	binary.LittleEndian.PutUint64(valueSize, uint64(len(node.Value)))

	timeStamp := make([]byte, 16)
	binary.LittleEndian.PutUint64(timeStamp, node.TimeStamp)

	size := binary.LittleEndian.Uint64(keySize) + binary.LittleEndian.Uint64(valueSize) + 16 + 16 + 1 + 4
	element := make([]byte, 0, size)

	crc := make([]byte, 4)
	tmp := crc32.ChecksumIEEE(node.Value)
	binary.LittleEndian.PutUint32(crc, tmp)
	// Collects all the data into one element that will be written in the SSTable
	element = append(element, crc...)
	element = append(element, timeStamp...)
	element = append(element, tombStone...)
	element = append(element, keySize...)
	element = append(element, valueSize...)
	element = append(element, key...)
	element = append(element, node.Value...)

	return element
}

func ResolveFileName(fileName fs.DirEntry) string {
	name := fileName.Name()
	num := strings.Split(name, "e")
	number := 0
	if num[1] == "" {
		number = 1
	}
	number, _ = strconv.Atoi(num[1])

	return "SSTable" + strconv.Itoa(number+1)
}

func CreateSSTable(level int) string {
	files, err := os.ReadDir("./Data/SSTable/Level" + strconv.Itoa(level))
	Panic(err)

	newFileName := ""
	if len(files) == 0 {
		newFileName = "SSTable"
	} else {
		newFileName = ResolveFileName(files[len(files)-1])
	}
	err = os.Mkdir("./Data/SSTable/Level"+strconv.Itoa(level)+"/"+newFileName, 0755)
	Panic(err)

	return newFileName
}

func FilesOfSSTable(FileName string, level int) (*os.File, *os.File, *os.File, string, *os.File, *os.File) {
	prefix := "./Data/SSTable/Level" + strconv.Itoa(level) + "/" + FileName + "/usertable-" + strconv.Itoa(level)

	data, err := os.Create(prefix + "-Data.db")
	Panic(err)
	index, err := os.Create(prefix + "-Index.db")
	Panic(err)
	TOC, err := os.Create(prefix + "-TOC.db")
	Panic(err)
	metaData, err := os.Create(prefix + "-Metadata.txt")
	Panic(err)
	summary, err := os.Create(prefix + "-Summary.db")
	Panic(err)
	return data, index, TOC, prefix + "-Filter.db", metaData, summary
}

func CreateTOC(level int, file *os.File) {
	suffix := [6]string{"-Data.db", "-Index.db", "-TOC.db", "-Filter.db", "-Metadata.db", "-Summary.db"}
	for _, fn := range suffix {
		_, err := file.WriteString("usertable-" + strconv.Itoa(level) + fn + "\n")
		Panic(err)
	}
}

func CheckData(path string, key string, ofs int64) *Element {
	crc, tombStone, timeStamp, keySize, valSize, currKey, value := ReadData(path, key, ofs)
	if binary.LittleEndian.Uint32(crc) != crc32.ChecksumIEEE(value) {
		panic("Greska u fajlu")
	} else {
		Elem := Element{}
		Elem.CRC = binary.LittleEndian.Uint32(crc)
		Elem.Timestamp = binary.LittleEndian.Uint64(timeStamp)
		if tombStone[0] == 1 {
			Elem.Tombstone = true
		} else {
			Elem.Tombstone = false
		}
		Elem.Key = string(currKey)
		Elem.KeySize = binary.LittleEndian.Uint64(keySize)

		Elem.Value = value
		Elem.ValueSize = binary.LittleEndian.Uint64(valSize)

		return &Elem
	}
}

func Flush(mt *Memtable.Memtable, bloom BloomFilter.BloomFilter) {
	level, _ := os.ReadDir("./Data/SSTable")
	var lvl int
	if len(level) == 1 {
		var err error
		lvl, err = strconv.Atoi(strings.Split(level[0].Name(), "l")[1])
		Panic(err)
	} else {
		lvl = len(level)
	}
	data, index, toc, fltr, mtData, summ := FilesOfSSTable(CreateSSTable(lvl), lvl)
	defer data.Close()
	defer index.Close()
	defer toc.Close()
	//defer fltr.Close()
	defer mtData.Close()
	defer summ.Close()

	//Toc
	CreateTOC(1, toc)

	//Bloom Filter
	//bloom := BloomFilter.CreateBloom(100, 5)

	dataOffset := 0
	indexOffset := 0
	//
	////Summary
	summary := Summary{}
	summary.Elements = make(map[string]int)

	node := mt.Skiplist.Header.Forward[0]
	summary.FirstKey = mt.Skiplist.Header.Key

	merkleHashVal := make([][20]byte, mt.GetThreshold())
	i := 0
	for node != nil {
		binData := DataSegToBin(node)
		_, err := data.Write(binData)
		Panic(err)

		bloom.AddElement([]byte(node.Key))
		binIndx := IndexSegToBin(node.Key, dataOffset)
		_, err = index.Write(binIndx)
		Panic(err)

		dataOffset += len(binData)

		summary.Elements[node.Key] = indexOffset

		indexOffset += len(binIndx)

		merkleHashVal[i] = MerkleTree.HashData(node.Value)
		i++

		nextNode := node.Forward[0]
		if nextNode == nil {
			summary.LastKey = node.Key
		}
		node = nextNode
	}
	Root := MerkleTree.Process(merkleHashVal)
	merkle := MerkleTree.Root{Root: Root}
	MerkleTree.Preorder(merkle.Root, mtData)

	bloom.EncodeAndSerialize(fltr)

	WriteSummary(&summary, summ)
}

func ReadData(path string, key string, offset int64) ([]byte, []byte, []byte, []byte, []byte, []byte, []byte) {
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
		return crc, tombStone, timeStamp, keySize, valueSize, currentKey, value
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

func PrintElement(elem *Element) {
	if elem == nil {
		fmt.Println("Nepostojeci kljuc")
		return
	}
	fmt.Println("-----------------------------------ELEMENT-----------------------------------")
	fmt.Print("CRC: " + strconv.Itoa(int(elem.CRC)) + ": Timestamp: " + strconv.Itoa(int(elem.Timestamp)))
	if elem.Tombstone {
		fmt.Print(": Tombstone: true")
	} else {
		fmt.Print(": Tombstone: false")
	}
	fmt.Print(": Key Size: " + strconv.Itoa(int(elem.KeySize)) + ": Value Size: " + strconv.Itoa(int(elem.ValueSize)))
	fmt.Println(": Key: " + elem.Key + ": Value: " + string(elem.Value))
	fmt.Println("-----------------------------------------------------------------------------")
}
