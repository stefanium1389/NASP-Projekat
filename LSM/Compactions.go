package LSM

import (
	"bufio"
	"encoding/binary"
	"io"
	"io/ioutil"
	"main/BloomFilter"
	"main/MerkleTree"
	"main/SSTable"
	"os"
	"strconv"
	"strings"
)

const (
	DIR_PATH = "./Data/SSTable/Level"
	BF_RATE = 0.4
	MT_THRESHOLD = 4
)

var dataOffset = 0
var indexOffset = 0
var merkleIndex = 0
var lastKey = ""

type LSM struct{
	maxLevels int
}

func NewLSM(maxLevels int) *LSM{
	lsm := LSM{}
	lsm.maxLevels = maxLevels
	return &lsm
}

func (lsm *LSM) Compaction(){
	for i := 1; i < lsm.maxLevels; i++{
		level := strconv.Itoa(i)

		dirs, err := ioutil.ReadDir(DIR_PATH + level)
		if err != nil{
			panic(err.Error())
		}

		if len(dirs) < 2{
			return
		}

		helper := 0
		for j := 0; j < len(dirs)/2; j++{
			indexOffset = 0
			dataOffset = 0
			merkleIndex = 0
			fileName := dirs[helper].Name()

			file1, err := os.OpenFile(DIR_PATH + level + "/" + fileName + "/usertable-" + level + "-Data.db", os.O_RDONLY, 0700)
			if err != nil{
				panic(err.Error())
			}

			fileName = dirs[helper+1].Name()
			helper+=2
			file2, err := os.OpenFile(DIR_PATH + level + "/" + fileName + "/usertable-" + level + "-Data.db", os.O_RDONLY, 0700)
			if err != nil{
				panic(err.Error())
			}

			helper++
			defer file1.Close()
			defer file2.Close()

			files ,_ := os.ReadDir("./Data/SSTable")
			n, _ := strconv.Atoi(strings.Split(files[len(files)-1].Name(),"l")[1])
			n+=1
			os.Mkdir(DIR_PATH+ strconv.Itoa(n), 0777)

			sstable := SSTable.CreateSSTable(i+1)
			data, index, toc, filter, metadata, summaryFile := SSTable.FilesOfSSTable(sstable, i+1)
			defer data.Close()
			defer index.Close()
			defer toc.Close()
			defer metadata.Close()
			defer summaryFile.Close()
			SSTable.CreateTOC(i, toc)


			bloomFilter := BloomFilter.BloomFilter{}
			bloomFilter.Initialize(MT_THRESHOLD*2, BF_RATE)

			summary := SSTable.Summary{}
			summary.Elements = make(map[string]int)

			merkleHash := make([][20]byte, MT_THRESHOLD*2)

			reader1 := bufio.NewReader(file1)
			reader2 := bufio.NewReader(file2)

			lsm.MergeData(file1, file2, data, index, &bloomFilter, &summary, &merkleHash, reader1, reader2)
			bloomFilter.EncodeAndSerialize(filter)

			Root := MerkleTree.Process(merkleHash)
			merkle := MerkleTree.Root{Root: Root}
			MerkleTree.Preorder(merkle.Root, metadata)

			merkleIndex = 0

			summary.LastKey = lastKey

			SSTable.WriteSummary(&summary, summaryFile)
		}
	}
}

func (lsm *LSM) MergeData(file1 *os.File, file2 *os.File, data, index *os.File, filter *BloomFilter.BloomFilter,
	summary *SSTable.Summary, merkleHash *[][20]byte, reader1 *bufio.Reader, reader2 *bufio.Reader){

	for k := 0; k < 100; k++{
		crc1, ts1, tb1, keySize1, valueSize1, key1, value1, err1 := lsm.ReadData(reader1)
		crc2, ts2, tb2, keySize2, valueSize2, key2, value2, err2 := lsm.ReadData(reader2)
		if err1 || err2 {
			if !err1 {
				Offset := 4 + 16 + 1 + 8 + 8 + int64(keySize1) + int64(valueSize1)
				file1.Seek(-Offset, 1)
			} else if !err2 {
				Offset := 4 + 16 + 1 + 8 + 8 + int64(keySize2) + int64(valueSize2)
				file2.Seek(-Offset, 1)
			}
			break
		}

		if key1 == key2{
			if k == 0{
				summary.FirstKey = key1
			}
			if ts1 > ts2{
				if tb1[0] == byte(0){
					//write element 1
					arr := DataSegToBin(crc1, ts1, tb1[0], key1, value1)

					lsm.WriteData(arr, key1,value2, data, index, filter, summary, merkleHash)
					merkleIndex++
				}else{
					continue
				}

			}else{
				if tb2[0] == byte(0){
					//write element 2
					arr := DataSegToBin(crc2, ts2, tb2[0], key2, value2)
					lsm.WriteData(arr, key2,value2, data, index, filter, summary, merkleHash)
					merkleIndex++
				}else{
					continue
				}
			}
		}else if key1 > key2{
			//write element2
			if k == 0{
				summary.FirstKey = key2
			}
			arr := DataSegToBin(crc2, ts2, tb2[0], key2, value2)
			lsm.WriteData(arr, key2,value2, data, index, filter, summary, merkleHash)
			merkleIndex++

			Offset := 4 + 16 + 1 + 8 + 8 + int64(keySize1) + int64(valueSize1)
			Offset = -Offset

			file1.Seek(Offset, 1)

		}else{
			//write element 1
			if k == 0{
				summary.FirstKey = key1
			}
			arr := DataSegToBin(crc1, ts1, tb1[0], key1, value1)
			lsm.WriteData(arr, key1,value1, data, index, filter, summary, merkleHash)
			merkleIndex++
			Offset := 4 + 16 + 1 + 8 + 8 + int64(keySize2) + int64(valueSize2)
			Offset = -Offset
			file2.Seek(Offset, 1)
		}
	}

	//finish files
	for true{
		crc, ts, tb, _, _, key, value, err := lsm.ReadData(reader1)
		if err{
			break
		}
		//write element
		arr := DataSegToBin(crc, ts, tb[0], key, value)
		lsm.WriteData(arr, key,value, data, index, filter, summary, merkleHash)
		lastKey = key
		merkleIndex++

	}
	for true{
		crc, ts, tb, _, _, key, value, err := lsm.ReadData(reader2)
		if err{
			break
		}
		//write element
		arr := DataSegToBin(crc, ts, tb[0], key, value)
		lsm.WriteData(arr, key, value, data, index, filter, summary, merkleHash)
		lastKey = key
		merkleIndex++
	}

}

func (lsm *LSM) WriteData(element []byte, key string, value []byte, data, index *os.File,
	bloomFilter *BloomFilter.BloomFilter, summary *SSTable.Summary, merkleHash *[][20]byte ) {

	bloomFilter.AddElement([]byte(key))

	_, err := data.Write(element)
	if err != nil{
		panic(err.Error())
	}

	binaryIndex := SSTable.IndexSegToBin(key, dataOffset)
	_, err = index.Write(binaryIndex)
	dataOffset += len(element)
	summary.Elements[key] = indexOffset

	indexOffset += len(binaryIndex)

	(*merkleHash)[merkleIndex] = MerkleTree.HashData(value)

}

func DataSegToBin(crc []byte, ts uint64, tb byte, key string, value []byte) []byte {

	keySize := make([]byte, 8)
	binary.LittleEndian.PutUint64(keySize, uint64(len(key)))

	valueSize := make([]byte, 8)
	binary.LittleEndian.PutUint64(valueSize, uint64(len(value)) )

	timeStamp := make([]byte, 16)
	binary.LittleEndian.PutUint64(timeStamp, ts)

	keyByte := []byte(key)

	size := binary.LittleEndian.Uint64(keySize) + binary.LittleEndian.Uint64(valueSize) + 16 + 16 + 1 + 4
	element := make([]byte, 0, size)

	// Collects all the data into one element that will be written in the SSTable
	element = append(element, crc...)
	element = append(element, timeStamp...)
	element = append(element, tb)
	element = append(element, keySize...)
	element = append(element, valueSize...)
	element = append(element, keyByte...)
	element = append(element, value...)

	return element
}

func (lsm *LSM) ReadData(br *bufio.Reader) ([]byte, uint64, []byte, uint64, uint64, string, []byte, bool) {

	crc := make([]byte, 4)
	_, err := br.Read(crc)
	if err == io.EOF{
		return nil, 0, nil, 0, 0, "", nil, true
	}
	//Panic(err)
	timeStamp := make([]byte, 16)
	_, err = br.Read(timeStamp)
	if err == io.EOF{
		return nil, 0, nil, 0, 0, "", nil, true
	}
	ts := binary.LittleEndian.Uint64(timeStamp)
	//Panic(err)
	tombStone := make([]byte, 1)
	_, err = br.Read(tombStone)
	if err == io.EOF{
		return nil, 0, nil, 0, 0, "", nil, true
	}
	//Panic(err)
	keySize := make([]byte, 8)
	_, err = br.Read(keySize)

	if err == io.EOF{
		return nil, 0, nil, 0, 0, "", nil, true
	}
	ks := binary.LittleEndian.Uint64(keySize)
	//Panic(err)
	valueSize := make([]byte, 8)
	_, err = br.Read(valueSize)
	if err == io.EOF{
		return nil, 0, nil, 0, 0, "", nil, true
	}
	vs := binary.LittleEndian.Uint64(valueSize)
	//Panic(err)

	if ks > 50 || vs > 50{
		return nil, 0, nil, 0, 0, "", nil, true
	}
	currentKey := make([]byte,ks)
	_, err = br.Read(currentKey)
	if err == io.EOF{
		return nil, 0, nil, 0, 0, "", nil, true
	}

	value := make([]byte, vs)
	_, err = br.Read(value)
	if err == io.EOF{
		return nil, 0, nil, 0, 0, "", nil, true
	}

	return crc, ts, tombStone, ks, vs, string(currentKey), value, false

}