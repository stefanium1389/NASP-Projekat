package LSM

import (
	"encoding/binary"
	"io"
	"io/ioutil"
	"main/BloomFilter"
	"main/MerkleTree"
	"main/SSTable"
	"os"
	"strconv"
)

const (
	DIR_PATH = "./Data/SSTable/Level"
	BF_RATE = 0.4
	MT_THRESHOLD = 10
)

var dataOffset = 0
var indexOffset = 0
var merkleIndex = 0

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

			sstable := SSTable.CreateSSTable(i)
			data, index, toc, _, metadata, summaryFile := SSTable.FilesOfSSTable(sstable, i)
			defer data.Close()
			defer index.Close()
			defer toc.Close()
			defer metadata.Close()
			defer summaryFile.Close()
			SSTable.CreateTOC(i, toc)

			bloomFilter := BloomFilter.BloomFilter{}
			bloomFilter.Initialize(MT_THRESHOLD, BF_RATE)
			summary := SSTable.Summary{}
			summary.Elements = make(map[string]int)

			merkleHash := make([][20]byte, MT_THRESHOLD)

			lsm.MergeData(file1, file2, data, index, &bloomFilter, &summary, &merkleHash)
			merkleIndex = 0
			//delete
		}
	}
}

func (lsm *LSM) MergeData(file1, file2, data, index *os.File, filter *BloomFilter.BloomFilter,
	summary *SSTable.Summary, merkleHash *[][20]byte){

	for true{
		crc1, ts1, tb1, keySize1, valueSize1, key1, value1, err1 := lsm.ReadData(file1)
		crc2, ts2, tb2, keySize2, valueSize2, key2, value2, err2 := lsm.ReadData(file2)

		if err1 || err2{
			if err2{
				Offset := 4 + 16 + 1 + 8 + 8 + int64(keySize1) + int64(valueSize1)
				file1.Seek(-Offset, 1)
			}else if err1{
				Offset := 4 + 16 + 1 + 8 + 8 + int64(keySize2) + int64(valueSize2)
				file2.Seek(-Offset, 1)
			}
			break
		}

		if key1 == key2{
			if ts1 > ts2{
				if tb1[0] == byte(0){
					//write element 1
					arr := DataSegToBin(crc1, ts1, tb1[0], key1, value1)
					lsm.WriteData(arr, key1,value2, data, index, filter, summary, merkleHash)
					merkleIndex++
				}

			}else{
				if tb2[0] == byte(0){
					//write element 2
					arr := DataSegToBin(crc2, ts2, tb2[0], key2, value2)
					lsm.WriteData(arr, key2,value2, data, index, filter, summary, merkleHash)
					merkleIndex++
				}
			}
		}else if key1 > key2{
			//write element2
			arr := DataSegToBin(crc2, ts2, tb2[0], key2, value2)
			lsm.WriteData(arr, key2,value2, data, index, filter, summary, merkleHash)
			merkleIndex++

			Offset := 4 + 8 + 1 + 8 + 8 + int64(keySize1) + int64(valueSize1)
			file1.Seek(-Offset, 1)

		}else{
			//write element 1
			arr := DataSegToBin(crc1, ts1, tb1[0], key1, value1)
			lsm.WriteData(arr, key1,value1, data, index, filter, summary, merkleHash)
			merkleIndex++
			Offset := 4 + 8 + 1 + 8 + 8 + int64(keySize2) + int64(valueSize2)
			file2.Seek(-Offset, 1)
		}
	}

	//finish files
	for true{
		crc, ts, tb, _, _, key, value, err := lsm.ReadData(file1)
		if err{
			break
		}
		//write element
		arr := DataSegToBin(crc, ts, tb[0], key, value)
		lsm.WriteData(arr, key,value, data, index, filter, summary, merkleHash)
		merkleIndex++

	}

	for true{
		crc, ts, tb, _, _, key, value, err := lsm.ReadData(file2)
		if err{
			break
		}
		//write element
		arr := DataSegToBin(crc, ts, tb[0], key, value)
		lsm.WriteData(arr, key, value, data, index, filter, summary, merkleHash)
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

func (lsm *LSM) ReadData(file *os.File) ([]byte, uint64, []byte, uint64, uint64, string, []byte, bool) {

	crc := make([]byte, 4)
	_, err := file.Read(crc)

	if err == io.EOF{
		return nil, 0, nil, 0, 0, "", nil, true
	}

	timeStamp := make([]byte, 16)
	_, err = file.Read(timeStamp)
	if err != nil{
		panic(err.Error())
	}
	ts := binary.LittleEndian.Uint64(timeStamp)

	tombStone := make([]byte, 1)
	_, err = file.Read(tombStone)
	if err != nil{
		panic(err.Error())
	}
	keySize := make([]byte, 8)
	_, err = file.Read(keySize)
	if err != nil{
		panic(err.Error())
	}
	keyS := binary.LittleEndian.Uint64(keySize)

	valueSize := make([]byte, 8)
	_, err = file.Read(valueSize)
	if err != nil{
		panic(err.Error())
	}
	valueS := binary.LittleEndian.Uint64(valueSize)

	currentKey := make([]byte, binary.LittleEndian.Uint64(keySize))
	_, err = file.Read(currentKey)
	if err != nil{
		panic(err.Error())
	}
	key := string(currentKey)

	value := make([]byte, binary.LittleEndian.Uint64(valueSize))
	if err != nil{
			panic(err.Error())
		}

	return crc, ts, tombStone, keyS, valueS, key, value, false

}