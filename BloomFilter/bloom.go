package BloomFilter

import (
	"encoding/gob"
	"fmt"
	"github.com/spaolacci/murmur3"
	"hash"
	"math"
	"os"
	"time"
)

type BloomFilter interface {
	Add(s string)
	MaybeContains(s string) bool
}

type BloomStruct struct {
	Data      []byte
	Hash      []hash.Hash32
	DataSize  int
	HashSize  int
	timeStamp uint
}

func (bs BloomStruct) Add(s string) {
	for i := 0; i < bs.HashSize; i++ {
		bs.Hash[i].Write([]byte(s))
		index := bs.Hash[i].Sum32() % uint32(bs.DataSize)
		bs.Hash[i].Reset()
		bs.Data[index] = 1
	}
}

func (bs BloomStruct) MaybeContains(s string) bool {
	for i := 0; i < bs.HashSize; i++ {
		bs.Hash[i].Write([]byte(s))
		index := bs.Hash[i].Sum32() % uint32(bs.DataSize)
		bs.Hash[i].Reset()
		if bs.Data[index] != 1 {
			return false
		}
	}
	return true
}

func CreateHashFunctions(hashSize int, ts uint) []hash.Hash32 {
	hs := []hash.Hash32{}
	for i := 1; i <= hashSize; i++ {
		hs = append(hs, murmur3.New32WithSeed(uint32(ts+1)))
	}
	return hs
}

func CreateBloom(capacity int, ratio float64) *BloomStruct {
	dataS := int(math.Ceil(float64(capacity) * math.Abs(math.Log(ratio)) / math.Pow(math.Log(2), float64(2))))
	hashS := int(math.Ceil((float64(dataS) / float64(capacity)) * math.Log(2)))
	ts := uint(time.Now().Unix())
	hs := CreateHashFunctions(hashS, ts)
	bf := BloomStruct{
		DataSize:  dataS,
		HashSize:  hashS,
		Data:      make([]byte, dataS),
		Hash:      hs,
		timeStamp: ts,
	}
	return &bf
}

func (bs *BloomStruct) WriteBloomFilter(file *os.File) {
	defer file.Close()
	fmt.Println("PRE fajlaaaa", bs.Hash)
	bs.Hash = nil
	encoder := gob.NewEncoder(file)
	err := encoder.Encode(&bs)
	if err != nil {
		panic(err.Error())
	}
}

func (bs *BloomStruct) ReadBfFromDisk(file *os.File) BloomStruct {
	defer file.Close()
	bs.Hash = nil
	bs.Data = nil
	decoder := gob.NewDecoder(file)
	err := decoder.Decode(&bs)
	bs.Hash = CreateHashFunctions(bs.HashSize, bs.timeStamp)
	fmt.Println("Iz fajlaaaa", bs.Hash)
	if err != nil {
		panic(err.Error())
	}
	return *bs
}
