package BloomFilter

import (
	"github.com/spaolacci/murmur3"
	"hash"
	"math"
	"time"
)

type BloomFilter interface {
	Add(s string)
	MaybeContains(s string) bool
}

type BloomStruct struct {
	data     []byte
	hash     []hash.Hash32
	dataSize int
	hashSize int
}

func (bs BloomStruct) Add(s string) {
	for i := 0; i < bs.hashSize; i++ {
		bs.hash[i].Write([]byte(s))
		index := bs.hash[i].Sum32() % uint32(bs.dataSize)
		bs.hash[i].Reset()
		bs.data[index] = 1
	}
}

func (bs BloomStruct) MaybeContains(s string) bool {
	for i := 0; i < bs.hashSize; i++ {
		bs.hash[i].Write([]byte(s))
		index := bs.hash[i].Sum32() % uint32(bs.dataSize)
		bs.hash[i].Reset()
		if bs.data[index] != 1 {
			return false
		}
	}
	return true
}

func CreateHashFunctions(hashSize int) []hash.Hash32 {
	hash := []hash.Hash32{}
	ts := uint(time.Now().Unix())
	for i := 1; i <= hashSize; i++ {
		hash = append(hash, murmur3.New32WithSeed(uint32(ts+1)))
	}
	return hash
}

func CreateBloom(capacity int, ratio float64) *BloomFilter {
	dataSize := int(math.Ceil(float64(capacity) * math.Abs(math.Log(ratio)) / math.Pow(math.Log(2), float64(2))))
	hashSize := int(math.Ceil((float64(dataSize) / float64(capacity)) * math.Log(2)))
	var bf BloomFilter = BloomStruct{
		dataSize: dataSize,
		hashSize: hashSize,
		data:     make([]byte, dataSize),
		hash:     CreateHashFunctions(hashSize),
	}
	return &bf
}
