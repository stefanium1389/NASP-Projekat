package BloomFilter

import (
	"encoding/gob"
	"github.com/spaolacci/murmur3"
	"hash"
	"math"
	"os"
	"time"
)

type BloomFilter struct {
	M, K, TimeStamp uint
	BitSet          []byte
	Hash            []hash.Hash32
}

func (bf *BloomFilter) Initialize(elemRange int, fpRate float64) {
	bf.M = uint(math.Ceil(float64(elemRange) * math.Abs(math.Log(fpRate)) / math.Pow(math.Log(2), float64(2))))
	bf.K = uint(math.Ceil((float64(bf.M) / float64(elemRange)) * math.Log(2)))
	bf.Hash, bf.TimeStamp = resolveHash(bf.K, 0)
	bf.BitSet = make([]byte, bf.M, bf.M)
}

func resolveHash(k uint, ts uint) ([]hash.Hash32, uint) {
	h := []hash.Hash32{}
	if ts == 0 {
		ts = uint(time.Now().Unix())
	}
	i := uint(0)
	for ; i < k; i++ {
		h = append(h, murmur3.New32WithSeed(uint32(ts+i)))
	}
	return h, ts
}

func (bf *BloomFilter) AddElement(key []byte) {
	i := 0
	for ; i < len(bf.Hash); i++ {
		bf.Hash[i].Reset()
		_, err := bf.Hash[i].Write(key)
		if err != nil {
			return
		}
		indx := bf.Hash[i].Sum32() % uint32(bf.M)
		bf.BitSet[indx] = 1
	}
}

func (bf *BloomFilter) MaybeContains(key []byte) bool {
	i := 0
	for ; i < len(bf.Hash); i++ {
		bf.Hash[i].Reset()
		_, err := bf.Hash[i].Write(key)
		if err != nil {
			return false
		}
		indx := bf.Hash[i].Sum32() % uint32(bf.M)
		if bf.BitSet[indx] == 0 {
			return false
		}
	}
	return true
}
func Panic(err error) {
	if err != nil {
		panic(err.Error())
	}
}
func (bf *BloomFilter) EncodeAndSerialize(fn string) bool {
	bf.Hash = nil
	file, err := os.Create(fn)
	Panic(err)

	encoder := gob.NewEncoder(file)
	err = encoder.Encode(bf)
	Panic(err)

	err = file.Close()
	Panic(err)
	return true
}

func (bf *BloomFilter) DeserializeAndDecode(fn string) {
	file, err := os.Open(fn)
	Panic(err)

	decoder := gob.NewDecoder(file)
	err = decoder.Decode(&bf)
	Panic(err)

	bf.Hash, _ = resolveHash(bf.K, bf.TimeStamp)

	err = file.Close()
	Panic(err)
}
