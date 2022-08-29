package CountMinSketch

import (
	"bytes"
	"encoding/gob"
	"github.com/spaolacci/murmur3"
	"hash"
	"math"
	"time"
)

type CountMinSketch struct{
	table [][]uint
	kNum uint32
	mNum uint32
	hashFuncs []hash.Hash32
	timeStamp uint
}

func NewCountMinSketch(epsilon, delta float64) *CountMinSketch{
	cms := CountMinSketch{}
	cms.kNum = cms.CalculateK(epsilon)
	cms.mNum = cms.CalculateM(delta)
	cms.timeStamp = uint(time.Now().Unix())
	cms.hashFuncs = CreateHashFunctions(cms.kNum, cms.timeStamp)

	cms.table = make([][]uint, cms.kNum)
	for i := range cms.table{
		cms.table[i] = make([]uint, cms.mNum)
	}
	return &cms
}

func (cms *CountMinSketch) Add(key string) {
	for i:= 0; i< int(cms.kNum); i++{
		_, err := cms.hashFuncs[i].Write([]byte(key))
		if err != nil {
			panic(err)
		}
		colValue := cms.hashFuncs[i].Sum32() % cms.mNum
		cms.table[i][colValue] += 1
		cms.hashFuncs[i].Reset()
	}
}

func (cms *CountMinSketch) GetFrequency(key string) uint{
	result := make([]uint, cms.kNum, cms.kNum)
	for i:=0; i < int(cms.kNum) ; i++{
		_, err := cms.hashFuncs[i].Write([]byte(key))
		if err != nil {
			panic(err)
		}
		colValue := cms.hashFuncs[i].Sum32() % cms.mNum
		result[i] = cms.table[i][colValue]
		cms.hashFuncs[i].Reset()
	}

	min:= result[0]
	for _, value := range result{
		if value < min{
			min = value
		}
	}
	return min

}

func (cms *CountMinSketch) CalculateM(epsilon float64) uint32 {
	return uint32(math.Ceil(math.E / epsilon))
}

func (cms *CountMinSketch) CalculateK(delta float64) uint32 {
	return uint32(math.Ceil(math.Log(math.E / delta)))
}

func CreateHashFunctions(k uint32, timeStamp uint) []hash.Hash32 {
	h := []hash.Hash32{}
	for i := uint32(0); i < k; i++ {
		h = append(h, murmur3.New32WithSeed(uint32(timeStamp+1)))
	}
	return h
}

func (cms *CountMinSketch) Encode() []byte {
	encoded := bytes.Buffer{}
	encoder := gob.NewEncoder(&encoded)
	err := encoder.Encode(cms)
	if err != nil {
		panic(err.Error())
	}
	return encoded.Bytes()
}

func(cms *CountMinSketch) Decode(data []byte){
	encoded := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(encoded)
	err := decoder.Decode(cms)
	if err != nil {
		panic(err.Error())
	}
	cms.hashFuncs = CreateHashFunctions(cms.kNum, cms.timeStamp)
}
