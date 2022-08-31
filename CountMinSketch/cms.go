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
	Table [][]uint
	KNum uint32
	MNum uint32
	HashFuncs []hash.Hash32
	TimeStamp uint
}

func NewCountMinSketch(epsilon, delta float64) *CountMinSketch{
	cms := CountMinSketch{}
	cms.KNum = cms.CalculateK(epsilon)
	cms.MNum = cms.CalculateM(delta)
	cms.TimeStamp = uint(time.Now().Unix())
	cms.HashFuncs = CreateHashFunctions(cms.KNum, cms.TimeStamp)

	cms.Table = make([][]uint, cms.KNum)
	for i := range cms.Table{
		cms.Table[i] = make([]uint, cms.MNum)
	}
	return &cms
}

func (cms *CountMinSketch) Add(key string) {
	for i:= 0; i< int(cms.KNum); i++{
		_, err := cms.HashFuncs[i].Write([]byte(key))
		if err != nil {
			panic(err)
		}
		colValue := cms.HashFuncs[i].Sum32() % cms.MNum
		cms.Table[i][colValue] += 1
		cms.HashFuncs[i].Reset()
	}
}

func (cms *CountMinSketch) GetFrequency(key string) uint{
	result := make([]uint, cms.KNum, cms.KNum)
	for i:=0; i < int(cms.KNum) ; i++{
		_, err := cms.HashFuncs[i].Write([]byte(key))
		if err != nil {
			panic(err)
		}
		colValue := cms.HashFuncs[i].Sum32() % cms.MNum
		result[i] = cms.Table[i][colValue]
		cms.HashFuncs[i].Reset()
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

func CreateHashFunctions(k uint32, TimeStamp uint) []hash.Hash32 {
	h := []hash.Hash32{}
	for i := uint32(0); i < k; i++ {
		h = append(h, murmur3.New32WithSeed(uint32(TimeStamp+1)))
	}
	return h
}

func (cms *CountMinSketch) Encode() []byte {
	encoded := bytes.Buffer{}
	encoder := gob.NewEncoder(&encoded)
	cms.HashFuncs = nil
	err := encoder.Encode(&cms)
	if err != nil {
		panic(err.Error())
	}
	return encoded.Bytes()
}

func(cms *CountMinSketch) Decode(data []byte){
	encoded := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(encoded)
	err := decoder.Decode(&cms)
	if err != nil {
		panic(err.Error())
	}
	cms.HashFuncs = CreateHashFunctions(cms.KNum, cms.TimeStamp)
}
