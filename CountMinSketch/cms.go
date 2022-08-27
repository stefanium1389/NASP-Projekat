package CountMinSketch

import (
	"encoding/gob"
	"github.com/spaolacci/murmur3"
	"hash"
	"math"
	"os"
	"time"
)

type CountMinSketch struct{
	table [][]uint
	kNum uint32
	mNum uint32
	hashFuncs []hash.Hash32
}

func CountMinSketchConstructor(epsilon, delta float64) *CountMinSketch{
	cms := CountMinSketch{}
	cms.kNum = cms.CalculateK(epsilon)
	cms.mNum = cms.CalculateM(delta)
	cms.hashFuncs = cms.CreateHashFunctions(cms.kNum)

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

func (cms *CountMinSketch) CreateHashFunctions(k uint32) []hash.Hash32 {
	h := []hash.Hash32{}
	ts := uint(time.Now().Unix())
	for i := uint32(0); i < k; i++ {
		h = append(h, murmur3.New32WithSeed(uint32(ts+1)))
	}
	return h
}

func (cms *CountMinSketch) Serialize (fileName string) {
	file, err := os.Open(fileName)
	if err != nil {
		file, err = os.Create(fileName)
		if err != nil{
			panic(err)
		}
	}
	encoder:= gob.NewEncoder(file)
	err = encoder.Encode(cms)
	if err != nil{
		panic(err)
	}
	err = file.Close()
	if err != nil{
		panic(err)
	}
}

func(cms *CountMinSketch) Deserialize(fileName string){
	file, err := os.Open(fileName)
	if err != nil{
		panic(err)
	}
	decoder := gob.NewDecoder(file)
	err = decoder.Decode(cms)
	if err != nil{
		panic(err)
	}
	err = file.Close()
	if err != nil{
		panic(err)
	}
}
