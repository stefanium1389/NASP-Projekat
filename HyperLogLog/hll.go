package HyperLogLog

import (
	"bytes"
	"encoding/gob"
	"github.com/spaolacci/murmur3"
	"hash"
	"math"
	"math/bits"
	"time"
)

type HyperLogLog struct{
	registers []uint
	m         uint32	//set size
	p         int	//number of leading bits
	hash hash.Hash32
	timeStamp uint
}

func NewHyperLogLog(p int) *HyperLogLog {
	hll := HyperLogLog{}
	hll.m = uint32(math.Pow(2, float64(p)))
	hll.timeStamp = uint(time.Now().Unix())
	hll.hash = CreateHashFunction(hll.timeStamp)
	hll.registers = make([]uint, hll.m)
	hll.p = p
	return &hll
}

func (hll *HyperLogLog) Add(data string) {
	hashedValue := hll.hashData(data)
	_, err := hll.hash.Write([]byte(data))
	if err != nil {
		panic(err)
	}
	mask := 32 - hll.p
	bucket := hashedValue >> mask
	zeroes := bits.TrailingZeros32(hashedValue)

	hll.registers[bucket] = uint(zeroes)
}

func (hll *HyperLogLog) hashData(data string) uint32 {
	_, err := hll.hash.Write([]byte(data))
	if err != nil {
		panic(err)
	}
	sum := hll.hash.Sum32()
	hll.hash.Reset()
	return sum
}

func (hll *HyperLogLog) Estimate() float64 {
	sum := 0.0
	for _, val := range hll.registers {
		sum += math.Pow(math.Pow(2.0, float64(val)),-1)
	}

	alpha := 0.7213 / (1.0 + 1.079/float64(hll.m))
	estimation := alpha * math.Pow(float64(hll.m), 2.0) / sum
	emptyRegs := hll.emptyCount()
	if estimation <= 2.5*float64(hll.m) { // do small range correction
		if emptyRegs > 0 {
			estimation = float64(hll.m) * math.Log(float64(hll.m)/float64(emptyRegs))
		}
	} else if estimation > 1/30.0*math.Pow(2.0, 32.0) { // do large range correction
		estimation = -math.Pow(2.0, 32.0) * math.Log(1.0-estimation/math.Pow(2.0, 32.0))
	}
	return estimation
}

func (hll *HyperLogLog) emptyCount() int {
	sum := 0
	for _, val := range hll.registers {
		if val == 0 {
			sum++
		}
	}
	return sum
}

func(hll *HyperLogLog) Encode() []byte{
	encoded := bytes.Buffer{}
	encoder := gob.NewEncoder(&encoded)
	err := encoder.Encode(hll)
	if err != nil {
		panic(err.Error())
	}
	return encoded.Bytes()
}

func (hll *HyperLogLog) Decode(data []byte) {
	encoded := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(encoded)
	err := decoder.Decode(hll)
	if err != nil {
		panic(err.Error())
	}
	hll.hash = CreateHashFunction(hll.timeStamp)
}

func CreateHashFunction(ts uint) hash.Hash32{
	return murmur3.New32WithSeed(uint32(ts))
}