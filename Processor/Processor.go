package Processor

import (
	"fmt"
	"main/BloomFilter"
	"main/Configuration"
	cache "main/LRUCache"
	"main/Memtable"
	"main/SSTable"
	"main/TokenBucket"
	"main/WAL"
	"os"
)

type Processor struct {
	memtable    *Memtable.Memtable
	cache       *cache.Cache
	tokenBucket *TokenBucket.TokenBucket
	wal         *WAL.WriteAheadLog
	bf          *BloomFilter.BloomStruct
}

func NewProcessor() *Processor {
	processor := Processor{}
	config := Configuration.Load()
	processor.cache = cache.NewCache(config.CacheCapacity)
	processor.memtable = Memtable.NewMemtable(config.MemtableThreshold, config.SLMaxLevel, config.SLProbability)
	processor.tokenBucket = TokenBucket.NewTokenBucket(config.TokenBucketMaxTokenNum, config.TokenBucketResetInterval)
	processor.wal = WAL.NewWAL(config.WALSegment, config.WALLowMark)
	processor.bf = BloomFilter.CreateBloom(100, 5)
	//TODO Generate files
	return &processor
}

func (processor *Processor) Put(key string, value []byte) bool {
	if !processor.tokenBucket.ProcessRequest() {
		fmt.Println("Prekoracili ste dozvoljeni broj zahteva u jedinici vremena")
		return false
	}
	processor.cache.Add(key, value)
	// TODO add to WAL
	success := processor.memtable.Insert(key, value)
	if !success {
		return false
	}

	return true
}

func (processor *Processor) Delete(key string) bool {
	if !processor.tokenBucket.ProcessRequest() {
		fmt.Println("Prekoracili ste dozvoljeni broj zahteva u jedinici vremena")
		return false
	}
	_, found := processor.Get(key)
	if found {
		//TODO delete from memtable
		processor.cache.Remove(key)
		return true
	}

	return false
}

func (processor *Processor) Get(key string) (SSTable.Element, bool) {
	if !processor.tokenBucket.ProcessRequest() {
		fmt.Println("Prekoracili ste dozvoljeni broj zahteva u jedinici vremena")
		//return "", false
	}
	// Cache Check
	flag, value := processor.cache.Get(key)
	if flag {
		fmt.Println("Vrednost pronadjena u Cache-u: ", string(value))
		return SSTable.Element{}, flag
	}
	//MemTable Check
	mtHas := processor.memtable.Find(key)
	if mtHas != nil {
		fmt.Println("Kljuc pronadjen u MemTable-u")
		fmt.Println("Key: ", mtHas.Key, "\tValue: ", mtHas.Value)
	}
	// Bloom Check
	var file, err = os.OpenFile("./Data/SSTable/Level1/SSTable/usertable-1-Filter.db", os.O_RDWR, 0777)
	SSTable.Panic(err)
	processor.bf.ReadBfFromDisk(file)
	if processor.bf.MaybeContains(key) {
		fmt.Println("Kljuc se mozda nalazi u BloomFilteru --> Pretraga se nastavlja u Summary")
		//Summary Check
		//TODO function for file path
		sumHas, offsetIndx := SSTable.CheckSummary("./Data/SSTable/Level1/SSTable/usertable-1-Summary.db", key)
		if !sumHas {
			fmt.Println("Kljuc se ne cuva u Summary strukturi")
			return SSTable.Element{}, false
		}
		//Index
		indHas, offsetData := SSTable.CheckIndex("./Data/SSTable/Level1/SSTable/usertable-1-Summary.db", key, offsetIndx)
		if !indHas {
			fmt.Println("Greska pri pronalazanju ofseta...")
			return SSTable.Element{}, false
		}
		//Data
		Elem := SSTable.CheckData("./Data/SSTable/Level1/SSTable/usertable-1-Data.db", key, offsetData)
		processor.cache.Add(key, Elem.Value)
		return *Elem, true
	}

	return SSTable.Element{}, false
}
