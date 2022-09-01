package Processor

import (
	"fmt"
	"main/BloomFilter"
	"main/Configuration"
	cache "main/LRUCache"
	"main/Memtable"
	"main/SSTable"
	"main/TokenBucket"
	"main/WriteAheadLog"
	"os"
	"strconv"
	"strings"
)

type Processor struct {
	memtable    *Memtable.Memtable
	cache       *cache.Cache
	tokenBucket *TokenBucket.TokenBucket
	wal         *WriteAheadLog.WriteAheadLog
	bf          *BloomFilter.BloomFilter
}

var FPRate float64

func NewProcessor() *Processor {
	processor := Processor{}
	config := Configuration.Load()
	processor.cache = cache.NewCache(config.CacheCapacity)
	processor.memtable = Memtable.NewMemtable(config.MemtableThreshold, config.SLMaxLevel, config.SLProbability)
	processor.tokenBucket = TokenBucket.NewTokenBucket(config.TokenBucketMaxTokenNum, config.TokenBucketResetInterval)
	processor.wal = WriteAheadLog.NewWAL(config.WALSegment)
	processor.bf = &BloomFilter.BloomFilter{}
	FPRate = config.FPRateBloomFilter
	processor.bf.Initialize(processor.memtable.GetThreshold(), FPRate)

	return &processor
}

func (processor *Processor) Put(key string, value []byte) bool {
	if !processor.tokenBucket.ProcessRequest() {
		fmt.Println("Prekoracili ste dozvoljeni broj zahteva u jedinici vremena")
		return false
	}
	processor.cache.Add(key, value)
	processor.wal.Put(key, value, false)
	success := processor.memtable.Insert(key, value)
	if !success {
		fmt.Println("*** MemTable Full ***")
		SSTable.Flush(processor.memtable, *processor.bf)
		processor.memtable.Empty()
		processor.bf.Initialize(processor.memtable.GetThreshold(), FPRate)
		success = processor.memtable.Insert(key, value)
	}

	return true
}

func (processor *Processor) Delete(key string) bool {
	if !processor.tokenBucket.ProcessRequest() {
		fmt.Println("Prekoracili ste dozvoljeni broj zahteva u jedinici vremena")
		return false
	}
	_, found := processor.Get(key)
	if !found {
		return false
	}
	processor.wal.Put(key, nil, true)
	deleted := processor.memtable.FindAndDelete(key)
	if !deleted {
		success := processor.memtable.Insert(key, []byte(""))
		if !success {
			SSTable.Flush(processor.memtable, *processor.bf)
			processor.memtable.Empty()
			processor.memtable.Insert(key, []byte(""))
		}
		processor.memtable.FindAndDelete(key)

	}
	processor.cache.Remove(key)
	return true
}

func (processor *Processor) Get(key string) (SSTable.Element, bool) {
	if !processor.tokenBucket.ProcessRequest() {
		fmt.Println("Prekoracili ste dozvoljeni broj zahteva u jedinici vremena")
		return SSTable.Element{}, false
	}
	// Cache Check
	flag, value := processor.cache.Get(key)
	if flag {
		fmt.Println("Vrednost pronadjena u Cache-u: ", string(value))
		return SSTable.Element{Key: key, Value: value}, flag
	}
	//MemTable Check
	mtHas := processor.memtable.Find(key)
	if mtHas != nil {
		fmt.Println("Kljuc pronadjen u MemTable-u")
		return SSTable.Element{Key: mtHas.Key, Value: mtHas.Value}, true
	}

	level, _ := os.ReadDir("./Data/SSTable/")
	var lvl string
	var dirPath string
	if len(level) == 1 {
		dirPath = "./Data/SSTable/" + level[0].Name()
		lvl = strings.Split(level[0].Name(), "l")[1]
	} else {
		dirPath = "./Data/SSTable/Level" + strconv.Itoa(len(level))
		lvl = strconv.Itoa(len(level))
	}
	files, _ := os.ReadDir(dirPath)
	for i := 1; i <= len(files); i++ {
		var prefix string
		if i == 1 {
			prefix = dirPath + "/SSTable/"
		} else {
			prefix = dirPath + "/SSTable" + strconv.Itoa(i-1) + "/"
		}
		// Bloom Check
		processor.bf.DeserializeAndDecode(prefix + "usertable-" + lvl + "-Filter.db")

		if processor.bf.MaybeContains([]byte(key)) {
			fmt.Println("Kljuc se mozda nalazi u BloomFilteru --> Pretraga se nastavlja u Summary")
			//Summary Check
			SSTable.PrintSummary(prefix + "usertable-" + lvl + "-Summary.db")
			sumHas, offsetIndx := SSTable.CheckSummary(prefix+"usertable-"+lvl+"-Summary.db", key)
			if !sumHas {
				fmt.Println("Kljuc se ne cuva u Summary strukturi")
				continue
			}
			//Index
			fmt.Println("Kluc se nalazi u Summary --> pretraga offset-a u Index")
			SSTable.PrintIndex(prefix + "usertable-" + lvl + "-Index.db")
			indHas, offsetData := SSTable.CheckIndex(prefix+"usertable-"+lvl+"-Index.db", key, offsetIndx)
			if !indHas {
				fmt.Println("Greska pri pronalazanju ofseta...")
				continue
			}
			//Data
			fmt.Println("Posle pronalazaenja offseta ulazimo u data file da zavrsimo pretragu")
			SSTable.PrintData(prefix + "usertable-" + lvl + "-Data.db")
			Elem := SSTable.CheckData(prefix+"usertable-"+lvl+"-Data.db", key, offsetData)
			processor.cache.Add(key, Elem.Value)
			return *Elem, true
		}
	}

	return SSTable.Element{}, false
}
