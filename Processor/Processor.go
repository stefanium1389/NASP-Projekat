package Processor

import (
	"fmt"
	"main/Configuration"
	cache "main/LRUCache"
	"main/Memtable"
	"main/TokenBucket"
	"main/WAL"
)

type Processor struct{
	memtable *Memtable.Memtable
	cache *cache.Cache
	tokenBucket *TokenBucket.TokenBucket
	wal *WAL.WriteAheadLog
}

func NewProcessor() *Processor{
	processor := Processor{}
	config := Configuration.Load()
	processor.cache = cache.NewCache(config.CacheCapacity)
	processor.memtable = Memtable.NewMemtable(config.MemtableThreshold, config.SLMaxLevel, config.SLProbability)
	processor.tokenBucket = TokenBucket.NewTokenBucket(config.TokenBucketMaxTokenNum, config.TokenBucketResetInterval)
	processor.wal = WAL.NewWAL(config.WALSegment, config.WALLowMark)

	return &processor
}

func (processor *Processor) Put(key string, value []byte) bool{
	if !processor.tokenBucket.ProcessRequest(){
		fmt.Println("Prekoracili ste dozvoljeni broj zahteva u jedinici vremena")
		return false
	}
	processor.cache.Add(key, value)
	// TODO add to WAL
	success := processor.memtable.Insert(key, value)
	if !success{
		return false
	}

	return true
}

func (processor *Processor) Get(key string) (string, bool){
	if !processor.tokenBucket.ProcessRequest(){
		fmt.Println("Prekoracili ste dozvoljeni broj zahteva u jedinici vremena")
		return "", false
	}
	//TODO read path
	return "", false
}

func (processor *Processor) Delete(key string) bool{
	if !processor.tokenBucket.ProcessRequest(){
		fmt.Println("Prekoracili ste dozvoljeni broj zahteva u jedinici vremena")
		return false
	}
	_, found := processor.Get(key)
	if found{
		//TODO delete from memtable
		processor.cache.Remove(key)
		return true
	}

	return false
}