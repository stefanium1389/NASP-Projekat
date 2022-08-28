package Processor

import (
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
	processor.cache = cache.CacheConstructor(config.CacheCapacity)
	processor.memtable = Memtable.NewMemtable(config.MemtableThreshold, config.SLMaxLevel, config.SLProbability)
	processor.tokenBucket = TokenBucket.TokenBucketConstructor(config.TokenBucketMaxTokenNum, config.TokenBucketResetInterval)
	processor.wal = WAL.NewWAL(config.WALSegment, config.WALLowMark)

	//TODO Generate files

	return &processor
}

func (processor *Processor) Put(key, value string){
	//TODO write path
}

func (processor *Processor) Get(key string) (string, bool){
	//TODO read path
	return "", false
}

func (processor *Processor) Delete(key string) bool{
	_, found := processor.Get(key)
	if found{
		//TODO delete from memtable
		processor.cache.Remove(key)
		return true
	}

	return false
}