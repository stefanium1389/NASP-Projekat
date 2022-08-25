package main

import (
	"time"
)

const (
	DEFAULT_RESET_INTERVAL = 10
	DEFAULT_MAX_TOKEN_NUM = 10
)

type TokenBucket struct{
	lastReset int64
	maxTokenNum int
	tokenNum int
	resetInterval int64
}

func tokenBucketConstructor(maxTokenNum int, resetInterval int64) *TokenBucket{
	tb := TokenBucket{}
	tb.lastReset = time.Now().Unix()
	if maxTokenNum <= 0{
		maxTokenNum = DEFAULT_MAX_TOKEN_NUM
	}
	tb.maxTokenNum = maxTokenNum
	tb.tokenNum = 0
	if resetInterval <= 0{
		resetInterval = DEFAULT_RESET_INTERVAL
	}
	tb.resetInterval = resetInterval

	return &tb
}

func (tokenBucket *TokenBucket) processRequest() bool{
	currentTime := time.Now().Unix()
	if  currentTime - tokenBucket.lastReset >= tokenBucket.resetInterval{
		tokenBucket.lastReset = currentTime
		tokenBucket.tokenNum = 0

	}else if tokenBucket.tokenNum >= tokenBucket.maxTokenNum{
		return false
	}
	tokenBucket.tokenNum++
	return true
}
