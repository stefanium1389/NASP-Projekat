package main

import (
	"time"
)

const (
	DEFAULT_RESET_INTERVAL = "10s"
	DEFAULT_MAX_TOKEN_NUM = 10
)

type TokenBucket struct{
	lastReset time.Time
	maxTokenNum int
	tokenNum int
	resetInterval time.Duration
}

func tokenBucketConstructor(maxTokenNum int, resetInterval string) *TokenBucket{
	tb := TokenBucket{}
	tb.lastReset = time.Now()
	if maxTokenNum <= 0{
		maxTokenNum = DEFAULT_MAX_TOKEN_NUM
	}
	tb.maxTokenNum = maxTokenNum
	tb.tokenNum = 0
	if resetInterval == ""{
		resetInterval = DEFAULT_RESET_INTERVAL
	}
	tb.resetInterval, _ = time.ParseDuration(resetInterval)

	return &tb
}

func (tokenBucket *TokenBucket) processRequest() bool{
	currentTime := time.Now()
	if currentTime.Sub(tokenBucket.lastReset) >= tokenBucket.resetInterval{
		tokenBucket.lastReset = currentTime
		tokenBucket.tokenNum = 0

	}else if tokenBucket.tokenNum >= tokenBucket.maxTokenNum{
		return false
	}
	tokenBucket.tokenNum++
	return true
}
