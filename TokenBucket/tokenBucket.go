package TokenBucket

import (
	"time"
)

type TokenBucket struct{
	lastReset int64
	maxTokenNum int
	tokenNum int
	resetInterval int64
}

func NewTokenBucket(maxTokenNum int, resetInterval int64) *TokenBucket{
	tb := TokenBucket{}
	tb.lastReset = time.Now().Unix()
	tb.maxTokenNum = maxTokenNum
	tb.tokenNum = 0
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
