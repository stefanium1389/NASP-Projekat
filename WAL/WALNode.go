package WAL

import (
	"hash/crc32"
	"time"
)

/*
   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
   |    CRC (4B)   | Timestamp (16B) | Tombstone(1B) | Key Size (8B) | Value Size (8B) | Key | Value |
   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
   CRC = 32bit hash computed over the payload using CRC
   Key Size = Length of the Key data
   Tombstone = If this record was deleted and has a value
   Value Size = Length of the Value data
   Key = Key data
   Value = Value data
   Timestamp = Timestamp of the operation in seconds
*/

type WALNode struct{
	CRC uint32
	timeStamp int64
	tombstone byte
	keySize uint64
	valueSize uint64
	key []byte
	value []byte
}

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

func NewNode(key, value string) *WALNode{
	node:= WALNode{}

	node.CRC = CRC32([]byte(key))
	node.timeStamp = time.Now().Unix()
	node.tombstone = 0
	node.key = []byte(key)
	node.value = []byte(value)
	node.keySize = uint64(len(node.key))
	node.valueSize = uint64(len(node.value))

	return &node
}