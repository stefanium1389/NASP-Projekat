package WriteAheadLog

import (
	"encoding/binary"
	"hash/crc32"
	"time"
)

type WALNode struct {
	CRC       uint32
	timeStamp uint64
	tombstone byte
	keySize   uint64
	valueSize uint64
	key       []byte
	value     []byte
}

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

func NewNode(key string, value []byte) *WALNode {
	node := WALNode{
		CRC:       0,
		timeStamp: uint64(time.Now().Unix()),
		tombstone: 0,
		key:       []byte(key),
		value:     value,
		keySize:   uint64(len(key)),
		valueSize: uint64(len(value)),
	}
	node.CRC = node.calculateSum()
	return &node
}

func (node *WALNode) Encode() []byte {
	retVal := make([]byte, 0)

	arr := make([]byte, 4)
	binary.LittleEndian.PutUint32(arr, node.CRC)
	retVal = append(retVal, arr...)
	arr = make([]byte, 16)
	binary.LittleEndian.PutUint64(arr, node.timeStamp)
	retVal = append(retVal, arr...)
	retVal = append(retVal, node.tombstone)
	arr = make([]byte, 8)
	binary.LittleEndian.PutUint64(arr, node.keySize)
	retVal = append(retVal, arr...)
	arr = make([]byte, 8)
	binary.LittleEndian.PutUint64(arr, node.valueSize)
	retVal = append(retVal, arr...)
	retVal = append(retVal, node.key...)
	retVal = append(retVal, node.value...)
	return retVal
}
func (node *WALNode) calculateSum() uint32 {
	bytes := make([]byte, 0)

	item := make([]byte, 16)
	binary.LittleEndian.PutUint64(item, node.timeStamp)
	bytes = append(bytes, item...)

	bytes = append(bytes, node.tombstone)

	item = make([]byte, 8)
	binary.LittleEndian.PutUint64(item, node.keySize)
	bytes = append(bytes, item...)

	item = make([]byte, 8)
	binary.LittleEndian.PutUint64(item, node.valueSize)
	bytes = append(bytes, item...)

	bytes = append(bytes, node.key...)
	bytes = append(bytes, node.value...)

	return CRC32(bytes)
}

func (node *WALNode) dataForWrite() []byte {
	//CRC (4B)   | Timestamp (16B) | Tombstone(1B) | Key Size (8B) | Value Size (8B) | Key | Value |
	data := make([]byte, 0)

	dataItem := make([]byte, 4)
	binary.LittleEndian.PutUint32(dataItem, node.CRC)
	data = append(data, dataItem...)

	dataItem = make([]byte, 16)
	binary.LittleEndian.PutUint64(dataItem, node.timeStamp)
	data = append(data, dataItem...)

	data = append(data, node.tombstone)

	dataItem = make([]byte, 8)
	binary.LittleEndian.PutUint64(dataItem, node.keySize)
	data = append(data, dataItem...)

	dataItem = make([]byte, 8)
	binary.LittleEndian.PutUint64(dataItem, node.valueSize)
	data = append(data, dataItem...)

	data = append(data, node.key...)
	data = append(data, node.value...)

	return data

}
func (node *WALNode) checkSum() bool {
	return node.CRC == node.calculateSum()
}
